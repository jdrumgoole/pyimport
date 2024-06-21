import _csv
import argparse
import asyncio
import os

import sys
from asyncio import TaskGroup

import time

import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from requests import exceptions
from asyncstdlib import enumerate as aenumerate

from pyimport import timer
from pyimport.command import seconds_to_duration
from pyimport.commandutils import prep_parser, prep_field_file
from pyimport.csvreader import AsyncCSVReader
from pyimport.enrichtypes import EnrichTypes
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.linereader import RemoteLineReader, is_url


def async_prep_collection(args):
    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = AsyncIOMotorClient(args.host, w=args.writeconcern)
    else:
        client = AsyncIOMotorClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    database = client[args.database]
    collection = database[args.collection]

    return collection


async def async_prep_import(log, args: argparse.Namespace, filename: str, field_info: FieldFile):
    collection = async_prep_collection(args)
    parser = prep_parser(args, field_info, filename)

    if is_url(filename):
        log.info(f"Reading from URL:'{filename}'")
        csv_file = RemoteLineReader(url=filename)
    else:
        log.info(f"Reading from file:'{filename}'")
        csv_file = await aiofiles.open(filename, "r")

    reader = AsyncCSVReader(file=csv_file,
                            limit=args.limit,
                            field_file=field_info,
                            has_header=args.hasheader,
                            delimiter=args.delimiter)

    return collection, reader, parser


async def get_csv_doc(q: asyncio.Queue, p: EnrichTypes, async_reader: AsyncCSVReader):

    async for i, doc in aenumerate(async_reader, 1):
        d = p.enrich_doc(doc, i)
        await q.put(d)
    await q.put(None)
    return i


async def put_db_doc(args, log, queue: asyncio.Queue, collection: AsyncIOMotorCollection, filename: str) -> int:
    buffer = []
    time_period = 1.0
    total_written = 0
    inserted_this_quantum = 0

    time_start = time.time()
    loop_timer = timer.Timer(start_now=True)
    while True:
        doc = await queue.get()
        if doc is None:
            queue.task_done()
            break
        else:
            buffer.append(doc)
            queue.task_done()
            if len(buffer) == args.batchsize:
                await collection.insert_many(buffer)
                total_written = total_written + len(buffer)
                inserted_this_quantum = inserted_this_quantum + len(buffer)
                buffer = []
                elapsed = loop_timer.elapsed()
                if elapsed > time_period:
                    docs_per_second = inserted_this_quantum / elapsed
                    loop_timer.reset()
                    inserted_this_quantum = 0
                    log.info(
                        f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
    if len(buffer) > 0:
        await collection.insert_many(buffer)
        total_written = total_written + len(buffer)

    time_finish = time.time()
    elapsed_time = time_finish - time_start

    return total_written, elapsed_time


async def process_file(log, args, audit, filename):

    total_written = 0
    q = asyncio.Queue()

    field_file = prep_field_file(args)
    collection, async_reader, parser = await async_prep_import(log, args, filename, field_file)
    try:
        async with TaskGroup() as tg:
            t1 = tg.create_task(get_csv_doc(q, parser, async_reader))
            t2 = tg.create_task(put_db_doc(args, log, q, collection, filename))

        total_documents_processed = t1.result()
        total_written, elapsed_time = t2.result()
        await q.join()

        if total_documents_processed != total_written:
            log.error(f"Total documents processed: {total_documents_processed} is not equal to  Total written: {total_written}")
            raise ValueError(f"Total documents processed: {total_documents_processed} is not equal to  Total written: {total_written}")

        if audit:
            audit_doc = {"command": "import",
                         "filename": filename,
                         "elapsed_time": elapsed_time,
                         "total_written": total_written}
            await audit.add_batch_info(audit.current_batch_id, audit_doc)
        log.info(f"imported file: '{filename}' ({total_written} rows)")
        log.info(f"Total elapsed time to upload '{filename}' : {seconds_to_duration(elapsed_time)}")
        log.info(f"Average upload rate per second: {round(total_written / elapsed_time)}")
    except OSError as e:
        log.error(f"{e}")
    except exceptions.HTTPError as e:
        log.error(f"{e}")
    except FieldFileException as e:
        log.error(f"{e}")
    except _csv.Error as e:
        log.error(f"{e}")
    except ValueError as e:
        log.error(f"{e}")
    except KeyboardInterrupt:
        log.error(f"Keyboard interrupt... exiting")
        sys.exit(1)

    finally:
        if not is_url(filename):
            await async_reader.file.close()
    return total_written, elapsed_time


async def process_files(log, args, audit):
    tasks = []
    total_written = 0
    total_elapsed = 0
    try:
        async with TaskGroup() as tg:
            for filename in args.filenames:
                if not os.path.isfile(filename):
                    log.warning(f"No such file: '{i}' ignoring")
                    continue
                task = tg.create_task(process_file(log, args, audit, filename))
                tasks.append(task)

        for task in tasks:
            written, elapsed_time = task.result()
            total_written = total_written + written
            total_elapsed = total_elapsed + elapsed_time

    except OSError as e:
        log.error(f"{e}")
    except exceptions.HTTPError as e:
        log.error(f"{e}")
    except FieldFileException as e:
        log.error(f"{e}")
    except _csv.Error as e:
        log.error(f"{e}")
    except ValueError as e:
        log.error(f"{e}")
    except KeyboardInterrupt:
        log.error(f"Keyboard interrupt... exiting")
        sys.exit(1)

    return total_written, elapsed_time
