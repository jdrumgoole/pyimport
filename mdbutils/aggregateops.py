import argparse
import pprint
import sys

from pymongo import MongoClient


def out_op(collection_name:str):
    return {"$out": collection_name}


def field_is_present(field):
    return {"$ne": [{ "$type": f"${field}"}, "missing"]}


def cond_op(if_expr:dict, then_expr:dict|str, else_expr:dict|str):
    return {"$cond": {"if": if_expr,
                      "then": then_expr,
                      "else": else_expr}}


def not_op(d:dict):
    return {"$not": d}


def and_op(d:list[dict]):
    if isinstance(d, list):
        return {"$and": d}
    else:
        raise TypeError("and_op expects a list of dictionaries")


def set_op(field:str, value:dict|str):
    return {"$set": {field: value}}


def unset_op(field:str):
    return {"$unset": field}


def limit_op(limit:int):
    return {"$limit": limit}


def split_op(field:str, separator:str):
    return {"$split": [f"${field}", separator]}


def rename_op(old_field_name, new_field_name, out_collection, limit=0) -> list[dict]:

    pipeline = []
    if limit > 0:
        pipeline.append(limit_op(limit))

    conditional = cond_op(field_is_present(new_field_name),
                          f"${new_field_name}",
                          cond_op(field_is_present(old_field_name),
                                  f"${old_field_name}",
                                  "$$REMOVE"))
    #pprint.pprint(conditional)
    pipeline.append(set_op(new_field_name, conditional))
    pipeline.append(unset_op(old_field_name))
    if out_collection:
        pipeline.append(out_op(out_collection))

    return pipeline


def rename_field(db, original_collection, new_collection, field_name, new_field_name, limit=0, show=None):
    aggregation = rename_op(field_name, new_field_name, new_collection, limit)
    if show:
        pprint.pprint(aggregation)
        return

    if new_collection:
        db[original_collection].aggregate(aggregation)
    else:
        cursor = db[original_collection].aggregate(aggregation)
        for doc in cursor:
            pprint.pprint(doc)


def field_to_list_op(field, target_collection, seperator=";", limit=0):
    ftl_op = {
            "$set": {
                f"{field}": {
                    "$map": {
                        "input": {"$split": [f"${field}", f"{seperator}"]},
                        "as": "item",
                        "in": {"$trim": {"input": "$$item"}}
                    }
                }
            }
        }
    pipeline = []
    if limit > 0:
        pipeline.append(limit_op(limit))

    pipeline.append(ftl_op)
    pipeline.append(out_op(target_collection))

    return pipeline


def field_to_list(db, source_collection, field, target_collection, seperator=";", limit=0):
    aggregation = field_to_list_op(field, target_collection, seperator, limit)
    if target_collection:
        db[source_collection].aggregate(aggregation)
    else:
        cursor = db[source_collection].aggregate(aggregation)
        for doc in cursor:
            pprint.pprint(doc)

def parse_db_and_col(s: str) -> [str, str]:
    if '.' not in s:
        return s, None
    db_name, collection_name = s.split('.', 1)
    return db_name, collection_name

def main():
    parser = argparse.ArgumentParser(description='MongoDB Aggregation Utilities')
    parser.add_argument('--mdburi', type=str, default='mongodb://localhost:27017/',
                        help="MongoDB connection URL [default: 'mongodb://localhost:27017/']")
    parser.add_argument("--distinct", type=str, help="The field to get distinct values from")
    parser.add_argument("--field", type=str, help="The field to get distinct values from")
    parser.add_argument("--col", type=str, help="The source collection")
    parser.add_argument('--timeout', type=int, default=1000, help="Timeout in milliseconds (default: 1000).")
    parser.add_argument('--show', action='store_true', help='Show the aggregation pipeline without running it')

    subparsers = parser.add_subparsers(dest='subcommand', title="Sub Commands",  help='Subcommands with their own args')
    rename_parser = subparsers.add_parser('rename', help='Rename the fields in a database.collection')
    rename_parser.add_argument('--db', type=str, help='The database name')
    rename_parser.add_argument('--src', type=str, help='The collection name')
    rename_parser.add_argument('--dest', type=str, help='The new collection name')
    rename_parser.add_argument('--field', type=str, help='The field to rename')
    rename_parser.add_argument('--newfield', type=str, help='The new field name')
    rename_parser.add_argument('--limit', type=int, default=0, help='The limit of documents to rename')
    rename_parser.add_argument('--show', action='store_true', help='Show the aggregation pipeline without running it')

    field_list_parser = subparsers.add_parser('listify', help='Convert a string to a list in a database.collection')
    field_list_parser.add_argument('--db', type=str, help='The database name')
    field_list_parser.add_argument('--src', type=str, help='The collection name')
    field_list_parser.add_argument('--field', type=str, help='The field to convert to a list')
    field_list_parser.add_argument('--dest', type=str, help='The new collection name')
    field_list_parser.add_argument('--seperator', type=str, default=';', help='The separator to split the field')
    field_list_parser.add_argument('--limit', type=int, default=0, help='The limit of documents to rename')
    args = parser.parse_args()

    client = MongoClient(args.mdburi, serverSelectionTimeoutMS=args.timeout)

    if args.distinct:
        src_db, src_collection = parse_db_and_col(args.col)
        if src_db is None:
            print("No db specified")
            sys.exit(1)
        if src_collection is None:
            print("No collection specified")
            sys.exit(1)
        values = client[src_db][src_collection].distinct(args.distinct)
        for i in values:
            print(i)

    if args.subcommand == 'rename':

        rename_field(client[args.db], args.src, args.dest, args.field, args.newfield, args.limit, args.show)

    if args.subcommand == 'listify':
        field_to_list(client[args.db], args.src, args.field, args.dest, args.seperator, args.limit)


if __name__ == "__main__":
    main()
