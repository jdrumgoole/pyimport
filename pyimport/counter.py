import argparse
import datetime


def count_lines_enumerate(filename):
    count = 0
    with open(filename, "r") as input_file:
        for (count, _) in enumerate(input_file, 1):
            # print( "%i\t%s" % ( thread_id,  l.rstrip()))
            pass
    return count


def blocks(files, size=1024 * 64):
    while True:
        b = files.read(size)
        if not b: break
        yield b


def count_lines_block(filename):
    with open(filename, "r", encoding="utf-8", errors='ignore') as f:
        return sum(bl.count("\n") for bl in blocks(f))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--test")
    parser.add_argument("--file")

    args = parser.parse_args()

    start = datetime.datetime.now(datetime.UTC)

    if args.test == "blocks":
        count_lines_block(args.file)
    elif args.test == "enumerate":
        count_lines_enumerate(args.file)

    end = datetime.datetime.now(datetime.UTC)

    print("Duration: %s" % (end - start))
