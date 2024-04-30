import argparse
import sys

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", type=int, default=0)
    parser.add_argument("filenames", nargs="*", help='list of files')

    args = parser.parse_args()

    for i in args.filenames:
        with open(i) as input_file:
            for count, line in enumerate(input_file, 1):
                if count == 0:
                    sys.exit()
                else:
                    if count == args.c:
                        print(f"{count}. '{line.strip()}'")
                        break

