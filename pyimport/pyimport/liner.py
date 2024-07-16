import argparse


def write_file(file, count):
    for i in range(count):
        print(i, file=file)


def make_line_file(count=1, doseol=False, filename="liner.txt"):
    if doseol:
        with open(filename, "w", newline="\r\n") as file:
            write_file(file,count)
    else:
        with open(filename, "w", newline="\n") as file:
            write_file(file,count)
    return filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", default=1, type=int, help="Number of lines to write")
    parser.add_argument("--filename", default="liner.txt")
    parser.add_argument("--doseol", default=False, action="store_true")
    args = parser.parse_args()

    make_line_file(args.count, args.doseol, args.filename)
    print("Created '{}' with {} line(s) and DOS EOL: '{}'".format(args.filename, args.count, args.doseol))
