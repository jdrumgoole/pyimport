import argparse
import os
import sys

from pyimport.fieldfile import FieldFile

TYPE_MAP = {
    "int": "int64",
    "float": "double",
    "str": "string",
    "datetime": "date",
    "date": "date",
    "bool": "bool"
}


def write_output(output, content):
    """
    Write content to either stdout or a file.

    :param output: Can be either a filename (str) or sys.stdout
    :param content: The content to be written (str)
    """
    if output == sys.stdout:
        output.write(content)
    else:
        with open(output, 'w', encoding='utf-8') as f:
            f.write(content)

DATE_FORMAT= "2024-06-27 23:44:58"

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Convert a field file to a MongoDB field file")
    parser.add_argument("--fieldfile", help="Field file to convert")
    parser.add_argument("--mdbfile", help="Output MongoDB field file")

    args = parser.parse_args()

    if args.fieldfile is None:
        print("Error: Fieldfile must be specified")
        sys.exit(1)

    if args.mdbfile == args.fieldfile:
        print("Error: Fieldfile and MDB file must be different")
        sys.exit(1)

    if not os.path.isfile(args.fieldfile):
        print(f"Error: Fieldfile '{args.fieldfile}' does not exist")
        sys.exit(1)

    field_info = FieldFile.load(args.fieldfile)

    if args.mdbfile is None:
        output = sys.stdout
    else:
        output = open(args.mdbfile, 'w')

    for k, v in field_info.field_dict.items():
        if v["type"] == "datetime":
            output.write(f"{k}.{date}({DATE_FORMAT})\n")
        else:
            output.write(f"{k}.{TYPE_MAP[v["type"]]}()\n")

    if output != sys.stdout:
        output.close()
