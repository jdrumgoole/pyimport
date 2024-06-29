import csv
import sys
import argparse
import os


def convert_delimiters(input_file, output_file, old_delimiter, new_delimiter):
    # Check if input and output filenames are the same
    if args.output and os.path.abspath(input_file) == os.path.abspath(output_file):
        print("Error: Input and output filenames are the same.")
        sys.exit(1)

    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter=old_delimiter)

            if output_file:
                with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                    writer = csv.writer(outfile, delimiter=new_delimiter)
                    for row in reader:
                        writer.writerow(row)
            else:
                writer = csv.writer(sys.stdout, delimiter=new_delimiter)
                for row in reader:
                    writer.writerow(row)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CSV delimiters.')
    parser.add_argument('input_file', help='Input CSV file')
    parser.add_argument('-d', '--old_delimiter', default=",", help='Old delimiter in the CSV file')
    parser.add_argument('-r', '--new_delimiter', default=",", help='New delimiter to use in the CSV file [default: ","]')
    parser.add_argument('-o', '--output', help='Output CSV file (optional)')

    args = parser.parse_args()

    if args.old_delimiter == 'tab':
        args.old_delimiter = '\t'
    if args.new_delimiter == 'tab':
        args.new_delimiter = '\t'

    if args.old_delimiter == args.new_delimiter:
        print("Error: Old and new delimiters are the same. Nothing to do.")
        sys.exit(1)

    convert_delimiters(args.input_file, args.output, args.old_delimiter, args.new_delimiter)
