import argparse
import os

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help='list of files')
    args = parser.parse_args()
    for filename in args.filenames:
        new_filename = os.path.splitext(filename)[0] + '.tff'
        print(f"Converting '{filename}' to '{new_filename}'")
        with open(filename) as input_file:
            with open(new_filename, "w") as output_file:
                for line_no,line in enumerate(input_file.readlines(), 1):
                    line = line.strip()
                    if ':' in line:
                        lhs, divider, rhs = line.partition(':')
                        rhs = rhs.strip()
                        lhs = lhs.strip()
                        if rhs == "int" or rhs == "str" or "float" or "date":
                            rhs = f"\"{rhs}\""
                        output_file.write( f"{lhs}={rhs}\n")
                    elif '=' in line:
                        lhs, divider, rhs = line.partition('=')
                        rhs = rhs.strip()
                        lhs = lhs.strip()
                        if rhs == "int" or rhs == "str" or "float" or "date":
                            rhs = f"\"{rhs}\""
                        output_file.write( f"{lhs}={rhs}\n")
                    elif line.startswith('['):
                        key_start = line.index('[') + 1
                        key_end = line.index(']')
                        output_file.write(f"[\"{line[key_start:key_end]}\"]\n")
                    else:
                        output_file.write(f"{line}\n")

        print(f"Processed {line_no} lines in {filename}")

