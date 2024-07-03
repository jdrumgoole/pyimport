# pyimport - A CSV loader for MongoDB

`pyimport` is a python command program that will import data into a MongoDB database. `pyimport` is written by 
Joe Drumgoole. Email is [joe@joedrumgoole.com](mailto:joe@joedrumgoole.com)  or hit me up on X: [jdrumgoole](https://x.com/jdrumgoole).
It is a Python program designed for python 3.10 and beyond. It is Apache 2.0 licensed. Full source code is on
[github](https://github.com/jdrumgoole/pyimport).

# Quick Start

**Step 1**: Install the program

```
$ pip install pyimport
```

Check it runs by executing the following command:
```
$ pyimport -v
pyimport 1.8.2
```
Find a suitable CSV file to test. You can download a this file,  [2018_Yellow_Taxi_Trip_Data_1000.csv](https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv)
if you have nothing handy. This is a sample of the New Taxi cab dataset from the pyimport test suite. here is 
the curl command:
```bash
$ curl https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv -o yellow_trip.csv

```
Now generate a file file so that we know what the types of the fields are.
```
$ pyimport --genfieldfile --delimiter ';' yellow_trip.csv
Forcing has_header true for --genfieldfile
Generating field file from 'yellow_trip.csv'
Created field filename(s) 'yellow_trip.tff' from ['yellow_trip.csv']
```
Note that for a fieldfile to be generated successfully the CSV file must has a header line. For this file 
the delimiter is a semi-colon (;) so we have to specify that with the `--delimiter` option.

Now we can import the data into a MongoDB database. We will accept the default options for database and collection.  
```
$ pyimport --delimiter ';' yellow_trip.csv
Using host       :'mongodb://localhost:27017'
Using database   :'PYIM'
Using collection :'imported'
Write concern    : 0
journal          : False
fsync            : False
has header       : False
Processing:'yellow_trip.csv'
imported file: 'yellow_trip.csv' (1000 rows)
Total elapsed time to upload 'yellow_trip.csv' : 00:00:00.30877
Average upload rate per second: 32387
Total elapsed time to upload all files : 00:00:00.32865 seconds
Average upload rate per second: 30428
Total records written: 1000
```
If you look at the files in [Compass](https://www.mongodb.com/try/download/compass) you will see that the data has been uploaded.

[![Compass](https://jdrumgoole.s3.eu-west-1.amazonaws.com/initial-compass.png)

# Installation

You can install `pyimport` using `pip` the python package manager. This installs a [package](https://pypi.org/project/pyimport/) from PYPI.

```bash:
pip install pyimport
```
Once installed the program should be available as a script on your path. 
Run `pyimport -v` to check that the program is installed correctly.


# Why pyimport?
 
Why do we have `pyimport`? MongoDB already has a perfectly good 
[mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) program 
that is available for free in the standard MongoDB [community download](https://www.mongodb.com/download-center#community).

Well `pyimport` does a few things that `mongoimport` doesn't do (yet).

- Automatic `fieldfile` generation with the option **--genfieldfile**.
- Supports several options to handle "dirty" data: fail, warning or ignore.
- `--multi` option to allow multiple files to be imported in parallel.
- `--asyncpro` an option to use asyncio to parallelize the import.
- `--spiltfiles` option to split a large file into smaller files for parallel import.
- `--delimiter` option to specify the delimiter for the CSV file.
- 

On the other hand [mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) supports the richer security options of the [MongoDB Enterprise Advanced](https://www.mongodb.com/products/mongodb-enterprise-advanced)
product. It is also allows importing of JSON files. This tool is specifically designed for importing CSV data into 
MongoDB and is the best choice for large scale data imports from CSV repos.

# Field Files
Field files are TOML formatted files that define the fields in a CSV file. They are used to define the types of the
fields (aka columns) in the CSV file. 

The field file format is simple. Each field is defined by a TOML section with the field name as the table name. 
The field can have an alternative name, a type and a format. The type can be one of the following:

- *"str"* : a string
- *"float"* : a floating point number
- *"int"* : an integer
- *"date"* : a date without a time
- *"datetime"* : a date with a time
- *"isodate"* : a date in the ISO format YYYY-MM-DD
- *"bool"* : a boolean value
- *"timestamp"* : a timestamp

Each file you intend to upload must have a field file defining the
contents of the CSV file you plan to upload.

If a fieldfile is not explicitly passed in the program will look for a
fieldfile corresponding to the file name with the extension replaced
by `.ff`. So for an input file `inventory.csv` the corresponding field
file would be `inventory.tff`.

If there is no corresponding field file the upload will fail.

Field files (normally expected to have the extension `.tff`) define the names of columns and their
types for the importer. A field file isa TOML formatted line.

For a csv file [https://raw.githubusercontent.com/jdrumgoole/pyimport/master/test/test_filesplitter/inventory.csv) defined by the following format,


Inventory Item|Amount|Last Order
---|---:|---
Screws|300|1-Jan-2016
Bolts|150|3-Feb-2017
Nails|25|31-Dec-2017
Nuts|75|29-Feb-2016

The field file generated by `--genfieldfile` is

```
#
# Created 'inventory.tff'
# at UTC: 2024-07-03 16:06:48.443089+00:00 by class pyimport.fieldfile
# Parameters:
#    csv        : 'inventory.csv'
#    delimiter  : ','
#
["Inventory Item"]
type = "str"
name = "Inventory Item"
format = ""

[" Amount"]
type = "int"
name = " Amount"
format = ""

[" Last Order"]
type = "date"
name = " Last Order"
format = ""

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
"CSV File" = "inventory.csv"
#end
```

The generate field file function uses the first line after the header
line to guess the type of each column. It tries in order for each
column to successfully convert the type to a string (str), integer
(int), float (float) or date (datetime).

The generate function may guess wrong if the first line is not
correctly parseable or is ambiguous. In this case the user can edit the .ff file to
correct the types.

In any case if the type conversion fails when reading the actual
data-file the program will degenerate to converting to a string
without failing (unless [--onerror fail](#onerror)  is specified).

Each file in the input list must correspond to a field file format that is
common across all the files.

## Date Fields
Date fields are special and support processing options. There are three types
of date field.

* *date* : A normal date generally without a timestamp
* *datetime* : fully qualified date including a timestamp.
* *isodate* : A normal date in the standard ISO format YYYY-MM-DD. 

Both date and datetime fields support date formatting strings. This allows
[strptime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)
to be used to efficient format a date. 

If you do not specify a format string then the program will attempt to parse
each date field it finds using `dateparse` from the [dateutil](https://pypi.org/project/python-dateutil/) 
library. 

### Simple date format entry
```
["test_date"]
type="datetime"
```

This will use `dateparse` to make sense of each field.

```
["test_date"]
type="datetime"
format="%Y-%m-%d"
```

This will use `strptime` to process each string.
```
["test_date"]
type="isodate"
```

This will use [datetime.fromisoformat](https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat)
to parse the date. This format only supports YYYY-MM-DD. 

format="%Y-%m-%d"
### Performance
The `dateparse` mode is slower by several orders of magnitude.For
large data sets prefer `date` and `datetime` with a `strptime` compatible format
string. The faster formatting is done with `isodate`.
# Options
`pyimport` has a number of options that can be used to control the import process. In general options are applied to
all the files included at the end of the command line.

## Arguments



### Optional arguments:

**-h, --help**

Show the help message and exit.

**--database** *name*

Specify the *name* of the database to use [default: *test*]

**--collection** *name*

Specify the *name* of the collection to use [default: *test*]

**--host** *mongodb URI*

Specify the URI for connecting to the database. [default: *mongodb://localhost:27017/test*]

The full connection URI syntax is documented on the [MongoDB docs website.](https://docs.mongodb.com/manual/reference/connection-string/)

**--batchsize** *batchsize*

Set batch os_size for bulk inserts. This is the amount of docs the client
will add to a batch before trying to upload the whole chunk to the
server (reduces server round trips). [default: *500*].

For larger documents you may find a smaller *batchsize* is more efficient.

**--drop**

drop collection before loading [default: False]

**--ordered**

force ordered inserts

**--fieldfile** *FIELDFILE*

Field and type mappings [default: will look for each filenames for a corresponding *filename.tt* file.]

**--delimiter** *DELIMITER*

The delimiter string used to split fields [default: ,]

**--version**

show program's version number and exit

**--addfilename**
         
Add filename field to every entry

**--addtimestamp** [none|now|gen]
                        
Add a timestamp to each record [default: none]

**--has_header**

Use header line for column names [default: False]

**--genfieldfile**        
  
Generate automatically a typed field file *filename.tt* from the data file *filename.xxx*, we set the option *has_header* to true [default: False]

**--id [mongodb|gen]**
    
Auto generate ID [default: mongodb]

**--onerror [fail|warn|ignore]**

What to do when we hit an error parsing a csv file. Possibility to default to a String if we cannot parse a value. [default: warn]




### Positional arguments:
*filenames*: list of files to be processed. 
