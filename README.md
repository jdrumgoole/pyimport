# pyimport

`pyimport` is a python program that will import data into a MongoDB database.

Warning - This documentation is incomplete, give it time :-).

For questions please email joe@joedrumgoole.com


## Why pyimport?
 
Why do we have `pyimport`? MongoDB already has a perfectly good (and much faster)
[mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) program 
that is available for free in the standard MongoDB [community download](https://www.mongodb.com/download-center#community).

Well `pyimport` does a few things that mongoimport doesn't do (yet).

- Automatic `fieldfile` generation with the option **--genfieldfile**.
- Ability to stop and restart an import.
- Supports several options to handle "dirty" data: fail, warning or ignore.

On the other hand [mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) supports the richer 
security options of the [MongoDB Enterprise Advanced](https://www.mongodb.com/products/mongodb-enterprise-advanced)
product and because it is written in Go it can use threads more effectively and is generally faster.


## Examples

How to generate a field file

```
$pyimport --genfieldfile inventory.csv
Creating 'inventory.ff' from 'inventory.csv'
```
An example run:

```
$pyimport --delimiter '|' --database demo --collection demo --fieldfile mot_test_set_small.ff mot_test_set_small.csv
Using database: demo, collection: demo
processing 1 files
Processing : mot_test_set_small.csv
using field file: 'mot_test_set_small.ff'
Input: 'mot_test_set_small.csv' : Inserted 100 records
Total elapsed time to upload 'mot_test_set_small.csv' : 0.047
```

An example run where we want the upload to restart

```
```

## Arguments

### Positional arguments:
*filenames*: list of files to be processed

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

**--restart**

`pyimport` also has the ability to restart an upload from the
point where it stopped previously. Import metadata are recorded in a collection `restartlog` in the current database. And audit record is
stored for each upload in progress and each completed upload. Thus the
audit collection gives you a record of all uploads by filename and
date time.

The restart log record format is :

```
{ 
  "name"       : <name of file being uploaded>, 
  "timestamp"  : <datetime that this doc was inserted>,
  "batch_size" : <the batchsize specified by --batchsize>,
  "count"      : <the total number of documents inserted from <name>>,
  "doc_id"     : <The mongodb _id field for the last record inserted in this batch>
}
```

The restart log is keyed of the filename so each filename must be unique otherwise
imports that are running in parallel will overwrite each others restart logs.
Use record count insert to restart at last write also enable restart logfile [default: False]


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


## Field Files

Each file you intend to upload must have a field file defining the
contents of the CSV file you plan to upload.

If a fieldfile is not explicitly passed in the program will look for a
fieldfile corresponding to the file name with the extension replaced
by `.ff`. So for an input file `inventory.csv` the corresponding field
file would be `inventory.ff`.

If there is no corresponding field file the upload will fail.

Field files (normally expected to have the extension `.ff`) define the names of columns and their
types for the importer. A field file is formatted line a
[python config file](https://docs.python.org/2/library/configparser.html)
with each section defined by a name inside square brackets ( `[` and `]` ) and values for
the section defined by `key=value` pairs.

For a csv file [inventory.csv](https://github.com/jdrumgoole/pymongo_import/blob/master/test/inventory.csv) defined by the following format,


Inventory Item|Amount|Last Order
---|---:|---
Screws|300|1-Jan-2016
Bolts|150|3-Feb-2017
Nails|25|31-Dec-2017
Nuts|75|29-Feb-2016

The field file generated by **--genfieldfile** is

```
[Inventory Item]
type=str
[Amount]
type=int
[Last Order]
type=datetime
```

The generate field file function uses the first line after the header
line to guess the type of each column. It tries in order for each
column to successfully convert the type to a string (str), integer
(int), float (float) or date (datetime).

The generate function may guess wrong if the first line is not
correctly parseable. In this case the user can edit the .ff file to
correct the types.

In any case if the type conversion fails when reading the actual
data-file the program will degenerate to converting to a string
without failing (unless [--onerror fail](#onerror)  is specified).

Each file in the input list must correspond to a field file format that is
common across all the files.

## Date Fields
Date fields are special and support processing options. There are three types
of date field.

* date : A normal date generally without a timestamp
* datetime : fully qualified date including a timestamp.
* isodate : A normal date in the standard ISO format YYYY-MM-DD. 

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
