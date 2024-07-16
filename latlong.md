# Representing Latitude and Longtitude in a TFF file

The pyimport program uses TOML as a spec for its configuration files. A
TFF file (TOML Field File) defines the way that fields in a CSV file get
mapped to the fields in a document. Each row of a CSV file becomes a single
document in a collection. The field names are the column titles and the
cel rows become the corresponding values.

If we have this data in a file called `dob.csv`:
```
Date Of Birth, First Name, Last Name, Shoe Size
1/2/2000, Peter, Philips, 11
2/3/2001, John, Smith, 9
4/6/1998, Alexis, Jones, 10
```

Then we can generate a field file from the data using this command:



Can be converted using this field file using the following command:
```bash
$ python pyimport_main.py --genfieldfile test/data/dob.csv
2021-07-29 13:35:59,131: pyimport_main.py:pyimport_main:184: INFO: Forcing has_header true for --genfieldfile
2021-07-29 13:35:59,133: command.py:post_execute:95: INFO: Created field filename
'test/data/dob.tff' from 'test/data/dob.csv'
```

This is what `dob.tff` will look like.
```
[Date of Birth]
type=datetime
[First Name]
type=str
[Last Name]
type=str
[Shoe Size]
type=int
```

We can convert the dob.csv file into a set of MongoDB documents using the command:

```bash
python pyimport/pyimport_main.py test/data/dob.csv --hasheader
```
This will generate the follow documents in the collection `imported` in
the database `PYIM`:

```json5
{ _id: ObjectId("61029dd9bcc4e7b1a1e90d41"),
  'Date Of Birth': 2000-01-02T00:00:00.000Z,
  'First Name': ' Peter',
  'Last Name': ' Philips',
  'Shoe Size': 11 }
{ _id: ObjectId("61029dd9bcc4e7b1a1e90d42"),
  'Date Of Birth': 2001-02-03T00:00:00.000Z,
  'First Name': ' John',
  'Last Name': ' Smith',
  'Shoe Size': 9 }
{ _id: ObjectId("61029dd9bcc4e7b1a1e90d43"),
  'Date Of Birth': 1998-04-06T00:00:00.000Z,
  'First Name': ' Alexis',
  'Last Name': ' Jones',
  'Shoe Size': 10 }
```
Imagine we had a data file `longlat.csv` with these longtitude and latitude values:

```
pickup_longitude,pickup_latitude
-73.987686157226562,40.724250793457031
-73.991569519042969,40.726932525634766
-73.981918334960938,40.783443450927734
```

We could convert the file but MongoDB would not be able to interpret it as a GeoSpatial data because
MongoDB expects a [GeoJSON](https://docs.mongodb.com/manual/reference/geojson/) coordinate format.
The format of a GeoJSON point is:

```json5
{ type: "Point", coordinates: [ <longitude>, <latitude> ] }
```
To convert to a GeoJSON point we must map the longtitude and latitude fields into this format.

To do this we must create a new TFF field type we will call `$location`.

The TFF to convert the `longlat.csv` file will be:

```json5
[$location]
  name:pickup location
  longitude: pickup_longitude
  latitude: pickup_latitude
```

`[$location]` is a metafield know to the parser that indicates we are specifying a field that will
not be found as a column in the input CSV file. Instead this document will called by its name (it will
default to `location` if name is not specified) and will specify a GeoJSON point.

Hence the output data will become:

```json5
{ "type" : "Point", "coordinates" : [-73.987686157226562,40.724250793457031]}
{ "type" : "Point", "coordinates" : [-73.991569519042969,40.726932525634766]}
{ "type" : "Point", "coordinates" : [-73.981918334960938,40.783443450927734]}
```

This can be parsed and indexed by MongoDB as GeoJSON data. 
