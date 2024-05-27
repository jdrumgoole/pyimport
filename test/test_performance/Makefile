#
# Test some data
#

export PYTHONPATH=${HOME}/GIT/pyimport

all : mongodb pyimport

mongodb: mot_mi mot_mi_4

pyimport: mot_pymi mot_pymi_iso mot_multi

mot_mi:
	/usr/local/mongodb/bin/mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast 1m.txt

mot_mi_4:
	/usr/local/mongodb/bin/mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast --numInsertionWorkers 4 1m.txt

mot_pymi:
	poetry run python ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10k.tff 1m.txt

mot_pymi_iso:
	poetry run python ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10kiso.tff 1m.txt

split:
	poetry run python ../../pyimport/splitfile.py --input --autosplit 5 1m.txt

mot_pymulti:
	poetry run python ../../pyimport/splitfile.py --autosplit 5 1m.txt --input > list_of_files.txt
	poetry run python ../../pyimport/pymultiimport_main.py --poolsize 5 --delimiter '|' --fieldfile 10k.tff `cat list_of_files.txt`
	rm  `cat list_of_files.txt`
	rm list_of_files.txt

