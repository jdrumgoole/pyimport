#
# Test some data
#

export PYTHONPATH=${HOME}/GIT/pyimport

mot_mi:
	/usr/local/mongodb/bin/mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast 1m.txt

mot_mi_4:
	/usr/local/mongodb/bin/mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast --numInsertionWorkers 4 1m.txt

mot_pymi:
	poetry run coverage run ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10k.tff 1m.txt

mot_pymi_iso:
	python ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10kiso.tff 1m.txt
ls
