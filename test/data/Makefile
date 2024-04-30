#
# Test some data
#

mot_mi:
	mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast 1m.txt

mot_mi_4:
	mongoimport --columnsHaveTypes --fieldFile mongoimport.ff --type csv --parseGrace autoCast --numInsertionWorkers 4 1m.txt

mot_pymi:
	python ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10k.tff 1m.txt

mot_pymi_iso:
	python ../../pyimport/pyimport_main.py  --delimiter '|' --fieldfile 10kiso.tff 1m.txt
