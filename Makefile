#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#


PYPIUSERNAME="jdrumgoole"
ROOT=${HOME}/GIT/pyimport
PYTHONPATH=${ROOT}
#
# Hack the right PYTHONPATH into make subshells.


SHELL:=PYTHONPATH=${ROOT} ${SHELL}

all: test_all build test_build
	-@echo "Ace King, Check it out! A full build"


build: clean
	python -m build

path:
	@echo PATH=${PATH}


pythonpath:
	@echo "PYTHONPATH=${PYTHONPATH}"

root:
	@echo "The project ROOT is '${ROOT}'"


python_bin:
	python -c "import os;print(os.environ.get('USERNAME'))"
	which python

prod_build:build test
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/* -u jdrumgoole

test_build:build test
	twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u jdrumgoole

#
# Just test that these scripts run
#
test_scripts:
	poetry run python pyimport/pyimport_main.py -h > /dev/null
	poetry run python pyimport/pyimport_main.py --delimiter '|' ./test/test_mot/10k.txt > /dev/null
	poetry run python pyimport/pymultiimport_main.py -h > /dev/null
	poetry run python pyimport/pwc.py -h > /dev/null
	poetry run python pyimport/splitfile.py -h > /dev/null

test_data:
	poetry run python pyimport/splitfile.py --autosplit 4 test/data/100k.txt > /dev/null
	poetry run python pyimport/pymultiimport_main.py --fieldfile test/data/100k.tff --delimiter "|" --poolsize 2 100k.txt.[1234] > /dev/null
	rm 100k.txt.* > /dev/null 2>&1

split_file:
	poetry run python pyimport/splitfile.py --autosplit 4 test/data/100k.txt > /dev/null
	rm 100k.txt.* > /dev/null 2>&1

test_yellowtrip:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv

test_multi:
	poetry run python pyimport/dropdb.py --database PYIM
	poetry run python pyimport/splitfile.py --autosplit 2 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv #> /dev/null
	poetry run python pyimport/pymultiimport_main.py --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --poolsize 3  yellow_tripdata_2015-01-06-200k.csv.1 yellow_tripdata_2015-01-06-200k.csv.2 yellow_tripdata_2015-01-06-200k.csv.3  > /dev/null
	rm yellow_tripdata_2015-01-06-200k.csv.*
	rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff
	#poetry run python pyimport/dropdb.py --database PYIM

test_small_multi:
	head -n 5000 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > yellow_tripdata_2015-01-06-5k.csv
	poetry run python pyimport/splitfile.py --autosplit 2 yellow_tripdata_2015-01-06-5k.csv
	poetry run python pyimport/pyimport_main.py --genfieldfile yellow_tripdata_2015-01-06-5k.csv #> /dev/null
	poetry run python pyimport/pymultiimport_main.py --database SMALL --collection yellowcab --fieldfile yellow_tripdata_2015-01-06-5k.tff --poolsize 2 yellow_tripdata_2015-01-06-5k.csv.1 yellow_tripdata_2015-01-06-5k.csv.2  > /dev/null
	#rm yellow_tripdata_2015-01-06-5k.csv.*
	rm yellow_tripdata_2015-01-06-5k.tff



genfieldfile:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	#rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff

test_all: pytest test_scripts

pytest:
	(cd test/test_command && poetry run pytest)
	(cd test/test_config && poetry run pytest)
	(cd test/test_e2e && poetry run pytest)
	(cd test/test_fieldfile && poetry run pytest)
	(cd test/test_file_processor && poetry run pytest)
	(cd test/test_filesplitter && poetry run pytest)
	(cd test/test_http_import && poetry run pytest)
	(cd test/test_linecounter && poetry run pytest)
	(cd test/test_linereader && poetry run pytest)
	(cd test/test_mot && poetry run pytest)
	(cd test/test_splitfile && poetry run pytest)
	(cd test/test_general && poetry run pytest)

test_top:
	(cd test && poetry run pytest)

test_install:
	pip install --extra-index-url=https://pypi.org/ -i https://test.pypi.org/simple/ pyimport

clean:
	rm -rf build dist

pkgs:
	pipenv install pymongo keyring twine nose semvermanager

init: pkgs
	keyring set https://test.pypi.org/legacy/ ${USERNAME}
	keyring set https://upload.pypi.org/legacy/ ${USERNAME}

collect:
	python pyimport

