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


build: test_all
	poetry build

publish: build
	poetry publish

path:
	@echo PATH=${PATH}


pythonpath:
	@echo "PYTHONPATH=${PYTHONPATH}"

root:
	@echo "The project ROOT is '${ROOT}'"


python_bin:
	python -c "import os;print(os.environ.get('USERNAME'))"
	which python

#
# Just test that these scripts run
#

quicktest:
	poetry run python pyimport/pyimport_main.py --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

async_quicktest:
	poetry run python pyimport/pyimport_main.py --asyncpro --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

multi_quicktest:
	poetry run python pyimport/pyimport_main.py --splitfile --multi --poolsize 2   --delimiter '|' --fieldfile ./test/test_mot/10k.tff ./test/test_mot/10k.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

test_scripts:
	poetry run python pyimport/pyimport_main.py -h > /dev/null  2>&1
	poetry run python pyimport/pyimport_main.py --delimiter '|' ./test/test_mot/10k.txt > /dev/null 2>&1
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/pyimport_main.py --asyncpro --delimiter '|' ./test/test_mot/10k.txt > /dev/null 2>&1
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/pwc.py -h > /dev/null 2>&1
	poetry run python pyimport/splitfile.py -h > /dev/null 2>&1
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null 2>&1

test_data:
	poetry run python pyimport/pyimport_main.py --drop --multi --splitfile --autosplit 4 --fieldfile test/data/100k.tff --delimiter "|" --poolsize 2 test/data/100k.txt > /dev/null
	poetry run python pyimport/dbop.py --drop PYIM.imported

split_file:
	poetry run python pyimport/pyimport_main.py --splitfile  --fieldfile test/data/100k.tff --delimiter "|" test/data/100k.txt > /dev/null


test_yellowtrip:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/dbop.py --drop PYIM.imported
	rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff

test_yellowtrip_async:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --asyncpro --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --async ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	@poetry run python pyimport/dbop.py --drop PYIM.imported
	@rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff


test_multi:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	poetry run python pyimport/pyimport_main.py  --multi --splitfile --autosplit 10 --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --poolsize 4 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	@rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff
	@poetry run python pyimport/dbop.py --drop PYIM.imported

test_small_multi:
	head -n 5000 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > yellow_tripdata_2015-01-06-5k.csv
	poetry run python pyimport/splitfile.py --autosplit 2 yellow_tripdata_2015-01-06-5k.csv
	poetry run python pyimport/pyimport_main.py --genfieldfile yellow_tripdata_2015-01-06-5k.csv #> /dev/null
	poetry run python pyimport/pyimport_main.py --database SMALL --collection yellowcab --splitfile --autosplit 2 --fieldfile yellow_tripdata_2015-01-06-5k.tff --poolsize 2 yellow_tripdata_2015-01-06-5k.csv  > /dev/null
	rm yellow_tripdata_2015-01-06-5k.tff yellow_tripdata_2015-01-06-5k.csv
	poetry run python pyimport/dbop.py --drop SMALL.yellowcab

genfieldfile:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff

test_all: pytest test_scripts test_multi test_small_multi test_yellowtrip test_yellowtrip_async test_data

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

