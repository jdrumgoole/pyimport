#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#


PYPIUSERNAME="jdrumgoole"
ROOT=${HOME}/GIT/pyimport
PYTHONPATH=${ROOT}
PATH=/bin:/usr/bin:/usr/local/mongodb/bin:/Users/jdrumgoole/.pyenv/shims/
#
# Hack the right PYTHONPATH into make subshells.

testenv:
	@echo "AUDITHOST" is: "$(AUDITHOST)"


SHELL:=PYTHONPATH=${ROOT} ${SHELL}

all: test_all build test_build
	-@echo "Ace King, Check it out! A full build"


build: test_all
	poetry build

publish: build
	poetry publish

path:
	@echo AUDITHOST=${AUDITHOST}


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



quick_test : std_quicktest async_quicktest thread_quicktest multi_quicktest

std_quicktest:
	poetry run python pyimport/pyimport_main.py --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

long_test:

audit_quicktest:
	poetry run python pyimport/pyimport_main.py --audit --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported

async_quicktest:
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt # > /dev/null
	poetry run python pyimport/pyimport_main.py --asyncpro --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

thread_quicktest:
	poetry run python pyimport/pyimport_main.py --audit --thread --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --asyncpro --thread --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

multi_quicktest:
	poetry run python pyimport/pyimport_main.py --splitfile --multi --poolsize 2   --delimiter '|' --fieldfile ./test/test_mot/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --splitfile --multi --poolsize 2   --audit --delimiter '|' --fieldfile ./test/test_mot/10k.tff ./test/test_command/120lines.txt > /dev/null
	@poetry run python pyimport/dbop.py --count PYIM.imported
	poetry run python pyimport/dbop.py --drop PYIM.imported > /dev/null

test_audit:
	poetry run python pyimport/pyimport_main.py --audit --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --multi --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --multi --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --threads --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
	poetry run python pyimport/pyimport_main.py --audit --threads --asyncpro --delimiter '|' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt > /dev/null
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
	poetry run python pyimport/pyimport_main.py --audit --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --audit --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --async ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --splitfile --multi  --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --async ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/pyimport_main.py --audit --asyncpro --splitfile --threads  --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv
	@poetry run python pyimport/dbop.py --drop PYIM.imported
	@rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff


test_multi:
	poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	poetry run python pyimport/pyimport_main.py  --multi --splitfile --autosplit 10 --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --poolsize 4 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
	@rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff
	@poetry run python pyimport/dbop.py --drop PYIM.imported

test_threads:
	poetry run python pyimport/pyimport_main.py   --asyncpro --threads --poolsize 8 --splitfile --autosplit 8 --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > /dev/null
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
	#rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff

mongoimport:
	mongoimport --db test --collection yellowcab --type csv --columnsHaveTypes --numInsertionWorkers=8 --fieldFile test/test_mongoimport/yellow_trip_data_10.mff  --file test/test_mongoimport/yellow_tripdata_200_noheader.csv
	poetry run python pyimport/pyimport_main.py --hasheader --forkmethod spawn --asyncpro --multi --splitfile --autosplit 10 --poolsize 8  --fieldfile ./test/test_command/yellow_trip.tff ./test/test_command/yellow_tripdata_2015-01-06-200k.csv
	poetry run python pyimport/dbop.py --drop PYIM.imported
	poetry run python pyimport/dbop.py --drop test.yellowcab


missing_records:
	poetry run python pyimport/pyimport_main.py --keepsplits --splitfile --autosplit 10 --hasheader --fieldfile ./test/test_command/yellow_trip.tff ./test/test_command/yellow_tripdata_2015-01-06-200k.csv

test_all_scripts: test_scripts test_audit test_multi test_small_multi test_yellowtrip test_data

test_all: pytest test_all_scripts

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


clean:
	rm -rf build dist

poetry_build:
	poetry build

poetry_publish:
	poetry publish
