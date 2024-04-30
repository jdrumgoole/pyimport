#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#


PYPIUSERNAME="jdrumgoole"
ROOT=${HOME}/GIT/pyimport

#
# Hack the right PYTHONPATH into make subshells.
#
SHELL:=PYTHONPATH=${ROOT} ${SHELL}

all: test_all build test_build
	-@echo "Ace King, Check it out! A full build"

build: clean
	python3 -m build

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
	(export PYTHONPATH=`pwd` && python pyimport/pyimport_main.py -h > /dev/null)
	(export PYTHONPATH=`pwd` && cd test && python ../pyimport/pyimport_main.py --delimiter '|' data/10k.txt > /dev/null)
	(export PYTHONPATH=`pwd` && python pyimport/pymongomultiimport_main.py -h > /dev/null )
	(export PYTHONPATH=`pwd` && python pyimport/pwc.py -h > /dev/null )
	(export PYTHONPATH=`pwd` && python pyimport/splitfile.py -h > /dev/null )

test_data:
	(export PYTHONPATH=`pwd` && cd test && python ../pyimport/pymongomultiimport_main.py --fieldfile data/yellow_tripdata.tff --poolsize 2  data/yellow_tripdata_2015-01-06-200k.csv.1 data/yellow_tripdata_2015-01-06-200k.csv.2  ) #> /dev/null 2>&1)
	(rm yellow_tripdata_2015-01-06-200k.csv.*)
	(export PYTHONPATH=`pwd` && cd test && python ../pyimport/splitfile.py --autosplit 4 data/100k.txt > /dev/null )
	(export PYTHONPATH=`pwd` && cd test && python ../pyimport/pymongomultiimport_main.py --delimiter "|" --poolsize 2 100k.txt.[12] > /dev/null )
	(rm 100k.txt.* > /dev/null 2>&1)

test_all: nose test_scripts

nose:
	which python
	nosetests

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

