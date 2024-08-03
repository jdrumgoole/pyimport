#!/bin/sh
export PYTHONPATH=$HOME/GIT/pyimport
IMPCMD="poetry run python pyimport/pyimport_main.py --hasheader --database SO --collection survey"
python mdbutils/dbop.py --drop SO
${IMPCMD} --addfield "year=2017" $HOME/stackoverflow/survey_results_public_2017.csv
${IMPCMD} --addfield "year=2018" $HOME/stackoverflow/survey_results_public_2018.csv
${IMPCMD} --addfield "year=2019" $HOME/stackoverflow/survey_results_public_2019.csv
${IMPCMD} --addfield "year=2020" $HOME/stackoverflow/survey_results_public_2020.csv
${IMPCMD} --addfield "year=2021" $HOME/stackoverflow/survey_results_public_2021.csv
${IMPCMD} --addfield "year=2022" $HOME/stackoverflow/survey_results_public_2022.csv
${IMPCMD} --addfield "year=2023" $HOME/stackoverflow/survey_results_public_2023.csv