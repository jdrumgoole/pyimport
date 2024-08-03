#!/bin/bash
poetry run python mdbutils/aggregateops.py rename --db SO --src survey --dest rename_1 --field DatabaseWorkedWith --newfield DatabaseHaveWorkedWith
poetry run python mdbutils/aggregateops.py rename --db SO --src rename_1 --dest survey_normalised --field HaveWorkedDatabase --newfield DatabaseHaveWorkedWith
poetry run python mdbutils/aggregateops.py listify --db SO --src survey_normalised --dest survey_listify --field DatabaseHaveWorkedWith --separator ";"