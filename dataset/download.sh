#!/bin/bash
curl -L -o ./duolingo-spaced-repetition-data.zip\
  https://www.kaggle.com/api/v1/datasets/download/aravinii/duolingo-spaced-repetition-data

unzip ./duolingo-spaced-repetition-data.zip

mv ./learning_traces.13m.csv ./duolingo-spaced-repetition-data.csv

rm ./duolingo-spaced-repetition-data.zip