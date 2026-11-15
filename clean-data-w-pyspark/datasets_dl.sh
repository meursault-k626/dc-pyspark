#!usr/bin/bash

for url in \
    "https://assets.datacamp.com/production/repositories/4336/datasets/f412f5acbef38630147c2956d8703bafbcd0f74c/AA_DFW_2014_Departures_Short.csv.gz" \
    "https://assets.datacamp.com/production/repositories/4336/datasets/c0aa672020bce21eef0d875a484a4fd44da042cf/DallasCouncilVoters.csv.gz" \
    "https://assets.datacamp.com/production/repositories/4336/datasets/ea700976560a6f1760782c6e7310a662120c63b5/DallasCouncilVotes.csv.gz" \
    "https://assets.datacamp.com/production/repositories/4336/datasets/c1abacafea802998597d6c68b27c7b8650a18ab8/AA_DFW_2016_Departures_Short.csv.gz" \
    "https://assets.datacamp.com/production/repositories/4336/datasets/04db01ffbd39f7bf2f88ffd5b7924b2de0419168/AA_DFW_2017_Departures_Short.csv.gz" \
    "https://assets.datacamp.com/production/repositories/4336/datasets/475d2803541ba8facb2c39024dd0d9497859dc6c/AA_DFW_2015_Departures_Short.csv.gz"
do
    curl -fLO "$url"
done

cp AA_DFW_2017_Departures_Short.csv.gz AA_DFW_2018.csv.gz

# for f in *.gz; do
#     gunzip -kf "$f"
# done
