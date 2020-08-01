#!/bin/bash
ARG=
if [ -z "${1}" ];
then
ARG="-P $1"
fi

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-02.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-03.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-04.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-05.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-06.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-07.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-08.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-09.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-10.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-11.csv $ARG
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-12.csv $ARG
