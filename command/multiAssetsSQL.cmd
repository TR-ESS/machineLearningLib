#!/bin/sh
# Created on 2018 01 29
# Prepared by Noriyuki Suzuki / Market Development Manager / Thomson Reuters
# The purpose of this script is to obtain target asset price and coresponding TRMI indices
# Mandatory Inbound Parameters for this command
# Assumption: Both Tick History and TRMI on Google Big Query
# $1: Tick History DB name
# $2: TRMI DB name
# $3: TRMI AssetCode name
# $4: TRMI DataType name
# $5: Output File name, will be created under ../data/ folder
tickdb=$1
assetCodes=$2
trmidbs=$3
dataType=$4
targetAsset=$5

trmidblist=($trmidbs)
count=0
echo `date`

outputFile=$targetAsset

echo 'select * from '$tickdb' AS PRC ' > ./multiJoin.sql
for asset in $assetCodes
do
    outputFile=`echo $outputFile'_'$asset`
    echo 'inner join (select * from '${trmidblist[$count]}' where assetCode='"'"$asset"'"' and dataType='"'"$dataType"'"') AS '$asset' ON PRC.windowTimestamp='$asset'.windowTimestamp ' >> ./multiJoin.sql
    count=$count+1
done

echo 'order by PRC.windowTimestamp' >> ./multiJoin.sql

cat ./multiJoin.sql

outputFile=`echo $outputFile'_'$dataType'.csv'`

# BQ QUERY with eliminate brank line
bq query -n 10000000 --format csv < ./multiJoin.sql | grep -v '^\s*$'  > ../data/$outputFile

