#!/bin/sh
# Created on 2018 01 29
# Prepared by Noriyuki Suzuki / Market Development Manager / Thomson Reuters
# The purpose of this script is to obtain target asset price and coresponding TRMI indices
# Mandatory Inbound Parameters for this command
# Assumption: Both Tick History and TRMI on Google Big Query
# $1: Tick History DB name
# $2: Asset Name list in TRMI
# $3: Corresponding TRMI database name
# $4: TRMI DataType name
# $5: Target Asset Class Name
# $6: Target Columns list to be extracted from TRMI database
# $7: Compute Operator: sum / subtr / mult to compute extracted columns value
tickdb=$1
assetCodes=$2
trmidbs=$3
dataType=$4
targetAsset=$5
targetColNames=$6
compOperator=$7


trmidblist=($trmidbs)
trmicollist=($targetColNames)
targetColName=''
indicateName=`echo $compOperator'_'`
#echo `date`

outputFile=$targetAsset

# CONSTRUCT COL of SUM TARGET VALUE
sumCol='('
count=0
for asset in $assetCodes
do
    if [ $count == 0 ]; then
	sumCol=`echo $sumCol' '$asset''$count'.'${trmicollist[$count]}`
	targetColName=`echo $targetColName''${trmicollist[$count]}`
	if [ "$compOperator" == "none" ]; then
	    indicateName=`echo $asset''${trmicollist[$count]}`
	else
	    indicateName=`echo $indicateName''$asset''${trmicollist[$count]}`
	fi
    else
	if [ "$compOperator" == "sum" ]; then
	    sumCol=`echo $sumCol' + '$asset''$count'.'${trmicollist[$count]}`
	elif [ "$compOperator" == "subtr" ]; then
	    sumCol=`echo $sumCol' - '$asset''$count'.'${trmicollist[$count]}`
	elif [ "$compOperator" == "none" ]; then
	    sumCol=`echo $sumCol' + '$asset''$count'.'${trmicollist[$count]}`
	else
	    echo "$compOperator is not supported, sorry"
	    exit
	fi
	targetColName=`echo $targetColName'_'${trmicollist[$count]}`
	indicateName=`echo $indicateName'_'$asset''${trmicollist[$count]}`
    fi
    let count=($count + 1)
done
sumCol=`echo $sumCol' ) '$indicateName`
# ADD COL OF EACH ASSET TARET COL
count=0

for asset in $assetCodes
do
    sumCol=`echo $sumCol', '$asset''$count'.'${trmicollist[$count]}`
    let count=($count + 1)
done
sumCol=`echo $sumCol' '`
# echo $sumCol

echo 'select PRC.windowTimestamp windowTimestamp, PRC.assetCode assetCode, PRC.Open Open, PRC.Last Last, '$sumCol' from '$tickdb' AS PRC ' > ./computeMultiJoin.sql
count=0
for asset in $assetCodes
do
    outputFile=`echo $outputFile'_'$asset`
    subCols=`echo 'windowTimestamp,'${trmicollist[$count]}`
    echo 'inner join (select '$subCols' from '${trmidblist[$count]}' where assetCode='"'"$asset"'"' and dataType='"'"$dataType"'" and windowTimestamp "> '"2009-12-31T23:59:59.000Z"'"') AS '$asset''$count' ON PRC.windowTimestamp='$asset''$count'.windowTimestamp ' >> ./computeMultiJoin.sql
    let count=($count + 1)
done

echo 'order by PRC.windowTimestamp' >> ./computeMultiJoin.sql

#cat ./subtrMultiJoin.sql

tableName=`echo $outputFile'_'$dataType'_'$targetColName'_'$compOperator`
outputFile=`echo $outputFile'_'$dataType'_'$targetColName'_'$compOperator'.csv'`

### VERY IMPORTANT ECHO, it will be used by caller
echo $outputFile

# BQ QUERY with eliminate brank line
bq query -n 10000000 --format csv < ./computeMultiJoin.sql | grep -v '^\s*$'  > ../data/$outputFile

# BQ VIEW CREATION ONTO RESEARCH DB
viewsql=`cat ./computeMultiJoin.sql`

bq rm -f -t research.$tableName
bq mk --view="$viewsql" research.$tableName
