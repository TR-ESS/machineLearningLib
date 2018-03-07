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
trmidb=$2
assetCode=$3
dataType=$4
outputFile=$5

echo `date`
echo "Obtaining Query Result from $tickdb and $trmidb with $assetCode based on $dataType"

SQL=`echo 'SELECT  a.windowTimestamp windowTimestamp, a.assetCode assetCode, a.Open Open, a.Last Last, b.buzz buzz, b.relativeBuzz relativeBuzz, b.sentiment sentiment, b.optimism optimism, b.fear fear, b.joy joy, b.trust trust, b.violence violence, b.conflict conflict, b.gloom gloom, b.stress stress, b.timeUrgency timeUrgency, b.uncertainty uncertainty, b.emotionVsFact emotionVsFact, b.longShort longShort, b.longShortForecast longShortForecast, b.priceDirection priceDirection, b.priceForecast priceForecast, b.volatility volatility, b.consumptionVolume consumptionVolume, b.productionVolume productionVolume, b.regulatoryIssues regulatoryIssues, b.supplyVsDemand supplyVsDemand, b.supplyVsDemandForecast supplyVsDemandForecast FROM '$tickdb' a Inner join '$trmidb' b on a.windowTimestamp=b.windowTimestamp where b.assetCode='"'"$assetCode"'"' and b.dataType='"'"$dataType"'"' order by windowTimestamp'`

# BQ QUERY with eliminate brank line
bq query -n 10000000 --format csv $SQL | grep -v '^\s*$'  > ../data/$outputFile

