#!/bin/sh
assetName=$1
assetTickDbName=$2
assetlist=$3
trmidbs=$4
sspan=$5
lspan=$6
sourceType=$7
storage=$8

if [ "$sourceType" == "" ]; then
    sourceType="News"
fi

if [ "$storage" == "" ]; then
    storage="gs://tr-jp-analytics/arc"
fi
columnlist="buzz sentiment optimism fear joy trust violence conflict gloom stress timeUrgency uncertainty emotionVsFact longShort longShortForecast priceDirection priceForecast volatility consumptionVolume regulatoryIssues supplyVsDemand supplyVsDemandForecast"

#assetlist="CRU NAP NGS POIL COR SOY1 WHT"
#trmidblist=(trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_agr_1h trmidb.com_agr_1h trmidb.com_agr_1h trmidb.com_agr_1h)

trmidblist=($trmidbs)

cd $HOME/mllib/sql

c=0
for asset in $assetlist
do
    echo $asset
    dbname=`echo ${trmidblist[$c]}`
    echo $dbname
    for indices in $columnlist
    do
	echo "-----$indices"
	fname=`../command/manipulateMultiAssetsColSQL.cmd tickdb.$assetTickDbName "$asset" "$dbname" "$sourceType" $assetName "$indices" "none"`
	fname=`echo $fname | awk '{print $1}'`
	if [ -e ../data/$fname ]; then
	    ofname=`echo $fname | sed 's/.csv/_corr.csv/g'`
	    echo "Good file with $fname, going to calculate correlation in $ofname"
	    python ../command/computeCorrelation.py -i ../data/$fname -c "windowTimestamp assetCode Open Last $asset$indices" -s $sspan -l $lspan
	    if [ -e ../data/$ofname ]; then
		# LOAD CORREL DATA INTO BIG QUERY
		table=`echo $ofname | sed 's/.csv//g'`
		bq rm -f -t research.$table
		gsutil cp ../data/$ofname $storage
		bq load  --replace --source_format=CSV --field_delimiter=',' --autodetect research.$table $storage/$ofname
		# GIVEN PARAMETERS
		echo 'assetName,dataType,shortSpan,longSpan,trmiAsset' > _tmpParam
		echo "$assetName,$sourceType,$sspan,$lspan,$asset" >> _tmpParam
		table=`echo $ofname | sed 's/.csv/_param/g'`
		bq rm -f -t research.$table
		gsutil cp _tmpParam $storage
		bq load --replace --source_format=CSV --field_delimiter=',' --autodetect research.$table $storage/_tmpParam
	    fi
	fi
    done
    let c=($c+1)
done 
