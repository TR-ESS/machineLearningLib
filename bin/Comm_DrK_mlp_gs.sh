#!/bin/sh
targetAsset=$1
targetAssetDB=$2
assetlist=$3
trmidblist=$4


columnlist="buzz sentiment optimism fear joy trust violence conflict gloom stress timeUrgency uncertainty emotionVsFact longShort longShortForecast priceDirection priceForecast volatility consumptionVolume regulatoryIssues supplyVsDemand supplyVsDemandForecast"

#columnlist="uncertainty"

#assetlist="CRU NAP NGS POIL COR SOY1 WHT"
#assetlist="CRU NAP NGS POIL"
#trmidblist=(trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_agr_1h trmidb.com_agr_1h trmidb.com_agr_1h trmidb.com_agr_1h)
#trmidblist="trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_enm_1h trmidb.com_agr_1h"

cd $HOME/mllib/sql

fname=`../command/compositeSQL.cmd 'tickdb.'$targetAssetDB "$assetlist" "$trmidblist" "News" $targetAsset "$columnlist" "sum"`
fname=`echo '../dataDrK/'$targetAsset'_News_sum.csv'`
trmiAssetNames=`echo $assetlist | sed -e 's/ /_/g'`
echo $fname
interval=10
c=20
#echo 'assetCode,trmiAsset,trmiIndex,short,long,corrSharp,avrSharp' > ../dataDrK/LCO_eval.csv
#for colName in $columnlist
#do

c=20
while true
do
    if [ $c -gt 100 ]; then
	break
    fi
    let d=($c+$interval)

    for colName in $columnlist
    do

	echo $colName'_'$c'_'$d
	python ../command/computeCorrelation.py -i $fname -c "windowTimestamp assetCode Open Last $colName" -s $c -l $d
	mv '../dataDrK/'$targetAsset'_News_sum_corr.csv' '../dataDrK/'$targetAsset'_News_'$c'_'$d'_'$colName'_corr.csv'
    done
    lsstr=`echo '../dataDrK/'$targetAsset'_News_'$c'_'$d'_*corr.csv'`
    ls -1 $lsstr > $targetAsset'_'$c'_'$d'_corr.log'

    ### BACKTESTING
    e=20
    while true
    do
	if [ $e -gt 100 ]; then
	    break
	fi
	python ../command/computeDrKmlp.py -i $targetAsset'_'$c'_'$d'_corr.log' -c Avr -s $c -l $d -n $e -p -1 &> $targetAsset'_'$trmiAssetNames'_mlp_eval_result_'$c'_'$d'_'$e'.log'

	let e=($e+10)
    done

    let c=($c+$interval)
done
#done 

echo 'TotalSharpRatio,WinningRage,Short,Long,Observe,AccPL' > $targetAsset'_'$trmiAssetNames'_mlp_eval_result.log'
more $targetAsset'_'$trmiAssetNames'_mlp_eval_result*log' | grep -v double_scalars | grep -v np.std | grep -v Namespace | grep -v TotalSharp | grep -v ::: | grep -v .log >> $targetAsset'_'$trmiAssetNames'_mlp_eval_result.log'


