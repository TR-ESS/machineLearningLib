# coding: utf-8  
## COMPUTING CORRELATION INDICES
#  2018 02 01
#  Noriyuki Suzuki
#  Params
#       1: In File Path (csvFileName)
#       2: Target Cols (e.g. "LastPrc,stress")
#       3: Correlation Computing Time Span
#       4: Output Result File Path
from indicesManipulator import machineLearningDataManipulation as mldm
import sys
import os
import pandas as pd
import argparse
from operator import itemgetter

#####################################################################
### COMMAND ARGS VALIDATION
#####################################################################
parser = argparse.ArgumentParser(description='computeLinearPerformance... ')
# ADD INPUT FILE ARGUMENT
parser.add_argument('-i', '--in_file', \
                    action='store', \
                    type=str, \
                    help='File Path to be loaded, should contain list of all PRICE-TRMI file path', \
                    metavar=None)
# ADD INPUT FILE ARGUMENT
parser.add_argument('-s', '--short_span', \
                    action='store', \
                    type=int, \
                    help='Short span of ovservation', \
                    metavar=None)
parser.add_argument('-l', '--long_span', \
                    action='store', \
                    type=int, \
                    help='Long span of ovservation', \
                    metavar=None)
parser.add_argument('-c', '--criteria', \
                    action='store', \
                    type=str, \
                    help='Which span shoud be applied? (Short or Long)', \
                    metavar=None)
parser.add_argument('-n', '--num_rank', \
                    action='store', \
                    type=int, \
                    help='Number of Learning Data', \
                    metavar=None)
parser.add_argument('-m', '--pickup_method', \
                    action='store', \
                    type=str, \
                    help='Pick Up methodology of Leaarning Data e.g. positive negative neutral', \
                    metavar=None)

try:
    args=parser.parse_args()
except Exception as e:
    print (e.message())
    #quit()

if args.in_file is None:
    print ('in_file not specified, required')
    quit()
if args.short_span is None:
    print ('Did you specify Short Span of Observation?')
    quit()
if args.long_span is None:
    print ('Did you specify Long Span of Observation?')
    quit()
if args.num_rank is None:
    print ('Number of Ranking is not specified, required')
    quit()
if args.criteria is None:
    print ('Criteria, which span should be applied? Short or Long')
    quit()
if args.pickup_method is None:
    print ('Which Pickup Method, would you like to apply? Positive or Negative or Neutral')
    quit()
print (args)
pInFile=args.in_file
pShort=args.short_span
pLong=args.long_span
pNumRank=args.num_rank
pMethod=args.pickup_method
pCriteria=args.criteria
pNumSpan=pShort
#####################################################################
## COMPUTE Correlation
#####################################################################

_df=None
_dfp=None
prc=None

count=0
pColForSpan='CorrelShort'
if pCriteria == "Short":
    pColForSpan='CorrelShort'
    pNumSpan=pShort
elif pCriteria == "Long":
    pColForSpan='CorrelLong'
    pNumSpan=pLong
else:
    pColForSpan='CorrelShort'
    pNumSpan=pShort

if os.path.exists(pInFile):
    f=open(pInFile)
    line=f.readline()
    while line:
        # FILE NAME with REMOVING RETURN CODE
        line=line.split('\n')[0]
        # PICKUP COLUMN NAME FROM FILE NAME
        _t=line.split('/')
        _t2=_t[len(_t)-1].split('_')
        tCol=_t2[1]+_t2[3]

        # CREATE Machine Lerning Instance for each FILE
        ml=mldm()
        ml.loadData(line,',')
        if ml.getLoadStatus() == True:
            if count == 0: # IMPORTANT, STORING TAREGT ASSET CLASS MARKET PRICE HISTORY
                ml.extractCols(['windowTimestamp','assetCode','Open','Last'])
                prc=ml.getDF()
            ml.extractCols(['windowTimestamp',pColForSpan])
            retdf=ml.getDF()

            if retdf is not None:
                retdf=retdf.rename(columns={pColForSpan : tCol})

                if count == 0:
                    _df=retdf
                else:
                    retdf=retdf.drop('windowTimestamp',axis=1)
                    try:
                        _df=pd.concat([_df, retdf],join='inner',axis=1)
                    except Exception as e:
                        print ('Exception detected on '+tCol)
                
                # DEBUG print (tCol+'-'+str(len(retdf))+'-'+str(len(_df)))
        line=f.readline()
        count+=1
    f.close()
#DEBUG next 2 lines
#print  (_df[['windowTimestamp','NGSsupplyVsDemand']])
#print (prc)


### IMPORTANT PROCESS, CONSTRUCT CORREL TOP X ranking based on the  methodology which was specified as
### input parameter ('Positive', 'Negative' and 'Neutral' are valid param for methodology
### Positive : TOP X Positive Correl
### Negative : TOPX Negative Correl
### Neutral :  TOP X LOW CORREL
corrRank=[]
negCorrelPosition=0
posCorrelPosition=1
neuCorrelPosition=2
corrRank=ml.getCorrelRankingList(_df,pNumSpan,pNumRank)

supervisedData=[]
signalData=[]
if len(corrRank) > 0:
    # CONSTRUCT SUPERVISED DATA FROM PRICE HIST
    # Open-Close > 0  --> -1
    # Open-Close < 0  --> 1
    # Open-Close == 0 --> 0
#    supervisedData=ml.getSupervisedData(prc)
    #print (supervisedData)
#    signalData=ml.computeDecisionTreeSignal(_df,corrRank ,pNumSpan,pNumRank,neuCorrelPosition,supervisedData, 20, 30)
#    sigDf=pd.DataFrame(signalData,columns=['windowTimestamp','Open','Last','Signal'])
#    sigDf.to_csv('./sig.csv',sep=',',index=False)

    ### COMPUTING PERFORMANCE 
    ml.loadData('./sig.csv',',')
    if ml.getLoadStatus() == True:
        ml.extractCols(['windowTimestamp','Open','Last','Signal'])
        pfmDf=ml.computePerformance2()
        pfmDf.to_csv('./pfm.csv',sep=',',index=False)

    ### EVALUATE MODEL
    ml.loadData('./pfm.csv',',')
    if ml.getLoadStatus() == True:
        ml.extractCols(['windowTimestamp','PL','AccPL'])
        evalDf=ml.computePerformanceEvaluation2()
        evalDf.to_csv('./eval.csv',sep=',',index=False)

#print ('len Supervised='+str(len(supervisedData)))
#print ('len CorrRanling='+str(len(corrRank[neuCorrelPosition])))






        


 

