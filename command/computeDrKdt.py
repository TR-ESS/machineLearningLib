#coding: utf-8  
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
import numpy as np
import math
from sklearn import tree

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
                    help='Which span shoud be applied? (Correl or Avr)', \
                    metavar=None)
parser.add_argument('-n', '--num_observe', \
                    action='store', \
                    type=int, \
                    help='Number of Learning Data', \
                    metavar=None)

parser.add_argument('-p', '--position_direction', \
                    action='store', \
                    type=int, \
                    help='Signal Direction 1 or -1', \
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
if args.num_observe is None:
    print ('Number of Observe is not specified, required')
    quit()
if args.criteria is None:
    print ('Criteria, which span should be applied? Correl or Avr')
    quit()
print (args)
pInFile=args.in_file
pShort=args.short_span
pLong=args.long_span
pNumObserve=args.num_observe
pCriteria=args.criteria # Correl or Avr
pDirection=args.position_direction
pNumSpan=pShort
#####################################################################
## COMPUTE Correlation
#####################################################################

_df=None
_dfp=None
prc=None

count=0
pColShort='CorrelShort'
pColLong='CorrelLong'
if pCriteria == "Correl":
    pColShort='CorrelShort'
    pColLong='CorrelLong'
    pNumSpan=pLong
elif pCriteria == "Avr":
    pColShort='AvrShort'
    pColLong='AvrLong'
    pNumSpan=pLong
else:
    pColShort='CorrelShort'
    pColLong='CorrelLong'
    pNumSpan=pLong

if os.path.exists(pInFile):
    f=open(pInFile)
    line=f.readline()
    while line:
        # FILE NAME with REMOVING RETURN CODE
        line=line.split('\n')[0]
        # PICKUP COLUMN NAME FROM FILE NAME
        _t=line.split('/')
        _t2=_t[len(_t)-1].split('_')
        tColShort=_t2[4]+_t2[2]
        tColLong=_t2[4]+_t2[3]
        tCol=_t2[4]
        # CREATE Machine Lerning Instance for each FILE
        ml=mldm()
        ml.loadData(line,',')
        if ml.getLoadStatus() == True:
            if count == 0: # IMPORTANT, STORING TAREGT ASSET CLASS MARKET PRICE HISTORY
                ml.extractCols(['windowTimestamp','assetCode','Open','Last'])
                prc=ml.getDF()
            ml.extractCols(['windowTimestamp',pColShort,pColLong])
            retdf=ml.getDF()

            sdf=pd.DataFrame(retdf['windowTimestamp'])
            sdf[tCol]=retdf[pColShort] - retdf[pColLong]

            if sdf is not None:
                if count == 0:
                    _df=sdf
                else:
                    sdf=sdf.drop('windowTimestamp',axis=1)
                    try:
                        _df=pd.concat([_df, sdf],join='inner',axis=1)
                    except Exception as e:
                        print ('Exception detected on '+tCol)
                
                # DEBUG print (tCol+'-'+str(len(retdf))+'-'+str(len(_df)))
        line=f.readline()
        count+=1
    f.close()
#DEBUG next 2 lines

#print (_df)

### IMPORTANT PROCESS, CONSTRUCT CORREL TOP X ranking based on the  methodology which was specified as
### input parameter ('Positive', 'Negative' and 'Neutral' are valid param for methodology
### Positive : TOP X Positive Correl
### Negative : TOPX Negative Correl
### Neutral :  TOP X LOW CORREL
totalRow=len(_df)
startPos=(totalRow % (pNumObserve*2))
endPos=totalRow-(pNumObserve*2)+1
#print (totalRow,startPos,endPos)
indices=_df.columns
#print (indices)

tAccPL=0
tPL=0
tapl=[]
tpl=[]
testStartAt=0
testEndAt=0
dryStartAt=0
dryEndAt=0

negativeIndices=[ ['buzz',1], ['conflict',-1], ['emotionVsFact',1], ['fear',-1], ['gloom',-1],['joy',1], ['longShort',1], ['longShortForecast',1], ['optimism',1], ['priceDirection',1],['priceForecast',1], ['sentiment',1], ['stress',-1], ['timeUrgency',-1], ['trust',1], ['uncertainty',-1], ['violence',-1], ['volatility',-1] ]

for i in range(startPos,totalRow-(pNumObserve*2)+1,pNumObserve):

    ### FIND BEST SHARP RATIO
    #if i == startPos:
    testStartAt=i
    testEndAt=i+pNumObserve-1
    #else:
    #    testStartAt=dryStartAt+1
    #    testEndAt=dryEndAt+1
    #print ('TESTING FROM '+str(testStartAt)+'-'+str(testEndAt))
    pPrc=[]
    pPrc=prc.iloc[testStartAt : testEndAt+1][['windowTimestamp','Last','Open']]
    sharpRatios=[]
    for x in range(1,len(indices)):
        ix=[]
        ix=_df.iloc[testStartAt : testEndAt+1][[indices[x]]]
        #print ('====>'+indices[x])
        pl=[]
        apl=[]
        accPL=0
        for m in range(0,pNumObserve-1):
            lDate=[]
            lPL=0
            lDate.append(pPrc.iloc[m+1]['windowTimestamp'])
            if ix.iloc[m][indices[x]] > 0:
                lPL = pPrc.iloc[m+1]['Last'] - pPrc.iloc[m+1]['Open'] # Last - Open
            elif ix.iloc[m][indices[x]] < 0:
                lPL = pPrc.iloc[m+1]['Open'] - pPrc.iloc[m+1]['Last'] # Open - Last
            else:
                lPL = 0 # NO ACTION
            accPL = accPL + lPL
            apl.append(lPL) # PL array for mean and std
            pl.append([lDate,lPL,accPL])
            #print (lDate)
        avrR=np.mean(apl)
        sharpR=(np.mean(apl) / np.std(apl))*100
        sharpRatios.append([sharpR,x])
        #print (indices[x]+' Sharp Ratio = '+str(sharpR) + ' : Average ='+str(avrR))

    ### SharpRatios Sort
    ### Sorting by best sharp ratio
    #pDirection=1
    sharpRanks=sharpRatios
    sharpRanks.sort() # Acsending sort
    sharpRanks.reverse() # reverse = Decsending sort
    if math.isnan(sharpRanks[0][0]) == True:
        continue

    highIndices=[]
    for x in range(0,10):
        highIndices.append(indices[sharpRanks[x][1]])
    #print (highIndices)
    tPrc=prc.iloc[testStartAt+1 : testEndAt+2][['windowTimestamp','Open','Last']]
    tDf=_df.iloc[testStartAt : testEndAt+1][highIndices].fillna(0).values.tolist()
    deltaPrc=(tPrc['Open'] - tPrc['Last']).values.tolist()
    for x in range(0,len(deltaPrc)):
        if deltaPrc[x] > 0:
            deltaPrc[x] = 1
        elif deltaPrc[x] < 0:
            deltaPrc[x] = -1
        else:
            deltaPrc[x] = 0
    #print (tPrc)
    #print (tDf)
    #print (deltaPrc)

    ### Decision Tree Training
    wRate=0
    bestDepth=3
    clf=tree.DecisionTreeClassifier()
    for x in range(3,20):
        clf=tree.DecisionTreeClassifier(max_depth=x)
        clf=clf.fit(tDf,deltaPrc)
        predicted=clf.predict(tDf)
        wRate=sum(predicted == deltaPrc) / len(deltaPrc)
        if wRate > 0.999:
            bestDepth=x
            break





    ### DRY RUN
    j=testEndAt#+1
    dryStartAt=j
    dryEndAt=j+pNumObserve-1
    #print ('DRY RUN FROM '+str(dryStartAt)+'-'+str(dryEndAt))
    dPrc=prc.iloc[dryStartAt+1 : dryEndAt+1+1][['windowTimestamp','Last','Open']]
    dateArr=dPrc['windowTimestamp'].values.tolist()
    deltaPrc=(dPrc['Last'] - dPrc['Open']).values.tolist()
    deltaPrc0=(dPrc['Last'] - dPrc['Open']).values.tolist()
    for x in range(0,len(deltaPrc)):
        if deltaPrc[x] > 0:
            deltaPrc[x] = 1
        elif deltaPrc[x] < 0:
            deltaPrc[x] = -1
        else:
            deltaPrc[x] = 0
    ddf=_df[highIndices][dryStartAt : dryEndAt+1].values.tolist()
    predicted=clf.predict(ddf)
    wRate=sum(predicted == deltaPrc) / len(deltaPrc)
    #print ('Winning Rate = '+str(wRate))
    for x in range(0,len(deltaPrc)):
        tPL = predicted[x] * deltaPrc0[x] * pDirection
        tpl.append(tPL)
        tAccPL = tAccPL + tPL
        tapl.append(tAccPL)
        #print (dateArr[x],tPL,tAccPL)

mAvr=np.mean(tpl)
mStd=np.std(tpl)
win=0
lose=0
mSharp=(mAvr/mStd)*100
for x in range(0,len(tpl)):
    if tpl[x] >= 0:
        win+=1
    else:
        lose+=1
mwRate=( win / (win+lose))*100
print ('TotalSharpRatio,WinningRage,Short,Long,Observe,AccPL')
print (str(mSharp)+',' +str(mwRate)+','+str(pShort) + ',' + str(pLong)+','+str(pNumObserve)+','+str(tAccPL),flush=True)

#    dix=_df[[indices[bestPos]]][dryStartAt : dryEndAt+1]
    
#    for m in range(0,pNumObserve):
#        lPL=0
#        lDate=''
#        if dix.iloc[m][indices[bestPos]] > 0:
#            lPL = (dPrc.iloc[m]['Last'] - dPrc.iloc[m]['Open']) * pDirection
#        elif dix.iloc[m][indices[bestPos]] < 0:
#            lPL = (dPrc.iloc[m]['Open'] - dPrc.iloc[m]['Last']) * pDirection
#        else:
#            lPL=0
#        tAccPL=tAccPL + lPL
#        lDate=dPrc.iloc[m]['windowTimestamp']
#        tapl.append([lDate,lPL,tAccPL])
#        tpl.append(lPL)
#        #print (lDate + ',' + str(lPL) + ',' + str(tAccPL), flush=True)

##print (tapl)
#tSharp=(np.mean(tpl) / np.std(tpl))*100
#print ('Total Sharp Ratio = ' + str(tSharp) + ' Span = '+str(pShort) + '/' + str(pLong), flush=True)

