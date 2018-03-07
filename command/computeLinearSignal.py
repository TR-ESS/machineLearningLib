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
#####################################################################
### COMMAND ARGS VALIDATION
#####################################################################
parser = argparse.ArgumentParser(description='computeLinearPerformance... ')
# ADD INPUT FILE ARGUMENT
parser.add_argument('-i', '--in_file', \
                    action='store', \
                    type=str, \
                    help='File Path to be loaded, should be CSV file layout', \
                    metavar=None)
# ADD INPUT FILE ARGUMENT
parser.add_argument('-c', '--columns', \
                    action='store', \
                    type=str, \
                    help='Target column to be used by backtesting. e.g. "windowTimestamp assetCode Open Last Correl15CRUsentiment Correl20CRUsentiment Avr15CRUsentiment Avr20CRUsentiment"', \
                    metavar=None)


try:
    args=parser.parse_args()
except Exception as e:
    print (e.message())
    #quit()

if args.in_file is None:
    print ('in_file not specified, required')
    quit()
if args.columns is None:
    print ('columns is not specified, required')
    quit()
print (args)
pInFile=args.in_file
pCols=args.columns.split(' ')
pTrmiVal1=pCols[4] #CORREL SHORT COL NAME
pTrmiVal2=pCols[5] #CORREL LONG COL NAME
pTrmiVal3=pCols[6] #AVR SHORT COL NAME
pTrmiVal4=pCols[7] #AVR LONG COL NAME




#####################################################################
## COMPUTE Correlation
#####################################################################
ml=mldm()
## LOAD INPUT CSV FILE HERE
ml.loadData(pInFile,',')
if ml.getLoadStatus() == False:
    print ('In FILE ' + pInFile + ' not exist')
    quit()

## EXTRACT TARGET COLUMNS
ml.extractCols(pCols)
retdf=ml.getDF()
if retdf is not None:
    retdf=ml.computeLinearSignal(pTrmiVal1, pTrmiVal2, pTrmiVal3, pTrmiVal4, 1)
    if retdf is not None:
        outFile=pInFile.replace('_corr.csv','_sig.csv')
        retdf.to_csv(outFile,sep=',',index=False)
        print(outFile)

        


 

