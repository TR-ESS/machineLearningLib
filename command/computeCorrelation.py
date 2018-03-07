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
parser = argparse.ArgumentParser(description='computeCorrelation... ')
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
                    help='Column list e.g. "windowTimestamp assetCode Open Last TRMI_index"', \
                    metavar=None)
# ADD COMPUTE TIME SPAN ARGUMENT
parser.add_argument('-s', '--short_span', \
                    action='store', \
                    type=int, \
                    help='Compute Short Time Span to be applied for Correlation calculation', \
                    metavar=None)
parser.add_argument('-l', '--long_span', \
                    action='store', \
                    type=int, \
                    help='Compute Long Time Span to be applied for Correlation calculation', \
                    metavar=None)

try:
    args=parser.parse_args()
except Exception as e:
    print (e.message())
    #quit()
print (args)
if args.in_file is None:
    print ('in_file not specified, required')
    quit()
if args.columns is None:
    print ('columns is not specified, required')
    quit()
if args.short_span is None:
    print ('short span is not specified, required')
    quit()
if args.long_span is None:
    print ('long span is not specified, required')
    quit()

pInFile=args.in_file
pCols=args.columns.split(' ')
pSpan= args.short_span
pSpan2=args.long_span

colNameOpen=pCols[2]
colNameLeft=pCols[3]
colNameRight=pCols[4]

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
    retdf=ml.computeTmCorrel2(colNameOpen, colNameLeft, colNameRight, pSpan, pSpan2)
    if retdf is not None:
        outFile=pInFile.replace('.csv','_corr.csv')
        retdf.to_csv(outFile,sep=',',index=False)
        print(outFile)

        


 

