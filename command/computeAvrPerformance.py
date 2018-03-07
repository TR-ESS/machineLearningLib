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




try:
    args=parser.parse_args()
except Exception as e:
    print (e.message())
    #quit()

if args.in_file is None:
    print ('in_file not specified, required')
    quit()

pInFile=args.in_file

#####################################################################
## COMPUTE Performance
#####################################################################
ml=mldm()
## LOAD INPUT CSV FILE HERE
ml.loadData(pInFile,',')
if ml.getLoadStatus() == False:
    print ('In FILE ' + pInFile + ' not exist')
    quit()

## EXTRACT TARGET COLUMNS
ml.extractCols(['windowTimestamp','assetCode','Open','Last','SignalAvr'])
retdf=ml.getDF()
if retdf is not None:
    retdf=ml.computeAvrPerformance()
    if retdf is not None:
        outFile=pInFile.replace('_sig.csv','_pfm.csv')
        retdf.to_csv(outFile,sep=',',index=False)
        print(outFile)

        


 

