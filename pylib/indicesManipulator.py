# coding: utf-8
## Indices Manipulator Python Module
#  2017 12 07
#  Noriyuki Suzuki
#  Class : indicesManipurator
#  Method
#   1: loadData(pathname,sep) return DataFrame
#   2: extractCols(column-list) return DataFrame (extracting specific columns data in 'column-list')

import os
import sys
import pandas as pd
import numpy as np
import math
from scipy.stats.stats import pearsonr
from sklearn import linear_model as lm
from operator import itemgetter
from sklearn import tree
from sklearn.model_selection import GridSearchCV

class indicesManipulator:
    _indicesFilePath = ''
    df = None
    targetdf = None
    targetlist=[]

    ### LOAD CSV DATA FILE
    def loadData(self,path, sep):
        self.df = None
        if os.path.exists(path):
            self.df=pd.read_csv(path, sep)

    ### GET FILE LOAD STATUS
    def getLoadStatus(self):
        return self.df

    ### GET CURRENT TARGET DATAFRAME
    def getDF(self):
        return self.targetdf

    ### EXTRACT TARGET COLUMN DATA AS DATAFRAME
    def extractCols(self, cols):
        if self.df is not None:
            self.targetlist=[]
            for i in range(0,len(cols)): # Traverse each column
                if cols[i] in self.df:
                    self.targetlist.append(cols[i])
            self.targetdf=pd.DataFrame(self.df[self.targetlist])

    ### GENERATE SUPERVISOR DATA FROM TARGET DATAFRAME
    def generateSuperviseData(self,thresholds, returnspan=1,fowardspan=0):
        if returnspan < 1:
            return False
        if self.targetdf is not None:
            siz=len(self.targetdf)
            sizc=len(self.targetdf.columns)

            ##### CONSTRUCT THRESHOLD LIST AS A SCALE OF GENERATING SUPERVISE DATA
            thrarray=[]
            for i in range(0,len(thresholds)):
                thrarray.append(thresholds[i])
            thrarray.append(0)
            for i in range(0,len(thresholds)):
                thrarray.append((-1) * thresholds[len(thresholds) - 1 - i])

            ##### INTERNAL FOR NORMALIZE RETURN TO SUPERVISED DATA FOR MACHINE LEARNING
            def threshold_manage(x):
                n = 1
                if math.isnan(x):
                    return 0
                for i in range(0,len(thrarray)):
                    if x >= thrarray[i]:
                        n = len(thrarray) - i + 1
                        break
                return (n)
            ### - DEBUG should be remoevd in live
            # threshold_manage(0.75)

            ##### GENERATE RETURN USING 'returnspan'
            for i in range(1,len(self.targetlist)):
                shiftColName='shift_' + self.targetlist[i]
                self.targetdf[shiftColName] = self.targetdf[self.targetlist[i]].shift(returnspan)
                returnColName='return_' + self.targetlist[i]
                self.targetdf[returnColName] = self.targetdf[self.targetlist[i]] - self.targetdf[shiftColName]

            ##### GENERATE SUPERVISED DATA USING 'returnColName' COLUMN
            for i in range(1,len(self.targetlist)):
                supColName='supervise_' + self.targetlist[i]
                returnColName='return_' + self.targetlist[i]
                self.targetdf[supColName]=self.targetdf[returnColName].apply(threshold_manage)

            ##### FORWARD SUPERVISED DATA USING 'forwardspan'
            if fowardspan != 1:
                for i in range(1,len(self.targetlist)):
                    fwdSuperviseColName='forward_supervise_' + self.targetlist[i]
                    supColName='supervise_' + self.targetlist[i]
                    self.targetdf[fwdSuperviseColName]=self.targetdf[supColName].shift(fowardspan)

            ##### DROP NaN DATA RECORDS FROM TARET DATAFRAME
            self.targetdf=self.targetdf.dropna()
            return True


### ADDED 2017 12 14 BY NORIYUKI SUZUKI                
class machineLearningDataManipulation:
    df = None
    df2 = None
    targetdf = None
    targetlist=[]

    ### LOAD CSV DATA FILE
    def loadData(self,path, sep):
        self.df = None
        if os.path.exists(path):
            self.df=pd.read_csv(path, sep)
    ### DELETE CVS DATA FRAME
    def freeData(self):
        del self.df

    ### MAP EXTERNAL DATAFRAME TO DF IN THIS CLASS
    def setData(self, edf):
        self.df = edf
    def setPair(self,edf):
        self.df2 = edf

    ### GET DATAFRAME STATUS
    def getLoadStatus(self):
        if self.df is not None:
            return True
        else:
            return False

    ### EXTRACT COLUMN
    def extractCols(self, cols):
        if self.df is not None:
            self.targetlist=[]
            for i in range(0,len(cols)): # Traverse each column
                if cols[i] in self.df:
                    self.targetlist.append(cols[i])
            self.targetdf=pd.DataFrame(self.df[self.targetlist])
            del self.targetlist

    ### GET CURRENT TARGET DATAFRAME
    def getDF(self):
        return self.targetdf
    def freeDF(self):
        del self.targetdf

    ### COMPUTE TIME SERIES CORRELATION B/W DF AND DF2
    def computeTmCorrel(self, key1, key2, assetColName, colNames, span=[10]):
        _df=pd.merge(self.df2,self.targetdf,how='inner', left_on=key1, right_on=key2)
        if len(_df) < min(span):
            return None
        if assetColName not in self.df2:
            return None
        for i in range(0,len(colNames)):
            if colNames[i] in self.targetdf:
                for j in range(0,len(span)):
                    corrColName='corr_'+colNames[i] + '_' + str(span[j])
                    _assetdf=None
                    _carray=[]
                    for n in range(0,span[j]-1):
                        _carray.append(None)
                    for n in range(0,len(_df)-span[j] + 1):
                        _assetdf=_df[assetColName][n:n+span[j]]
                        _indexdf=_df[colNames[i]][n:n+span[j]]
                        p = pearsonr(_assetdf, _indexdf)
                        _carray.append(p[0])
                    _df=pd.concat([_df,pd.DataFrame(_carray,columns=[corrColName])],axis=1)
        return _df

    ### COMPUTE TIME SERIES CORRELATION B/W COL1 and COL2
    def computeTmCorrel2(self, colnameOpen, colnameLeft,colnameRight, maspan=10, maspan2=20):
        _df =None
        ### VALIDATE COL LEFT AND RIGHT EXIST
        if colnameLeft not in self.targetdf:
            return _df
        if colnameRight not in self.targetdf:
            return _df

        date=self.targetdf['windowTimestamp']
        asset=self.targetdf['assetCode']
        opn=self.targetdf[colnameOpen]
        prc=self.targetdf[colnameLeft]
        val=self.targetdf[colnameRight]

        ### RIGHT COLUMN MOVING AVERAGE & STANDARD DEVIATION
        avrarray=val.rolling(window=maspan,min_periods=maspan).mean()
        avr=pd.DataFrame({'AvrShort' : avrarray})

        stdarray=val.rolling(window=maspan,min_periods=maspan).std()
        std=pd.DataFrame({'StdShort' : stdarray})

        avrarray2=val.rolling(window=maspan2,min_periods=maspan2).mean()
        avr2=pd.DataFrame({'AvrLong' : avrarray2})

        stdarray2=val.rolling(window=maspan2,min_periods=maspan2).std()
        std2=pd.DataFrame({'StdLong' : stdarray2})

        avrdf=pd.concat([date,asset,opn,prc,val,avr,avr2,std,std2],axis=1)

        ### CORRELATION
        ## Short Span Correlation
        corrarray=[]
        for i in range(0,maspan-1):
            corrarray.append(None)
        for i in range(0,len(prc) - maspan + 1):
            prcArr=prc[i:i+maspan]
            valArr=val[i:i+maspan]
            p = pearsonr(prcArr, valArr)
            corrarray.append(p[0])
        corr=pd.DataFrame({'CorrelShort' : corrarray})

        ## Long Span Correlation
        corrarray=[]
        for i in range(0,maspan2-1):
            corrarray.append(None)
        for i in range(0,len(prc) - maspan2 + 1):
            prcArr=prc[i:i+maspan2]
            valArr=val[i:i+maspan2]
            p = pearsonr(prcArr, valArr)
            corrarray.append(p[0])
        corr2=pd.DataFrame({'CorrelLong' : corrarray})

        _df=pd.concat([avrdf,corr,corr2], axis=1)

        return _df

    ### OBTAIN TRADE SIGNAL WITH SIMPLE LINEAR MODEL
    def computeLinearSignal(self,colName1, colName2, colName3, colName4,positionDirection):
        ### colName1 - Correl Short Col Name
        ### colName2 - Correl Long Col Name
        ### colName3 - Moving Avr Short Col Name
        ### colName4 - Moving Avr Long Col Name
        _df =None
        ### VALIDATE COL LEFT AND RIGHT EXIST                                                                       
        if colName1 not in self.targetdf:
            return _df
        if colName2 not in self.targetdf:
            return _df
        if colName3 not in self.targetdf:
            return _df
        if colName4 not in self.targetdf:
            return _df

        ### EXTRACT VALUES
        tm=self.targetdf['windowTimestamp']
        ast=self.targetdf['assetCode']
        opn=self.targetdf['Open']
        prc=self.targetdf['Last']
        v1=self.targetdf[colName1]
        v2=self.targetdf[colName2]
        v3=self.targetdf[colName3]
        v4=self.targetdf[colName4]

        ### SIGNAL GENERATION
        sig=[] # for Sig from Correl
        sig2=[] # for Sig from Moving Avr

        ### FOLLOWING TWO APPEND IS VERY IMPORTANT of SIGNAL GENERATION for NEXT TIME FRAME
        sig.append(None)
        sig2.append(None)
        for i in range(0,len(prc)):
            ### GENERATING SIGNAL FROM CORRELATION in B/w PRICE and TRMI
            if math.isnan(v1[i]) or math.isnan(v2[i]):
                sig.append(None)
            else:
                if v2[i] - v1[i] > 0:
                    sig.append(1*positionDirection)
                elif v2[i] == v1[i]:
                    sig.append(0)
                else:
                    sig.append(-1*positionDirection)
            ### GENERATING SIGNAL FROM MOVING AVERAGE of TRMI
            if math.isnan(v3[i]) or math.isnan(v4[i]):
                sig2.append(None)
            else:
                if v4[i] - v3[i] > 0:
                    sig2.append(1*positionDirection)
                elif v4[i] == v3[i]:
                    sig2.append(0)
                else:
                    sig2.append(-1*positionDirection)
        wt=pd.DataFrame({'windowTimestamp' : tm})
        at=pd.DataFrame({'assetCode' : ast})
        op=pd.DataFrame({'Open' : opn})
        pr=pd.DataFrame({'Last' : prc})
        sg=pd.DataFrame({'SignalCorrel' : sig})
        sg2=pd.DataFrame({'SignalAvr' : sig2})
        _df=pd.concat([wt,at,op,pr,sg,sg2], axis=1)
        return _df

    def computeAvrSignal(self,colName1,positionDirection):
        ### colName1 - Moving Avr Short Col Name

        _df =None
        ### VALIDATE COL LEFT AND RIGHT EXIST                                                                       
        if colName1 not in self.targetdf:
            return _df

        ### EXTRACT VALUES
        tm=self.targetdf['windowTimestamp']
        ast=self.targetdf['assetCode']
        opn=self.targetdf['Open']
        prc=self.targetdf['Last']
        v1=self.targetdf[colName1]


        ### SIGNAL GENERATION
        sig=[] # for Sig from Correl


        ### FOLLOWING TWO APPEND IS VERY IMPORTANT of SIGNAL GENERATION for NEXT TIME FRAME
        sig.append(None)
        for i in range(0,len(prc)):
            ### GENERATING SIGNAL FROM CORRELATION in B/w PRICE and TRMI
            if math.isnan(v1[i]):
                sig.append(None)
            else:
                if v1[i] > 0:
                    sig.append(1*positionDirection)
                elif v1[i] == 0:
                    sig.append(0)
                else:
                    sig.append(-1*positionDirection)

        wt=pd.DataFrame({'windowTimestamp' : tm})
        at=pd.DataFrame({'assetCode' : ast})
        op=pd.DataFrame({'Open' : opn})
        pr=pd.DataFrame({'Last' : prc})
        sg=pd.DataFrame({'SignalAvr' : sig})
        _df=pd.concat([wt,at,op,pr,sg], axis=1)
        return _df

    def computePerformance(self):
        _df =None
        ### EXTRACT VALUES
        tm=self.targetdf['windowTimestamp']
        ast=self.targetdf['assetCode']
        opn=self.targetdf['Open']
        prc=self.targetdf['Last']
        sig=self.targetdf['SignalCorrel']
        sig2=self.targetdf['SignalAvr']

        newtm=[]
        newat=[]
        newopn=[]
        newlast=[]
        # FOR CORREL
        newsig=[]
        pl=[]
        accpl=[]
        # FOR MOVING AVR
        newsig2=[]
        pl2=[]
        accpl2=[]

        prevpl=0 # FOR CORREL
        prevAcc=0 # FOR CORREL
        prevpl2=0 # FOR AVR
        prevAcc2=0 # FOR AVR
        ### SIGNAL GENERATION
        for i in range(0,len(prc)):
            try:
                ret=math.isnan(tm[i])
            except Exception as e:
                ret=False
            if ret == False: #Record not for next signal
                newtm.append(tm[i])
                newat.append(ast[i])
                newopn.append(opn[i])
                newlast.append(prc[i])
                newsig.append(sig[i])
                newsig2.append(sig2[i])
                ret2=math.isnan(sig[i]) # same for sig2[i]
                if ret2 == True: # There is no signal
                    pl.append(0)
                    pl2.append(0)
                else: # There is a signal
                    ### PL FROM CORREL SIGNAL
                    if sig[i] == 1:
                        pl.append(prc[i]-opn[i])
                        prevpl=prc[i]-opn[i]
                    elif sig[i] == 0:
                        pl.append(0)
                        prevpl=0
                    else:
                        pl.append(opn[i] - prc[i])
                        prevpl=opn[i] - prc[i]

                    ### PL FROM MOVING AVR SIGNAL
                    if sig2[i] == 1:
                        pl2.append(prc[i]-opn[i])
                        prevpl2=prc[i]-opn[i]
                    elif sig2[i] == 0:
                        pl2.append(0)
                        prevpl2=0
                    else:
                        pl2.append(opn[i] - prc[i])
                        prevpl2=opn[i] - prc[i]
                if i == 0:
                    accpl.append(0)
                    prevAcc=0
                    accpl2.append(0)
                    prevAcc2=0
                else:
                    accpl.append(prevAcc+prevpl)
                    prevAcc=prevAcc+prevpl
                    accpl2.append(prevAcc2+prevpl2)
                    prevAcc2=prevAcc2+prevpl2

                    
        t=pd.DataFrame({'windowTimestamp' : newtm})
        n=pd.DataFrame({'assetCode' : newat})
        o=pd.DataFrame({'Open' : newopn})
        c=pd.DataFrame({'Last' : newlast})
        s=pd.DataFrame({'SignalCorrel' : newsig})
        p=pd.DataFrame({'PLCorrel' : pl})
        a=pd.DataFrame({'AccPLCorrel' : accpl})
        s2=pd.DataFrame({'SignalAvr' : newsig2})
        p2=pd.DataFrame({'PLAvr' : pl2})
        a2=pd.DataFrame({'AccPLAvr' : accpl2})
        _df=pd.concat([t,n,o,c,s,p,a,s2,p2,a2],axis=1)
        return _df

    def computeAvrPerformance(self):
        _df =None
        ### EXTRACT VALUES
        tm=self.targetdf['windowTimestamp']
        ast=self.targetdf['assetCode']
        opn=self.targetdf['Open']
        prc=self.targetdf['Last']
        sig=self.targetdf['SignalAvr']

        newtm=[]
        newat=[]
        newopn=[]
        newlast=[]
        # FOR MOVING AVR
        newsig=[]
        pl=[]
        accpl=[]

        prevpl=0 # FOR AVR
        prevAcc=0 # FOR AVR

        ### SIGNAL GENERATION
        for i in range(0,len(prc)):
            try:
                ret=math.isnan(tm[i])
            except Exception as e:
                ret=False
            if ret == False: #Record not for next signal
                newtm.append(tm[i])
                newat.append(ast[i])
                newopn.append(opn[i])
                newlast.append(prc[i])
                newsig.append(sig[i])

                ret2=math.isnan(sig[i]) # same for sig2[i]
                if ret2 == True: # There is no signal
                    pl.append(0)

                else: # There is a signal
                    ### PL FROM AVERAGE
                    if sig[i] == 1:
                        pl.append(prc[i]-opn[i])
                        prevpl=prc[i]-opn[i]
                    elif sig[i] == 0:
                        pl.append(0)
                        prevpl=0
                    else:
                        pl.append(opn[i] - prc[i])
                        prevpl=opn[i] - prc[i]


                if i == 0:
                    accpl.append(0)
                    prevAcc=0
                else:
                    accpl.append(prevAcc+prevpl)
                    prevAcc=prevAcc+prevpl

                    
        t=pd.DataFrame({'windowTimestamp' : newtm})
        n=pd.DataFrame({'assetCode' : newat})
        o=pd.DataFrame({'Open' : newopn})
        c=pd.DataFrame({'Last' : newlast})
        s=pd.DataFrame({'SignalAvr' : newsig})
        p=pd.DataFrame({'PLAvr' : pl})
        a=pd.DataFrame({'AccPLAvr' : accpl})
        _df=pd.concat([t,n,o,c,s,p,a],axis=1)
        return _df

    def computePerformance2(self): #FOR MACHINE LEARNING ONE
        _df =None
        ### EXTRACT VALUES
        tm=self.targetdf['windowTimestamp']
        opn=self.targetdf['Open']
        prc=self.targetdf['Last']
        sig=self.targetdf['Signal']

        newtm=[]
        newopn=[]
        newlast=[]
        # FOR CORREL
        newsig=[]
        pl=[]
        accpl=[]

        prevpl=0
        prevAcc=0

        ### SIGNAL GENERATION
        for i in range(0,len(prc)):
            try:
                ret=math.isnan(tm[i])
            except Exception as e:
                ret=False
            if ret == False: #Record not for next signal
                newtm.append(tm[i])
                newopn.append(opn[i])
                newlast.append(prc[i])
                newsig.append(sig[i])
                ret2=math.isnan(sig[i]) # same for sig2[i]
                if ret2 == True: # There is no signal
                    pl.append(0)
                else: # There is a signal
                    ### PL FROM CORREL SIGNAL
                    if sig[i] == 1:
                        pl.append(prc[i]-opn[i])
                        prevpl=prc[i]-opn[i]
                    elif sig[i] == 0:
                        pl.append(0)
                        prevpl=0
                    else:
                        pl.append(opn[i] - prc[i])
                        prevpl=opn[i] - prc[i]

                if i == 0:
                    accpl.append(0)
                    prevAcc=0
                else:
                    accpl.append(prevAcc+prevpl)
                    prevAcc=prevAcc+prevpl

                    
        t=pd.DataFrame({'windowTimestamp' : newtm})
        o=pd.DataFrame({'Open' : newopn})
        c=pd.DataFrame({'Last' : newlast})
        s=pd.DataFrame({'Signal' : newsig})
        p=pd.DataFrame({'PL' : pl})
        a=pd.DataFrame({'AccPL' : accpl})
        _df=pd.concat([t,o,c,s,p,a],axis=1)
        return _df

    ### COMPUTE PERFORMANCE EVALUATION
    def computePerformanceEvaluation(self):
        _df=None
        pl1=self.targetdf['PLCorrel']
        pl2=self.targetdf['PLAvr']

        x=0 #Total Num Trade
        w=0 #Total Num Win Trade
        l=0 #Total Num Lose Trade
        t=0 #Total PL
        for i in range(0,len(pl1)):
            if pl1[i] > 0:
                w+=1
                x+=1
            elif pl1[i] < 0:
                l+=1
                x+=1
            else:
                x+=1
            t=t+pl1[i]

        pl1Win=(w / x) * 100
        pl1Avr=(t / x)
        pl1SP=(pl1Avr / (np.std(pl1))) * 100 

        x=0 #Total Num Trade
        w=0 #Total Num Win Trade
        l=0 #Total Num Lose Trade
        t=0 #Total PL
        for i in range(0,len(pl2)):
            if pl2[i] > 0:
                w+=1
                x+=1
            elif pl2[i] < 0:
                l+=1
                x+=1
            else:
                x+=1
            t=t+pl2[i]

        pl2Win=(w / x) * 100
        pl2Avr=(t / x)
        pl2SP=(pl2Avr / (np.std(pl2))) * 100 

        arpl1Win=[pl1Win]
        arpl1Avr=[pl1Avr]
        arpl1SP=[pl1SP]
        arpl2Win=[pl2Win]
        arpl2Avr=[pl2Avr]
        arpl2SP=[pl2SP]
        
        p1w=pd.DataFrame({'CorrelWinRate' : arpl1Win})
        p1a=pd.DataFrame({'CorrelAverage' : arpl1Avr})
        p1s=pd.DataFrame({'CorrelSharpRatio' : arpl1SP})
        p2w=pd.DataFrame({'MovingAvrWinRate' : arpl2Win})
        p2a=pd.DataFrame({'MovingAvrAverage' : arpl2Avr})
        p2s=pd.DataFrame({'MovingAvrSharpRatio' : arpl2SP})

        _df=pd.concat([p1w,p1a,p1s,p2w,p2a,p2s],axis=1)
        return _df

    def computeAvrPerformanceEvaluation(self):
        _df=None
        pl1=self.targetdf['PLAvr']


        x=0 #Total Num Trade
        w=0 #Total Num Win Trade
        l=0 #Total Num Lose Trade
        t=0 #Total PL
        for i in range(0,len(pl1)):
            if pl1[i] > 0:
                w+=1
                x+=1
            elif pl1[i] < 0:
                l+=1
                x+=1
            else:
                x+=1
            t=t+pl1[i]

        pl1Win=(w / x) * 100
        pl1Avr=(t / x)
        pl1SP=(pl1Avr / (np.std(pl1))) * 100 


        arpl1Win=[pl1Win]
        arpl1Avr=[pl1Avr]
        arpl1SP=[pl1SP]
        
        p1w=pd.DataFrame({'MovingAvrWinRate' : arpl1Win})
        p1a=pd.DataFrame({'MovingAvrAverage' : arpl1Avr})
        p1s=pd.DataFrame({'MovingAvrSharpRatio' : arpl1SP})


        _df=pd.concat([p1w,p1a,p1s],axis=1)
        return _df

    def computePerformanceEvaluation2(self): #FOR MACHINE LEARNING ONE
        _df=None
        pl1=self.targetdf['PL']

        x=0 #Total Num Trade
        w=0 #Total Num Win Trade
        l=0 #Total Num Lose Trade
        t=0 #Total PL
        for i in range(0,len(pl1)):
            if pl1[i] > 0:
                w+=1
                x+=1
            elif pl1[i] < 0:
                l+=1
                x+=1
            else:
                x+=1
            t=t+pl1[i]

        pl1Win=(w / x) * 100
        pl1Avr=(t / x)
        pl1SP=(pl1Avr / (np.std(pl1))) * 100 

        arpl1Win=[pl1Win]
        arpl1Avr=[pl1Avr]
        arpl1SP=[pl1SP]
        
        p1w=pd.DataFrame({'WinRate' : arpl1Win})
        p1a=pd.DataFrame({'Average' : arpl1Avr})
        p1s=pd.DataFrame({'SharpRatio' : arpl1SP})

        _df=pd.concat([p1w,p1a,p1s],axis=1)
        return _df
        
    ### OBTAIN CORREL RANKING LIST
    def getCorrelRankingList(self,df,numSpan, numRanking):
        _ret=[] #_ret[0]: negativeCorrel, #_ret[1]: positiveCorrel, #_ret[2]: neutralCorrel
        if df is not None:
            clist=df.columns
            posCorrArr=[]
            negCorrArr=[]
            neuCorrArr=[]

            for i in range(0,numSpan-1):
                _zeroArr=[]
                _posCorrArr=[]
                _negCorrArr=[]
                _neuCorrArr=[]
                for j in range(0,numRanking):
                    _zeroArr=[j,None]
                    _posCorrArr.append(_zeroArr)
                    _negCorrArr.append(_zeroArr)
                    _neuCorrArr.append(_zeroArr)
                posCorrArr.append(_posCorrArr)
                negCorrArr.append(_negCorrArr)
                neuCorrArr.append(_neuCorrArr)

            for i in range(numSpan-1,len(df)):
                regArr=[]
                absArr=[]
                for j in range(1, len(clist)): #skip the first column (timestamp)
                    val=df.iloc[i,j]
                    regarr=[j,val]
                    absarr=[j,abs(val)]
                    regArr.append(regarr)
                    absArr.append(absarr)

                # SORT each Array (Reguar Arr and Absolute Arr) - Ascending Sort
                regArr.sort(key=itemgetter(1))
                absArr.sort(key=itemgetter(1))

                # COMPUTE posCorrArr, negCorrArr and neuCorrArr
                _posCorrArr=[]
                _negCorrArr=[]
                _neuCorrArr=[]
                for j in range(0,numRanking):
                    n=regArr[j][0]
                    _negCorrArr.append([j,clist[n]])

                    n=regArr[len(clist)-2-j][0]
                    _posCorrArr.append([j,clist[n]])

                    n=absArr[len(clist)-2-j][0]
                    _neuCorrArr.append([j,clist[n]])
                negCorrArr.append(_negCorrArr)
                posCorrArr.append(_posCorrArr)
                neuCorrArr.append(_neuCorrArr)
            _ret.append(negCorrArr)
            _ret.append(posCorrArr)
            _ret.append(neuCorrArr)
        return _ret

                    
    ### CONSTRUT SUPERVISED DATA FROM OPEN-CLOSE HISTORY
    def getSupervisedData(self,df):
        _ret=[]
        df=df.shift(-1) # SHIFTING PRICE ROW 1 up
        timArr=df['windowTimestamp'].tolist()
        opnArr=df['Open'].tolist()
        clsArr=df['Last'].tolist()
        
        if df is not None:
            for i in range(0,len(df)-1):
                val=opnArr[i]-clsArr[i]
                if val > 0:
                    _ret.append([timArr[i],opnArr[i],clsArr[i],-1])
                elif val < 0:
                    _ret.append([timArr[i],opnArr[i],clsArr[i],1])
                else:
                    _ret.append([timArr[i],opnArr[i],clsArr[i],0])
        return _ret

    ### CONSTRUCT DECISITION TREE (MACHINE LEARNING SIGNAL)
    ### 1: CONSTRUCT LEARNING DATA

    def computeDecisionTreeSignal(self,df,rankArr,numSpan,numRank,targetPos,supervisedData, minDepth, maxDepth,direction):
        _ret=[]

        supData=pd.DataFrame(supervisedData,columns=['windowTimestamp','Open','Last','Norm'])
        if df is not None:
            ### 1ST POTION, CONSTRUCT LEARNING DATA ###
            for i in range(0,(numSpan-1)*2 +1):
                _ret.append([supervisedData[i][0],supervisedData[i][1],supervisedData[i][2],None])
            for i in range((numSpan-1)*2, len(rankArr[targetPos])-1 - 1):
                _time=df[['windowTimestamp']].iloc[i-numSpan+1:i+1,:]
                _learnData=None
                _supervisedData=None
                _colNames=[]
                for j in range(0,numRank):
                    _learnData=pd.concat([_learnData,df[[rankArr[targetPos][i][j][1]]].iloc[i-numSpan+1:i+1,:]],axis=1)
                    _colNames.append(rankArr[targetPos][i][j][1])
                _supervisedData=supData.iloc[i-numSpan+1:i,:]

                ### DECISITION TREE CURVE FITTING
                X=_learnData[_colNames].iloc[0:numSpan-1,:].fillna(0) #LEARNING DATA
                T=_supervisedData['Norm'][0:numSpan-1].fillna(0) #SUPERVISED DATA
                
                pwin=-1
                finalDepth=minDepth

                ### GRID SEARCH HYPER PARAM
                #param={ 'max_depth': list(range(5,25)) }
                #clf=tree.DecisionTreeClassifier()
                #gs=GridSearchCV(clf, param_grid=param, cv=10)
                #gs.fit(X,T)
                #w=gs.best_score_
                #finalDepth=gs.best_params_['max_depth']
                for n in range(minDepth,maxDepth):
                    # STUDYING 
                    clf=tree.DecisionTreeClassifier(max_depth=int(n))
                    try:
                        clf=clf.fit(X,T)
                    except:
                        print (X)
                    # TESTING
                    predict=clf.predict(X)
                    # EVALUATION (WINNING RATE)
                    win=sum(predict == T ) / len(T)
                    if win > pwin:
                        finalDepth=n
                        pwin=win
                        if win == 1:
                            break
                ### MAKING SIGNAL for NEXT TIME FRAME
                clf=tree.DecisionTreeClassifier(max_depth=int(finalDepth))
                try:
                    clf=clf.fit(X,T)
                except:
                    print (X)
                    print (T)
                #F=[] # NEXT LEARNING DATA for PREDICTION
                F=_learnData[_colNames].iloc[numSpan-1:numSpan,:].fillna(0) #LEARNING DATA


                predictSig=[]
                try:
                    predictSig=clf.predict(F)
                except:
                    predictSig.append(0)
                try:
                    _ret.append([supervisedData[i+1][0],supervisedData[i+1][1],supervisedData[i+1][2],direction * predictSig[0]])
                except:
                    print ('Signal for next of '+ supervisedData[i] + ' = ' + str(predictSig))

        return _ret
    ### OBTAIN COLUMNS LIST WHICH DOES NOT HAVE MULTICOLLINEARITY WITH OTHER COLUMNS
    def getIndependentColumns(self, colNames, criteria):
        for i in range(0,len(colNames)):
            if colNames[i] not in self.df:
                return []
        _def=self.df[colNames]
        print (_df)



                
