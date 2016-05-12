import numpy as np
import scipy
import pandas as pd
import re
from collections import Counter
from datetime import datetime
from datetime import timedelta




class SessionBuilder:
    def __init__(self):
        print("Start sessionbuilder")
        #self.queryReader()
        self.nrOfSessions = 0
        self.documentFrame = self.documentClickReader()
        self.queryFrame = self.queryReader()
        self.matchQueryDocument()
        self.statisticsGenerator()


    # function that reads in event 164
    #
    def queryReader(self):
        print("start queryReader")
        #column we want to use
        cols = ['UserID', 'TimeStamp', 'EventID', 'SearchText', 'TotalResult', 'FilterPath' ]
        queryFrame = pd.read_csv("../data/compID349_EventID164_Rows100000.csv",  skipinitialspace=True, usecols=cols)
        queryFrame = queryFrame.sort_values(['UserID', 'TimeStamp'], ascending=[True, True])
        print(queryFrame.columns)
        #queryFrame = queryFrame.set_index('UserID')


        #remove all NaN SearchText values
        #queryFrame = queryFrame[pd.notnull(queryFrame['SearchText'])]
        queryFrame = queryFrame.dropna()
        return queryFrame


    def documentClickReader(self):
        cols = ['UserID','TimeStamp', 'EventID', 'DocumentURL']

        print("start documentClickReader")
        documentFrame = pd.read_csv("../data/compID349_EventID27_Rows100000.csv",  skipinitialspace=True, usecols=cols)
        documentFrame = documentFrame.dropna()
        documentFrame = documentFrame.sort_values(['UserID', 'TimeStamp'], ascending=[True, True])
        #documentFrame = documentFrame.set_index('UserID')
        #print(documentFrame.columns)
        #print(documentFrame)
        return documentFrame

    def matchQueryDocument(self):
        print("startmerge")
        print(self.queryFrame.shape)
        print(self.documentFrame.shape)
        # merge the two dataframes
        df = pd.concat([self.queryFrame, self.documentFrame])
        print(df.shape)
        df = df.sort_values(['UserID', 'TimeStamp'], ascending=[True, True])

        #with pd.option_context('display.max_rows', 999, 'display.max_columns', 6):
        #    print(df)
        queryUser = pd.Series(self.queryFrame.UserID)
        documentUser = pd.Series(self.documentFrame.UserID)
        userMatch = 0
        noClickUser = 0
        total  = Counter(queryUser)
        currentUser = 0
        self.multipleQueries = 0
        beginOfSession = 0
        nrOfSessions = 0
        singleSearches = 0
        firstQuery = True
        queryTimeStamp = datetime.now()
        for row in df.itertuples():
            # same user detected
            if currentUser == row.UserID:
                queryTimeStamp = self.parseDate(row.TimeStamp)
                if beginOfSession is not False:
                    firstQuery = False
                    if queryTimeStamp - timedelta(minutes=30) > beginOfSession:
                        print()
                        print("new session because expire")
                        nrOfSessions += 1
                        difference = queryTimeStamp - beginOfSession
                        beginOfSession = self.parseDate(row.TimeStamp)
                    difference = queryTimeStamp - beginOfSession
                    if row.EventID == 164:
                        self.multipleQueries += 1
                        print(row.SearchText)
                        print(row.TimeStamp)
                        print(row.TotalResult)
                        print(row.FilterPath)
                    if row.EventID == 27:
                        #print(row.DocumentURL)
                        print("DOC")
                        print(row.TimeStamp)
                else:
                    print("begin of session was not detected")
            #next (or new) user detected
            else:
                print()
                print("new user found")

                currentUser = row.UserID
                if row.EventID == 164:
                    print()
                    print("new session")
                    print(row.SearchText)
                    nrOfSessions += 1
                    beginOfSession = self.parseDate(row.TimeStamp)
                    if firstQuery == True:
                        singleSearches += 1
                    firstQuery = True
                if row.EventID == 27:
                    print()
                    print("new session")
                    beginOfSession = False
        print(singleSearches)
        self.nrOfSessions = nrOfSessions
        self.singleSearches = singleSearches

    def statisticsGenerator(self):
        print('stats')
        print(self.multipleQueries)
        print('Nr of total executed queries')
        print(self.queryFrame.shape[0])
        print('Nr of unique users')
        print(self.queryFrame.UserID.nunique())
        print('Nr of sessions')
        print(self.nrOfSessions)
        print('Query mean per session')
        print(self.queryFrame.shape[0]/self.nrOfSessions)
        print('Session mean per user')
        print(self.nrOfSessions/self.queryFrame.UserID.nunique())



    def parseDate(self, text):
        for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')


        #print(total)
        #print(us2)
        #inte = 76748
        #print(us2.values)
        #print(len(us1.unique()))
        #for user in us1.unique():
        #    if user in us2.values:
        #        userMatch += 1
        #    else:
        #        noClickUser += 1
        #print(userMatch)
        #print(noClickUser)

#We lose about 2.5 direct
        #print(df)











def main():
    sessionBuilder = SessionBuilder()

if __name__ == "__main__":
    main()


#searchText = pd.Series(queryFrame.SearchText)
#userID = pd.Series(queryFrame.UserID)
#print(len(userID))
#print("Nr of search queries before NaN removal")
#print(len(searchText))
#print("Nr of search queries after Nan removal")
#
#searchText = searchText.dropna()
#print(len(searchText))
## list that represents word values
#nrWordsPerQuery = []
#
#for query in searchText :
#    nrofWords = len(query.split())
#    nrWordsPerQuery.append(nrofWords)
#    if(nrofWords > 20):
#        print(re.sub(r"[^0-9a-zA-Z]+", " ", query))
#        print("")
#mean = np.mean(nrWordsPerQuery)
#print("Mean words within a query")
#print(mean)
