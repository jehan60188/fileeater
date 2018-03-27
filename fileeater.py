import pandas as pd

class fileEater():
    '''
    this class handles reading data from a set of csvs
    fnameList = set of csvs
    linesPerQuery = max number of lines to return
    
    automatically moves to next file if current file is exhausted
    returns empty dataframe if all files are exhausted
    can choose to reset feed, returning the same data over and over
    '''
    def __init__(self,fnameList,linesPerQuery = 1000,rand= False,randF=1,repeat = False):
        '''
        initializer
        Args:
            fnameList ([str]): array of strings, pointing to csv files
            linesPerQuery (int): number of lines to read each time; default= 1000
            rand (boolean): whether to return a random subsample of the data; default = False
            randF (float): fraction to return if rand=True; default =1 (shuffles the data up)
            repeat (boolean): whether to reset the file list once its exhausted. default= False
        '''
        self.fnameList = fnameList
        self.linesPerQuery = linesPerQuery
        self.currentFile = self.fnameList.pop()
        self.currentLineInFile = 0
        self.rand= rand
        self.randF = randF
        self.listbakup = fnameList.copy()
        self.repeat = repeat
    
    def gimmeData(self,linesPerQuery = None):
        '''
        returns a pandas dataframe
        Args:
            linesPerQuery (int, optional): how many lines to return
                if None, revert to self.linesPerQuery; default = None
        Returns: 
            pandas dataframe of the next linesPerQuery lines
        '''
        if not linesPerQuery: linesPerQuery = self.linesPerQuery
        df = pd.read_csv(self.currentFile,
                         skiprows=range(1,self.currentLineInFile),
                         nrows=linesPerQuery,
                         header  = 0)
        self.currentLineInFile = self.currentLineInFile + linesPerQuery
        if df.empty:
            if(len(self.fnameList) ==0): #if we're out of files
                if not self.repeat: return pd.DataFrame() #no repeat -> return empty
                self.fnameList = self.listbakup.copy() #otherwise, reset and start over!
            self.currentFile = self.fnameList.pop()
            self.currentLineInFile = 0
            return self.gimmeData(linesPerQuery)
        if self.rand: df = df.sample(frac = self.randF)
        return df
