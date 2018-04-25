
from dbscanner import *
import csv
import re

configPath = './dbscan/config.txt'
dataPath = './dbscan/abc.csv'

def main():
    [Data,eps,MinPts]= getData()
    dbc= dbscanner()
    dbc.dbscan(Data, eps, MinPts)
    
def getData():
    Data = []

    with open(dataPath,'rt') as f:
        reader = csv.reader(f)
        for row in reader:
            #row = re.split(r'\t+',row[0])
            Data.append([float(row[0]),float(row[1])])
            
    f = open(configPath,'rt')
    
    [eps,MinPts] = parse(f.readline())
    
    print (eps,MinPts)
    
    return [Data,eps,MinPts]

def parse(line):
    data = line.split(" ")
    return [int(data[0]),int(data[1])]
    
            
    
main() 