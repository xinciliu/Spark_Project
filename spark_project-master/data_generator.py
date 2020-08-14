import pandas as pd
import numpy as np
import csv
file = open('mockdata3.csv', 'a', newline='')
writer = csv.writer(file)
def createdf(size, w):
    cur = [" AUD", " USD", " NZD", " SGD"]
    for i in range(size):
        w.writerow([" IR_SWAP", " C"+str(np.random.randint(0, 100)), " T"+str(i+99), " A"+str(np.random.randint(0, 100)), " TB"+str(np.random.randint(0, 10)), np.random.randint(20190000, 20200000), np.random.randint(20200000, 20490000),cur[np.random.randint(0,4)]])

createdf(4000000, writer)
file.close()