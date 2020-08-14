file = '/Users/xinciliu/Desktop/mockdata1.csv'
###only preprocessing data into spark dataframe
def proprecessing_file(file):
            df = spark.read.format("csv").option("header", "true").load(file)
            return df

###take in dataframe and column_name, calculate rows according to column_name
def count_column(df,column_name):
            df.groupby(column_name).count()
            return 0

###split dataframe according to column_name
def split_according_column(df,column_name):
            lis=set()
            consumer=[]
            k=df.select(column_name).collect()
            n=set()
            for x in k:
                f=list(x)
                n.add(f[0])
            now_list=list(n) 
            for i in range(len(now_list)):
                now=now_list[i]
                dff=df.where(column_name+"='"+now+"'")
                consumer.append(dff)
            for x in consumer:
                x.show()
            return 0         

import multiprocessing
import time
if __name__ == '__main__':  
    ###you have to define the file path here
    ###file=''
    df=proprecessing_file(file)
    p0=multiprocessing.Process(target=split_according_column,args=(df,'customer',)) ###you can change 'customer' to any column name
    p1=multiprocessing.Process(target=count_column,args=(df,'tradeCurrency',)) ###you can change 'tradeCurrency' to any column name
    p0.start()
    start1 = time.time()        
    p1.start()
    start2= time.time() 
    p0.join()
    end1=time.time() 
    p1.join()
    end2=time.time() 
    print('start1:')
    print(start1)
    print('start2:')
    print(start2)
    print('end1:')
    print(end1)
    print('end2:')
    print(end2)

