###I only want to generate the dataframe and parallelize it once
###then the functions can take in dataframe or the parallelized product
def proprecessing_file(file):
            df = spark.read.format("csv").option("header", "true").load(file)
            a=df.collect()
            c=sc.parallelize(a)
            zz=c.map(lambda x: x.asDict())
            k=zz.collect()
            return df,k

###function 1: take in dataframe and column_name, calculate rows for specific column_name
def count_column(df,column_name):
            df.groupby(column_name).count()
            return 0
            
            

###function 2: divide the dataframes according to specific column name,take in dictionary list parallelized from sample file
def split_according_column(df,dictionary_extract,column_name):
            lis=set()
            consumer=[]
            for x in dictionary_extract:
                lis.add(x[column_name])
            now_list=list(lis)
            for i in range(len(now_list)):
                now=now_list[i]
                dff=df.where(column_name+"='"+now+"'")
                consumer.append(dff)
            for x in consumer:
                x.show()
            return 0
                
###function 3: for each customer, count their total transcation amounts. 
###if they use the different trade accurancy, count them sparately 
###take in dictionary list parallelized from sample file
def total_transcation(dictionary_extract):
      lis=[]
      new_consumer_currency=[]
      for x in dictionary_extract:
            a=(x['customer'],x['tradeCurrency'])
            if a not in new_consumer_currency:
                  now_amount=x['transcation_amont']
                  x['transcation_amont']=int(now_amount)
                  lis.append(x)
                  new_consumer_currency.append(a)
            else:
                  for y in lis:
                        if y['customer']==x['customer'] and y['tradeCurrency']==x['tradeCurrency']:
                              amount=int(y['transcation_amont'])
                              new_amount=amount+int(x['transcation_amont'])
                              y['transcation_amont']=new_amount
                              if x['StartDate']<y['StartDate']:
                                    y['StartDate']=x['StartDate']
                              if x['endDate']>y['endDate']:
                                    y['endDate']=x['endDate']
      import pandas as pd
      df2=pd.DataFrame(lis,columns=['AggGroup','AssetType','StartDate','customer','endDate','tradeCurrency','transcation_amont'])
      print(df2)   
      return 0

import multiprocessing
import time
if __name__ == '__main__':  
    ###you should define the path of file 
    ###file = ' '
    df, k=proprecessing_file(file)
    p0=multiprocessing.Process(target=total_transcation,args=(k,))
    p1=multiprocessing.Process(target=split_according_column,args=(df,k,'customer',)) ###you can change the 'customer' to any column name
    p2=multiprocessing.Process(target=count_column,args=(df,'tradeCurrency',)) ###you can change the 'tradeCurrecy' to any column name
    p0.start()
    start1 = time.time()        
    p1.start()
    start2= time.time() 
    p2.start()
    start3= time.time() 
    p0.join()
    end1=time.time() 
    p1.join()
    end2=time.time() 
    p2.join()
    end3=time.time() 
    print('start1:')
    print(start1)
    print('start2:')
    print(start2)
    print('start3:')
    print(start3)
    print('end1:')
    print(end1)
    print('end2:')
    print(end2)
    print('end3:')
    print(end3)

