def total_transcation(file):
      df = spark.read.format("csv").option("header", "true").load(file)
      a=df.collect()
      c=sc.parallelize(a)
      zz=c.map(lambda x: x.asDict())
      k=zz.collect()
      lis=[]
      new_consumer_currency=[]
      for x in k:
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
      return df2                        

b=lambda x:total_transcation(x)
execution=lambda function,file:function(file)
execution(b,file)
