def split_according_consumer(file):
            df = spark.read.format("csv").option("header", "true").load(file)
            df2= df.groupBy("customer").count()
            aa=df2.collect()
            dd=sc.parallelize(aa)
            zz=dd.map(lambda x: x.asDict())
            k=zz.collect()
            lis=[]
            consumer=[]
            for x in k:
                lis.append(x['customer'])
            for i in range(len(lis)):
                now=lis[i]
                dff=df.where("customer='"+now+"'")
                consumer.append(dff)
            for x in consumer:
                x.show()
            return consumer
            
file="file:/Users/xinciliu/Desktop/SampleData_Position_1.csv"
x=lambda a:split_according_consumer(a)  
###x(file) is the list of sample data splited according to consumer columns
excution=lambda function1,file:function1(file)   
excution(x,file)
###there will be a lot of functions in the future
###thus we can call k=lambda function2,file:function2(file)....in the future

