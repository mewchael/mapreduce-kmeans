mapreduce实现kmeans    
MaxMin.java求input中的最大值和最小值，输出两个数组到output中。map函数得到一条数据，返回key为数据维数，value为数据本身。combine函数求本地的最大值最小值， reduce 函数得到全局的最大值和最小值    
Norm.java对数据进行归一化，输入是input，同时读取output中的最大值和最小值，结果输出到output1中。    
把output1中的结果重命名为wine.data放到MyKmeans中的input文件夹下，同时创建centers.txt手动指定初始聚类中心。    
MyKmeans文件夹是kmeans算法的主要实现，kmeans.java是mapreduce代码，map函数求每条数据最近的聚类中心，combine函数求本地相同聚类中心数据之和以及数据个数。Reduce函数求所有相同聚类中心数据的平均值。run函数运行一次mapreduce。main函数为死循环调用run函数，当新旧聚类中心相同时停止循环。Untils.java是实现的库函数，包括读取文件，删除文件，Text转Array和比较聚类中心。    
分别运行上面三个mapreduce，最后的聚类中心会更新在centers.txt，MyKmeans/out/kmean下会生成经过聚类的数据。
