mapreduce实现kmeans    
MaxMin.java求input中的最大值和最小值，输出两个数组到output中    
Norm.java对数据进行归一化，输入是input，同时读取output中的最大值和最小值，结果输出到output1中。    
把output1中的结果重命名为wine.data放到MyKmeans中的input文件夹下，同时创建centers.txt手动指定初始聚类中心。    
分别运行上面三个mapreduce，最后的聚类中心会更新在centers.txt，MyKmeans/out/kmean下会生成经过聚类的数据。
