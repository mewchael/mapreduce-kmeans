import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//求最大值和最小值
public class MaxMin {
    //map函数得到一行数据，key是数据的维数，value是这行数据
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] nums = line.split(",");
            context.write(new Text(String.valueOf(nums.length)), value);
        }
    }

    //combile函数求出本地的最大值和最小值
    public static class Combine extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //得到数据维数
            Integer size = Integer.parseInt(key.toString());
            //最大值和最小值数组
            double[] max = new double[size];
            double[] min = new double[size];
            //赋初值
            for (int i = 0; i < size; i++) {
                max[i] = 0;
                min[i] = 9999;
            }
            //遍历找出本地的最大值和最小值
            for (Text value : values) {
                String line = value.toString();
                String[] nums = line.split(",");
                for (int i = 0; i < nums.length; i++) {
                    double t = Double.parseDouble(nums[i]);
                    if (t > max[i])
                        max[i] = t;
                    if (t < min[i])
                        min[i] = t;
                }
            }
            //写回最大值和最小值，key仍然是维数，value是最大值和最小值两条数据
            context.write(key, new Text(Arrays.toString(max).replace("[", "").replace("]", "")));
            context.write(key, new Text(Arrays.toString(min).replace("[", "").replace("]", "")));
        }
    }
    
    //得到全局的最大值和最小值
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //得到数据维数
            int size = Integer.parseInt(key.toString());
            //最大值和最小值列表
            double[] tmax = new double[size];
            double[] tmin = new double[size];
            //赋初值
            for (int i = 0; i < size; i++) {
                tmax[i] = 0;
                tmin[i] = 9999;
            }
            //遍历得到最大值和最小值
            for (Text value : values) {
                String line = value.toString();
                String[] nums = line.split(",");
                for (int i = 0; i < nums.length; i++) {
                    double t = Double.parseDouble(nums[i]);
                    if (t > tmax[i])
                        tmax[i] = t;
                    if (t < tmin[i])
                        tmin[i] = t;
                }
            }
            //写回最大值和最小值
            context.write(new Text(""), new Text(Arrays.toString(tmax).replace("[", "").replace("]", "")));
            context.write(new Text(""), new Text(Arrays.toString(tmin).replace("[", "").replace("]", "")));
        }

    }

    public static void main(String[] args) throws Exception {
        // 以下部分为HadoopMapreduce主程序的写法，对照即可
        // 创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "MaxMin");
        // 设置运行Job的类
        job.setJarByClass(MaxMin.class);
        // 设置Mapper类
        job.setMapperClass(Map.class);
        // 设置Map输出的Key value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置Combiner类
        job.setCombinerClass(Combine.class);
        // 设置Reducer类
        job.setReducerClass(Reduce.class);

        // 设置Reduce输出的Key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if (!b) {
            System.out.println("MaxMin task fail!");
        }
    }

}
