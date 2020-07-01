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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.LineReader;

//利用得到的最大值和最小值进行归一化
public class Norm {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 文件路径
            String maxminPath = context.getConfiguration().get("maxminPath");
            Path path = new Path(maxminPath);
            // 配置文件
            Configuration conf = new Configuration();
            // hadoop文件操作的Java接口
            FileSystem fileSystem = path.getFileSystem(conf);
            // hadoop读入文件操作
            FSDataInputStream fsis = fileSystem.open(path);
            // Hadoop读一行数据
            LineReader lineReader = new LineReader(fsis, conf);
            Text maxline = new Text();
            Text minline = new Text();
            //读入最大值和最小值
            lineReader.readLine(maxline);
            lineReader.readLine(minline);
            //转换为String数组
            String[] maxstr = maxline.toString().split(",");
            String[] minstr = minline.toString().split(",");
            //value转换为String数组
            String line = value.toString();
            String[] nums = line.split(",");
            //遍历求归一化后的结果
            for (int i = 1; i < maxstr.length; i++) {
                double num = Double.parseDouble(nums[i]);
                double max = Double.parseDouble(maxstr[i]);
                double min = Double.parseDouble(minstr[i]);
                nums[i] = String.valueOf((num - min) / (max - min));
            }
            //写回归一化后的结果
            context.write(new Text(""), new Text(Arrays.toString(nums).replace("[", "").replace("]", "")));
        }
    }
    //reduce收集归一化后的结果并保存到文件中
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(new Text(""), value);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        String maxminPath = "output/part-r-00000";
        Configuration conf = new Configuration();
        conf.set("maxminPath", maxminPath);
        // 创建Job对象
        Job job = Job.getInstance(conf, "Norm");
        // 设置运行Job的类
        job.setJarByClass(Norm.class);
        // 设置Mapper类
        job.setMapperClass(Map.class);
        // 设置Map输出的Key value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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
            System.out.println("Norm task fail!");
        }
    }

}
