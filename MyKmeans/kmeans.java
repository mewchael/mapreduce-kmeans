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

public class kmeans {
    // Map函数，输出key是每条数据最近的聚类中心，value是数据信息
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        // 用于存放聚类中心的二维数组
        ArrayList<ArrayList<Double>> centers = null;
        // 聚类中心个数
        int k = 0;

        // 读取旧的聚类中心
        protected void setup(Context context) throws IOException, InterruptedException {
            centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"), false);
            k = centers.size();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 每条数据转换为一个数组
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();
            double minDistance = 99999999;
            int centerIndex = 0;
            // 循环计算到每个中心的距离
            for (int i = 0; i < k; i++) {
                double currentDistance = 0;
                for (int j = 1; j < sizeOfFileds; j++) {
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double filed = Math.abs(fileds.get(j));
                    currentDistance += Math.pow((centerPoint - filed), 2);
                }
                // 找到更短的距离就换成当前的中心
                if (currentDistance < minDistance) {
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            // 输出key为聚类中心的编号，value为数据
            context.write(new IntWritable(centerIndex + 1), value);
        }

    }

    public static class Combine extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            // 保存所有聚类中心相同的数据
            ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
            // value中的数据添加到filedsList中
            for (Iterator<Text> it = value.iterator(); it.hasNext();) {
                ArrayList<Double> tempList = Utils.textToArray(it.next());
                filedsList.add(tempList);
            }
            // 每条数据长度
            int filedSize = filedsList.get(0).size();
            // 数据数量
            Integer count = filedsList.size();
            // 所有相同聚类中心的数据之和
            double[] SUM = new double[filedSize];
            // 对数据求和添加到SUM中
            for (int i = 0; i < filedSize; i++) {
                double sum = 0;
                for (int j = 0; j < count; j++) {
                    sum += filedsList.get(j).get(i);
                }
                SUM[i] = sum;
            }
            context.write(key,
                    new Text(count.toString() + ";" + Arrays.toString(SUM).replace("[", "").replace("]", "")));
        }

    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
        protected void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            // 每种聚类中心的数据条数
            Integer count = 0;
            // 存放combine得到的局部和
            ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
            // 循环得到数据总数和所有局部和，添加到二维数组中
            for (Text it : value) {
                // 临时存放一条局部和
                ArrayList<Double> tempList = new ArrayList<Double>();
                // value转成String
                String val = it.toString();
                // 得到分号表示的分界点
                int splitIndex = val.indexOf(";");
                // 前半部分是数据数量，累加到count中
                count += Integer.parseInt(val.substring(0, splitIndex));
                // 后半部分是局部和，转成数组添加到filedsList中
                String val1 = val.substring(splitIndex + 1);
                tempList = Utils.textToArray(new Text(val1));
                filedsList.add(tempList);
            }
            // 数据长度
            int filedSize = filedsList.get(0).size();
            // 平均值（新的聚类中心）
            double[] avg = new double[filedSize];
            // 求出平均值
            for (int i = 0; i < filedSize; i++) {
                double sum = 0;
                int size = filedsList.size();
                for (int j = 0; j < size; j++) {
                    sum += filedsList.get(j).get(i);
                }
                avg[i] = sum / count;
            }
            // 写回新的聚类中心
            context.write(new Text(""), new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
        }
    }

    public static void run(String centerPath, String dataPath, String newCenterPath, boolean runReduce)
            throws IOException, ClassNotFoundException, InterruptedException {
        // 设置聚类中心的位置
        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);

        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(kmeans.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if (runReduce) {
            job.setCombinerClass(Combine.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
        // 数据文件
        FileInputFormat.addInputPath(job, new Path(dataPath));
        // 聚类中心
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String centerPath = "input/centers.txt";
        String dataPath = "input/wine.data";
        String newCenterPath = "out/kmean";
        // 运行次数
        int count = 0;

        while (true) {
            run(centerPath, dataPath, newCenterPath, true);
            System.out.println(" 第 " + ++count + " 次计算 ");
            if (Utils.compareCenters(centerPath, newCenterPath)) {
                run(centerPath, dataPath, newCenterPath, false);
                break;
            }
        }
    }
}
