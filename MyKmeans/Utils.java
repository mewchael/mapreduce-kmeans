import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Utils {
    // 读取聚类中心
    public static ArrayList<ArrayList<Double>> getCentersFromHDFS(String centersPath, boolean isDirectory)
            throws IOException {
        // 新建一个二维数组存储读到的聚类中心
        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
        // 聚类中心的路径
        Path path = new Path(centersPath);
        // 新建配置文件用于读取文件
        Configuration conf = new Configuration();
        // 得到FileSystem文件接口
        FileSystem fileSystem = path.getFileSystem(conf);
        // 判断是否是文件夹
        if (isDirectory) {
            // 如果是文件夹就得到文件夹中的文件列表
            FileStatus[] listFile = fileSystem.listStatus(path);
            // 对所有文件应用getCentersFromHDFS
            for (int i = 0; i < listFile.length; i++) {
                result.addAll(getCentersFromHDFS(listFile[i].getPath().toString(), false));
            }
            return result;
        }
        // 得到文本输入流
        FSDataInputStream fsis = fileSystem.open(path);
        // 每次读一行的阅读器
        LineReader lineReader = new LineReader(fsis, conf);
        // 保存读到的一行
        Text line = new Text();
        // 每读到一行就转换成array然后添加到结果中
        while (lineReader.readLine(line) > 0) {
            ArrayList<Double> tempList = textToArray(line);
            result.add(tempList);
        }
        // 关闭阅读器
        lineReader.close();
        return result;
    }

    // 删除pathStr处的文件
    public static void deletePath(String pathStr) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path, true);
    }

    // Text转为array
    public static ArrayList<Double> textToArray(Text text) {
        ArrayList<Double> list = new ArrayList<Double>();
        String[] fileds = text.toString().split(",");
        for (int i = 0; i < fileds.length; i++) {
            list.add(Double.parseDouble(fileds[i]));
        }
        return list;
    }

    // 比较两个聚类中心是否相同
    public static boolean compareCenters(String centerPath, String newPath) throws IOException {
        // 读取旧的聚类中心，存放在一个文件中
        List<ArrayList<Double>> oldCenters = Utils.getCentersFromHDFS(centerPath, false);
        // 读取新的聚类中心，因为是输出的所以存放在文件夹中
        List<ArrayList<Double>> newCenters = Utils.getCentersFromHDFS(newPath, true);
        // 聚类中心个数
        int size = oldCenters.size();
        // 聚类中心维度
        int fildSize = oldCenters.get(0).size();
        // 计算距离
        double distance = 0;
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < fildSize; j++) {
                double t1 = Math.abs(oldCenters.get(i).get(j));
                double t2 = Math.abs(newCenters.get(i).get(j));
                distance += Math.pow((t1 - t2) / (t1 + t2), 2);
            }
        }
        // 距离为0，删除新聚类中心
        if (distance == 0.0) {
            Utils.deletePath(newPath);
            return true;
        } else {
            // 距离不为0，把旧聚类中心清空，把新聚类中心复制到旧聚类中心，然后删除新聚类中心
            // 清空旧聚类中心（不是删除）
            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);
            FSDataOutputStream overWrite = fileSystem.create(outPath, true);
            overWrite.writeChars("");
            overWrite.close();
            // 把新聚类中心复制到旧聚类中心
            Path inPath = new Path(newPath);
            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (int i = 0; i < listFiles.length; i++) {
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            // 删除新聚类中心
            Utils.deletePath(newPath);
        }
        return false;
    }
}
