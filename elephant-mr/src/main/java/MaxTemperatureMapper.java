import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Hadoop 本身提供了一套可优化网络序列化传输的基本类型，而不是直接使用 Java 内置的类型，这些类型都在 org.apache.hadoop.io 包中。
LongWritable 类型相当于 Java 的 Long 类型
Text 类型相当于 Java 的 String 类型
IntWritable 类型相当于 Java 的 Integer 类型
 */

// Mapper 类是一个泛型类型，它有四个形参类型，分别指定 map 函数的输入键、输入值、输出键和输出值的类型
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将包含有一行输入的 Text 的值准换成 Java 的 String 类型
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;

        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        String quality = line.substring(92, 93);

        if (airTemperature != MISSING && quality.matches("[01459]")) {
            // Context 实例用于输出内容的写入
            // 将 year 封装在 Text 类型中，将 airTemperature 封装在 IntWritable 类型中
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
