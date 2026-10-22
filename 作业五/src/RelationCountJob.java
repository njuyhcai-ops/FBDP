package edu.fin.hw5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
伪代码
def map(一行):
    指数, 股票, 标记 = 一行.split()
    本分片计数[指数][标记] += 1

def 分片结束():
    for 指数, 计数 in 本分片计数:
        输出(指数, 计数)

def reduce(指数, 部分和列表):
    交集, 只在A, 只在B = 把部分和相加
    并集 = 交集 + 只在A + 只在B
    输出(指数, 交集, 只在A, 只在B, 并集)
*/
public class RelationCountJob {

  public static class CountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static final class Counts {
      long both;
      long aOnly;
      long bOnly;
    }

    private final Map<Integer, Counts> counts = new HashMap<Integer, Counts>();
    private final IntWritable indexOut = new IntWritable();
    private final Text countOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString().replace("\r", "").trim();
      if (line.isEmpty()) {
        return;
      }
      String[] parts = line.split("\\t");
      if (parts.length < 3) {
        parts = line.split("\\s+");
      }
      if (parts.length < 3) {
        return;
      }
      int index = Integer.parseInt(parts[0]);
      Counts row = counts.get(index);
      if (row == null) {
        row = new Counts();
        counts.put(index, row);
      }
      String relation = parts[parts.length - 1];
      if ("BOTH".equals(relation)) {
        row.both++;
      } else if ("A_ONLY".equals(relation)) {
        row.aOnly++;
      } else if ("B_ONLY".equals(relation)) {
        row.bOnly++;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, Counts> entry : counts.entrySet()) {
        Counts row = entry.getValue();
        indexOut.set(entry.getKey());
        countOut.set(row.both + "\t" + row.aOnly + "\t" + row.bOnly);
        context.write(indexOut, countOut);
      }
    }
  }

  public static class CountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private final Text out = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      long both = 0;
      long aOnly = 0;
      long bOnly = 0;
      for (Text value : values) {
        String[] parts = value.toString().split("\\t");
        if (parts.length < 3) {
          continue;
        }
        both += Long.parseLong(parts[0]);
        aOnly += Long.parseLong(parts[1]);
        bOnly += Long.parseLong(parts[2]);
      }
      long union = both + aOnly + bOnly;
      out.set(both + "\t" + aOnly + "\t" + bOnly + "\t" + union);
      context.write(key, out);
    }
  }

  public static boolean run(Configuration base, String input, String output) throws Exception {
    Configuration conf = new Configuration(base);
    JobConf.tune(conf);
    Job job = Job.getInstance(conf, "hw5-task2-count");
    job.setJarByClass(RelationCountJob.class);
    job.setMapperClass(CountMapper.class);
    job.setReducerClass(CountReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(input));
    Path out = new Path(output);
    JobConf.delete(conf, out);
    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true);
  }
}
