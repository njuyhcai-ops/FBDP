package edu.fin.hw5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/*
伪代码
def map(文件名, 一行):
    指数, 股票 = 一行.split()
    来源 = "A" if 文件名以A开头 else "B"
    输出(指数 + 股票, 来源)

def reduce(指数和股票, 来源列表):
    来源 = 去重(来源列表)          
    if "A" in 来源 and "B" in 来源:
        输出(指数和股票, "BOTH")
    elif "A" in 来源:
        输出(指数和股票, "A_ONLY")
    else:
        输出(指数和股票, "B_ONLY")
*/

public class StockRelationJob {

  public static class SourceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text keyOut = new Text();
    private final Text tagOut = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      FileSplit split = (FileSplit) context.getInputSplit();
      String name = split.getPath().getName().toUpperCase(Locale.ROOT);
      if (name.startsWith("A")) {
        tagOut.set("A");
      } else if (name.startsWith("B")) {
        tagOut.set("B");
      } 
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString().replace("\r", "").trim();
      if (line.isEmpty()) {
        return;
      }
      String[] parts = line.split("\\s+");
      if (parts.length < 2) {
        return;
      }
      keyOut.set(parts[0] + "\t" + parts[1]);
      context.write(keyOut, tagOut);
    }
  }

  public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
    private final Text relation = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Set<String> sources = new HashSet<String>();
      for (Text value : values) {
        sources.add(value.toString());
      }
      boolean inA = sources.contains("A");
      boolean inB = sources.contains("B");
      if (inA && inB) {
        relation.set("BOTH");
      } else if (inA) {
        relation.set("A_ONLY");
      } else if (inB) {
        relation.set("B_ONLY");
      } else {
        return;
      }
      context.write(key, relation);
    }
  }

  public static boolean run(Configuration base, String input, String output) throws Exception {
    Configuration conf = new Configuration(base);
    JobConf.tune(conf);
    Job job = Job.getInstance(conf, "hw5-task1-relation");
    job.setJarByClass(StockRelationJob.class);
    job.setMapperClass(SourceMapper.class);
    job.setReducerClass(RelationReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(input));
    Path out = new Path(output);
    JobConf.delete(conf, out);
    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true);
  }
}
