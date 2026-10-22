package edu.fin.hw5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 先跑任务一，再跑任务二。 */
public class Hw5Driver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      return 2;
    }
    Configuration conf = getConf();
    System.out.println("开始任务一：");
    if (!StockRelationJob.run(conf, args[0], args[1])) {
      return 1;
    }
    System.out.println("开始任务二：");
    if (!RelationCountJob.run(conf, args[1], args[2])) {
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Hw5Driver(), args));
  }
}
