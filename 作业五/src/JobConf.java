package edu.fin.hw5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** 小内存虚拟机上的作业参数。任务放进 AppMaster，避免再拉起一堆容器。 */
public final class JobConf {
  private JobConf() {}

  public static void tune(Configuration conf) {
    conf.setBoolean("mapreduce.job.ubertask.enable", true);
    conf.setInt("mapreduce.job.ubertask.maxmaps", 9);
    conf.setInt("mapreduce.job.ubertask.maxreduces", 1);
    conf.setLong("mapreduce.job.ubertask.maxbytes", 256L * 1024 * 1024);
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 1536);
    conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx1024m");
    conf.setInt("mapreduce.map.memory.mb", 1024);
    conf.setInt("mapreduce.reduce.memory.mb", 1024);
    conf.set("mapreduce.map.java.opts", "-Xmx512m");
    conf.set("mapreduce.reduce.java.opts", "-Xmx768m");
    conf.setInt("mapreduce.task.io.sort.mb", 64);
    conf.set("mapreduce.job.reduce.slowstart.completed.maps", "1.0");
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);
    conf.set("mapreduce.output.textoutputformat.separator", "\t");
    conf.setInt("dfs.replication", 2);
  }

  public static void delete(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }
}
