# 作业五：MapReduce 求指数成分股集合关系

本次作业采用的方式是先根据任务把伪代码写出来，然后利用Grok4.7进行代码书写从而避免Java代码书写以及规范性问题，在写完之后逐行翻译代码之后进行了人工核验，最后在lab1已经配好的hadoop集群上跑完两个任务

## 设计思路

任务 1：

```python
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
```

只用 1 个 Reducer。键是文本，Shuffle 按字典序排列后，结果是指数编号升序、股票代码升序。`101`、`102`、`103` 位数相同，字典序和数值序一致。

任务 2 读任务 1 的输出。Map 在本分片内先计数，Reduce 再把部分和相加：

```python
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
```



## 目录

```text
作业五
├── src
│   ├── Hw5Driver.java           串联两个任务
│   ├── StockRelationJob.java    任务 1
│   ├── RelationCountJob.java    任务 2
│   └── JobConf.java
├── target/hw5.jar
├── output/task1/part-r-00000
├── output/task2/part-r-00000
├── screenshots
├── pom.xml
├── A.txt
└── B.txt
```



## 运行

先启动 `lab1` 里的集群，把 `A.txt`、`B.txt` 放到 HDFS 的同一个目录，再提交作业。jar 没有写 Main-Class，类名要写在命令里。

```bash
hdfs dfs -mkdir -p /user/root/hw5/input
hdfs dfs -D dfs.replication=2 -put -f A.txt /user/root/hw5/input/A.txt
hdfs dfs -D dfs.replication=2 -put -f B.txt /user/root/hw5/input/B.txt

hadoop jar target/hw5.jar edu.fin.hw5.Hw5Driver \
  /user/root/hw5/input \
  /user/root/hw5/output_task1 \
  /user/root/hw5/output_task2
```

## 运行结果

任务 1 共 547 行，这里因为太长不展示了，在output的task1里面可以看到具体情况

任务 2：

```text
101	3	13	4	20
102	3	19	3	25
103	52	195	255	502
```

四列依次是交集、A 独有、B 独有、并集。

三个指数合计：交集 58，只在 A 227，只在 B 262。

## 页面截图

YARN Finished 页面里，`hw5-task1-relation` 和 `hw5-task2-count` 都是 FINISHED / SUCCEEDED。

![YARN 上全部作业成功](screenshots/yarn-finished.png)

NameNode 处于 active，两个 DataNode 都在服务中。

![NameNode Overview](screenshots/namenode-overview.png)

![DataNode 列表](screenshots/namenode-datanodes.png)

HDFS 上能看到输入目录和两个输出目录。

![作业五的 HDFS 目录](screenshots/hdfs-hw5.png)

## 不足和可以改进的地方

- 任务一的键是一整段文本 `指数\t股票`，顺序靠字典序。现在三个指数都是三位数，所以没问题。一旦出现 `99` 和 `100`，`100` 会排在 `99` 前面。键可以拆开：指数按整数比较，股票代码按字符串比较。
- 两份结果各自全局有序，靠的是只开 1 个 Reducer。股票再多时，可以按指数编号分区，这个本实验由于数据量小没有做，但是数据量大需要多个reducer的时候就很有必要，每个 Reduce 只给自己的指数排序。分区号和指数顺序一致时，把 `part-r-00000`、`part-r-00001` 按文件名接起来，整体仍然有序。
