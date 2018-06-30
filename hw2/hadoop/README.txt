1. start hadoop

   $ start-dfs.sh
   $ start-yarn.sh

2. Example: WordCount.java

   (1) edit WordCount.java  (have a look at the code)
		编辑WordCount.java这个文件
   (2) edit WordCount-manifest.txt (have a look at this)
		生成jar包用
   (3) compile and generate jar	编译生成jar包的步骤
   $ rm -f *.class *.jar
   $ javac WordCount.java
   $ jar cfm WordCount.jar WordCount-manifest.txt WordCount*.class
清理hdfs目录下的输出，运行mapreduce工作
   (4) remove output hdfs directory then run MapReduce job
   $ hdfs dfs -rm -f -r /hw2/output
   $ hadoop jar ./WordCount.jar /hw2/example-input.txt /hw2/output

   (5) display output
   $ hdfs dfs -cat '/hw2/output/part-*'

3. Homework 2 part 1 specification

  (1) java class name: Hw2Part1

  (2) command line:

  $ hadoop jar ./Hw2Part1.jar <input file> <output directory>

  <input file> : on hdfs
  <output directory> : on hdfs, it is removed before running the command

  (3) input file format
  every line consists of 3 fields separated by space:

     <source> <destination> <duration>

  (4) output file format
  every line should consist of four fields:

     <source> <destination> <count> <average duration>

  the four fields are sparated by either space or tab
