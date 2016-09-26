hdfs dfs -rm -r /user/span/out*
hadoop com.sun.tools.javac.Main WordCount.java Stemmer.java
jar cf wc.jar *.class
hadoop jar wc.jar WordCount input out out1 > a
cat a
