hadoop com.sun.tools.javac.Main WordCount.java 
jar cf wc.jar WordCount*.class

hadoop jar wc.jar WordCount input output
