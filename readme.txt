Build dir in HDFS:
hdfs dfs -mkdir /input

place file on HDFS:
hdfs dfs -put userdata.txt /input
hdfs dfs -put soc-LiveJournal1Adj.txt /input


delete the ouput directories if they are not empty:
hdfs dfs -rm -r /out1
hdfs dfs -rm -r /out2
hdfs dfs -rm -r /out1 && hdfs dfs -rm -r /out2

Use jar to run the program

1.	hadoop jar MyWordCount.jar hw1/mutualFriend_Q1 /input/soc-LiveJournal1Adj.txt /out1 
2.	hadoop jar MyWordCountQ2.jar hw1/Q2 /input/soc-LiveJournal1Adj.txt /out1 /out2
3.	hadoop jar MyWordCountQ3.jar hw1/Q3 /input/userdata.txt /input/soc-LiveJournal1Adj.txt /out1 /out2 28041 28056
4.	hadoop jar MyWordCountQ4.jar hw1/Q4 /input/userdata.txt /input/soc-LiveJournal1Adj.txt /out1 
5.	hadoop jar MyWordCountQ5.jar hw1/Q5 /input/userdata.txt /out1 



get file from HDFS:

hdfs dfs -get /out1/part-r-00000

hdfs dfs -get /out2/part-r-00000
