javac -cp "./lib/*" Section07/*.java

jar cf DeIdentifyData.jar Section07/*.class

hadoop jar DeIdentifyData.jar Section07.DeIdentifyData /Section07/input.txt /Section07/output


hadoop fs -cat /Section07/output3/part-r-00000