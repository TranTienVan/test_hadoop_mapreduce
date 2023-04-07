javac -cp "./lib/*" Section08/*.java

jar cf UniqueListener.jar Section08/*.class

hadoop jar UniqueListener.jar Section08.UniqueListener /Section08/input.txt /Section08/output






javac -cp "./lib/*" Section08/*.java

jar cf MusicDataAnalysis.jar Section08/*.class

hadoop jar MusicDataAnalysis.jar Section08.MusicDataAnalysis /Section08/input.txt /Section08/output


hadoop fs -cat /Section08/output/part-r-00000