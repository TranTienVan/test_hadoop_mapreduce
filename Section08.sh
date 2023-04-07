javac -cp "./lib/*" Section08/*.java

jar cf UniqueListener.jar Section08/*.class

hadoop jar UniqueListener.jar Section08.UniqueListener /input.txt /output






javac -cp "./lib/*" Section08/*.java

jar cf MusicDataAnalysis.jar Section08/*.class

hadoop jar MusicDataAnalysis.jar Section08.MusicDataAnalysis /input.txt /output