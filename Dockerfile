FROM flink
ADD target/flink-examples-streaming-sdt4_2.11-beta-jar-with-dependencies.jar /opt/flink/usrlib/flink-app.jar
