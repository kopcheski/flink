package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Processor {

	public static void main(String[] args) throws Exception {
		final SourceFunction<LogRecord> logRecordSource = new LogRecordProducer();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000L);

		DataStreamSource<LogRecord> logRecordDataStreamSource = env.addSource(logRecordSource);

		DataStream<Task> tasks = logRecordDataStreamSource
			.keyBy(LogRecord::getMachine)
			.flatMap(new EventMapper());

//		tasks.print();

		env.execute("State machine job");

	}
}
