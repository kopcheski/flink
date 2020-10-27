package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class TaskSink {

	public static SinkFunction<Task> sink() {
		return JdbcSink.sink(
			"insert into public.task (machine, name, start_timestamp, stop_timestamp) values (?,?,?,?)",
			(ps, task) -> {
				ps.setString(1, task.getMachine());
				ps.setString(2, task.getName());
				ps.setString(3, task.getStartTimestamp());
				ps.setString(4, task.getStopTimestamp());
			},
			JdbcExecutionOptions.builder()
				.withBatchSize(2)
				.build(),
			new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl("jdbc:postgresql://postgres:5432/database")
				.withUsername("postgres")
				.withPassword("postgres")
				.withDriverName("org.postgresql.Driver")
				.build());
	}

}
