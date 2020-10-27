package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class EventSink {

	public static SinkFunction<LogRecord> sink() {
		// jdbc sinks are at least once. so it can only have the same output as a once and only once using upserts.
		return JdbcSink.sink(
			"insert into public.log_record (machine, name, log_record_timestamp, type, sequence) values (?, ?,?,?,?) " +
				"on conflict on constraint log_record_pkey " +
				"do nothing ",
			(ps, logRecord) -> {
				ps.setString(1, logRecord.getMachine());
				ps.setString(2, logRecord.getName());
				ps.setString(3, logRecord.getTimestamp());
				ps.setString(4, logRecord.getType());
				ps.setLong(5, logRecord.getSequence());
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
