package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;

public class Processor {

	public static void main(String[] args) throws Exception {
		String kafkaTopic = "events";
		String broker = "kafka:29092";

		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink.consumer");
		kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		FlinkKafkaConsumer<LogRecord> kafka = new FlinkKafkaConsumer(
			kafkaTopic, new CustomSchema(), kafkaProps);
		kafka.setStartFromGroupOffsets();
//		kafka.setStartFromEarliest();
		kafka.setCommitOffsetsOnCheckpoints(true);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1); // need to understand this better
		env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

		DataStreamSource<LogRecord> logRecordDataStreamSource = env.addSource(kafka);

		DataStreamSink<Task> tasks = logRecordDataStreamSource
			.keyBy(LogRecord::getMachine)
			.flatMap(new EventMapper())
			.addSink(JdbcSink.sink(
				"insert into task (machine, name, start_timestamp, stop_timestamp) values (?,?,?,?)",
				(ps, task) -> {
					ps.setString(1, task.getMachine());
					ps.setString(2, task.getName());
					ps.setString(3, task.getStartTimestamp());
					ps.setString(4, task.getStopTimestamp());
				},
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
					.withUrl("jdbc:postgresql://postgres:5432/database")
					.withUsername("postgres")
					.withPassword("postgres")
					.withDriverName("org.postgresql.Driver")
					.build()));

		env.execute("edge pipeline");
	}

	static class CustomSchema implements DeserializationSchema<LogRecord> {

		static ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public LogRecord deserialize(byte[] bytes) throws IOException {
			return objectMapper.readValue(bytes, LogRecord.class);
		}

		@Override
		public boolean isEndOfStream(LogRecord inputMessage) {
			return false;
		}

		@Override
		public TypeInformation<LogRecord> getProducedType() {
			return TypeInformation.of(LogRecord.class);
		}

	}

}
