package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;

public class Pipeline {

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
		kafka.setStartFromEarliest();
		kafka.setCommitOffsetsOnCheckpoints(true);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1); // need to understand this better
		env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

		DataStreamSink<Task> tasks = env.addSource(kafka)
			.keyBy(LogRecord::getMachine)
			.flatMap(new EventMapper())
			.addSink(TaskSink.sink());

		tasks.name("Tasks sink");
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
