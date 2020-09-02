package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

public class Processor {

	public static void main(String[] args) throws Exception {
		String kafkaTopic = "events";
		String broker = "localhost:9092";

		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", broker);
		kafkaProps.setProperty("group.id", "flink.consumer");
		kafkaProps.setProperty("enable.auto.commit", "false");

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

		DataStream<Task> tasks = logRecordDataStreamSource
			.keyBy(LogRecord::getMachine)
			.flatMap(new EventMapper());

//		tasks.print();

		env.execute("State machine job");

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
