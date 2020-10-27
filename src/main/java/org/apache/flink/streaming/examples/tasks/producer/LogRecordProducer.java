package org.apache.flink.streaming.examples.tasks.producer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.examples.tasks.LogRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LogRecordProducer {

	static long sequence = 0;

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		List<LogRecord> logRecords = run();
		Producer<String,LogRecord> kafkaProducer = new KafkaProducer<>(
			properties,
			new StringSerializer(),
			new KafkaJsonSerializer()
		);
		for (LogRecord logRecord : logRecords) {
			kafkaProducer.send(new ProducerRecord("events", "0", logRecord));
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		kafkaProducer.flush();
	}

	private static List<LogRecord> run() {
		List<LogRecord> logRecords = new ArrayList<>();
		logRecords.add(create("22:30", "start", "A"));
		logRecords.add(create("22:31", "none", ""));
		logRecords.add(create("22:32", "none", ""));
		logRecords.add(create("22:33", "start", "B"));
		logRecords.add(create("22:34", "none", ""));
		logRecords.add(create("22:35", "stop", "B"));
		logRecords.add(create("22:36", "start", "C"));
		logRecords.add(create("22:37", "stop", "A"));
		logRecords.add(create("22:38", "none", ""));
		logRecords.add(create("22:39", "none", ""));
		logRecords.add(create("22:40", "start", "D"));
		logRecords.add(create("22:41", "none", ""));
		logRecords.add(create("22:42", "none", ""));
		logRecords.add(create("22:43", "none", ""));
		logRecords.add(create("22:44", "none", ""));
		logRecords.add(create("22:45", "none", ""));
		logRecords.add(create("22:46", "none", ""));
		logRecords.add(create("22:47", "none", ""));
		logRecords.add(create("22:48", "stop", "C"));
		logRecords.add(create("22:49", "stop", "D"));
		return logRecords;
	}

	public static LogRecord create(String timestamp, String type, String name) {
		return new LogRecord(timestamp, type, name, sequence++);
	}

	static class KafkaJsonDeserializer<T> implements Deserializer {

		private Logger logger = LogManager.getLogger(this.getClass());

		private Class <T> type;

		public KafkaJsonDeserializer(Class type) {
			this.type = type;
		}

		@Override
		public void configure(Map map, boolean b) {

		}

		@Override
		public Object deserialize(String s, byte[] bytes) {
			ObjectMapper mapper = new ObjectMapper();
			T obj = null;
			try {
				obj = mapper.readValue(bytes, type);
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
			return obj;
		}

		@Override
		public void close() {

		}
	}

	static class KafkaJsonSerializer implements Serializer {

		private Logger logger = LogManager.getLogger(this.getClass());

		@Override
		public void configure(Map map, boolean b) {

		}

		@Override
		public byte[] serialize(String s, Object o) {
			byte[] retVal = null;
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				retVal = objectMapper.writeValueAsBytes(o);
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
			return retVal;
		}

		@Override
		public void close() {

		}
	}

}


