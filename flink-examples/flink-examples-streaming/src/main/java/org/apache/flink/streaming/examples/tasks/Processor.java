package org.apache.flink.streaming.examples.tasks;

import java.util.List;

public class Processor {

	public static void main(String[] args) {
		List<LogRecord> logRecords = new LogRecordProducer().produce();
		EventMapper eventMapper = new EventMapper();
		logRecords.forEach(eventMapper::process);
	}
}
