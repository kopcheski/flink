package org.apache.flink.streaming.examples.tasks;

import java.util.ArrayList;
import java.util.List;

public class LogRecordProducer {

	public List<LogRecord> produce() {
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
		logRecords.add(create("22:42", "stop", "C"));
		logRecords.add(create("22:43", "stop", "D"));
		return logRecords;
	}

	private LogRecord create(String s, String start, String a) {
		return new LogRecord(s, start, a);
	}

}
