package org.apache.flink.streaming.examples.tasks;

public class LogRecord {

	private String machine;

	private String timestamp;

	private String type;

	private String name;

	public LogRecord(String machine, String timestamp, String type, String name) {
		this.machine = machine;
		this.timestamp = timestamp;
		this.type = type;
		this.name = name;
	}

	public LogRecord(String timestamp, String type, String name) {
		this("1234", timestamp, type, name);
	}

	public String getMachine() {
		return machine;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}
}
