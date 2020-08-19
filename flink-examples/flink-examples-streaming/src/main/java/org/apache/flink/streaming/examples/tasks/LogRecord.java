package org.apache.flink.streaming.examples.tasks;

import java.util.Objects;

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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		LogRecord logRecord = (LogRecord) o;
		return machine.equals(logRecord.machine) &&
			timestamp.equals(logRecord.timestamp) &&
			type.equals(logRecord.type) &&
			name.equals(logRecord.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(machine, timestamp, type, name);
	}
}
