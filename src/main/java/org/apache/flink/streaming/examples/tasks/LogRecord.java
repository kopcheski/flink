package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

@JsonSerialize
@JsonDeserialize
public class LogRecord {

	private long sequence;

	private String machine;

	private String timestamp;

	private String type;

	private String name;

	public LogRecord() {
	}

	public LogRecord(String machine, String timestamp, String type, String name, long sequence) {
		this.machine = machine;
		this.timestamp = timestamp;
		this.type = type;
		this.name = name;
		this.sequence = sequence;
	}

	public LogRecord(String timestamp, String type, String name, long sequence) {
		this("1234", timestamp, type, name, sequence);
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

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		LogRecord logRecord = (LogRecord) o;
		return sequence == logRecord.sequence &&
				machine.equals(logRecord.machine);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sequence, machine);
	}
}
