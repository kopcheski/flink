package org.apache.flink.streaming.examples.tasks;

public class Task {

	private String machine;

	private String startTimestamp;

	private String stopTimestamp;

	private String name;

	public Task(String machine, String startTimestamp, String name) {
		this.machine = machine;
		this.startTimestamp = startTimestamp;
		this.name = name;
	}

	public String getMachine() {
		return machine;
	}

	public String getStartTimestamp() {
		return startTimestamp;
	}

	public String getName() {
		return name;
	}

	public String getStopTimestamp() {
		return stopTimestamp;
	}

	public void setStopTimestamp(String stopTimestamp) {
		this.stopTimestamp = stopTimestamp;
	}
}
