package org.apache.flink.streaming.examples.tasks;

import java.util.Objects;

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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Task task = (Task) o;
		return machine.equals(task.machine) &&
			name.equals(task.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(machine, name);
	}
}
