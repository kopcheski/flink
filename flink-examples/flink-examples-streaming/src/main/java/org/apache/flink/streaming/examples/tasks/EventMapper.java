package org.apache.flink.streaming.examples.tasks;

import java.util.HashMap;

public class EventMapper {

	HashMap<String, Task> openedTasks = new HashMap<String, Task>();

	public void process(LogRecord logRecord) {
		if (logRecord.getType().equals("start")) {
			Task task = new Task(logRecord.getMachine(), logRecord.getTimestamp(), logRecord.getName());
			openedTasks.put(task.getName(), task);
			System.out.println(String.format("Task %s started at %s", task.getName(), task.getStartTimestamp()));
		} else if (logRecord.getType().equals("stop")) {
			Task task = openedTasks.remove(logRecord.getName());
			task.setStopTimestamp(logRecord.getTimestamp());
			System.out.println(String.format("Task %s stopped at %s", task.getName(), task.getStopTimestamp()));
		} else {
//			System.out.println(String.format("Event of type %s was ignored", logRecord.getType()));
		}

		try {
			Thread.sleep(1000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
