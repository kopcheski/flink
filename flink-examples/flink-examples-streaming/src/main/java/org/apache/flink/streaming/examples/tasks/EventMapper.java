package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class EventMapper extends RichFlatMapFunction<LogRecord, Task> {

	MapState<String, Task> openedTasks;

	@Override
	public void open(Configuration conf) {
		openedTasks = getRuntimeContext()
			.getMapState(new MapStateDescriptor<>("state", String.class, Task.class));
	}

	@Override
	public void flatMap(LogRecord logRecord, Collector<Task> out) throws Exception {
		if (logRecord.getType().equals("start")) {
			Task task = new Task(logRecord.getMachine(), logRecord.getTimestamp(), logRecord.getName());
			openedTasks.put(task.getName(), task);
			System.out.println(String.format("Task %s started at %s", task.getName(), task.getStartTimestamp()));
		} else if (logRecord.getType().equals("stop")) {
			Task task = openedTasks.get(logRecord.getName());
			task.setStopTimestamp(logRecord.getTimestamp());
			System.out.println(String.format("Task %s stopped at %s", task.getName(), task.getStopTimestamp()));
			openedTasks.remove(logRecord.getName());
		} else {
//			System.out.println(String.format("Event of type %s was ignored", logRecord.getType()));
		}

	}
}
