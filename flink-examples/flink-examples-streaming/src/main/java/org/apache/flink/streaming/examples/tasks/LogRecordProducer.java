package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class LogRecordProducer extends RichParallelSourceFunction<LogRecord> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<LogRecord> sourceContext) throws Exception {
		sourceContext.collect(create("22:30", "start", "A"));
		sourceContext.collect(create("22:31", "none", ""));
		sourceContext.collect(create("22:32", "none", ""));
		sourceContext.collect(create("22:33", "start", "B"));
		sourceContext.collect(create("22:34", "none", ""));
		sourceContext.collect(create("22:35", "stop", "B"));
		sourceContext.collect(create("22:36", "start", "C"));
		sourceContext.collect(create("22:37", "stop", "A"));
		sourceContext.collect(create("22:38", "none", ""));
		sourceContext.collect(create("22:39", "none", ""));
		sourceContext.collect(create("22:40", "start", "D"));
		sourceContext.collect(create("22:41", "none", ""));
		sourceContext.collect(create("22:42", "none", ""));
		sourceContext.collect(create("22:43", "none", ""));
		sourceContext.collect(create("22:44", "none", ""));
		sourceContext.collect(create("22:45", "none", ""));
		sourceContext.collect(create("22:46", "none", ""));
		sourceContext.collect(create("22:47", "none", ""));
		sourceContext.collect(create("22:48", "stop", "C"));
		sourceContext.collect(create("22:49", "stop", "D"));
	}

	@Override
	public void cancel() {
		this.running = false;
	}

	private LogRecord create(String timestamp, String type, String name) {
//		try {
//			Thread.sleep(1000L);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		return new LogRecord(timestamp, type, name);
	}
}
