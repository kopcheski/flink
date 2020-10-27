package org.apache.flink.streaming.examples.tasks;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.examples.tasks.producer.LogRecordProducer.create;
import static org.junit.Assert.assertEquals;

public class PipelineTest {

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster =
		new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setNumberSlotsPerTaskManager(2)
				.setNumberTaskManagers(1)
				.build());

	@Test
	public void givenEvents_whenGoingThroughThePipeline_thenATaskIsCreated() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// configure your test environment
		env.setParallelism(2);

		// values are collected in a static variable
		OnlyOneMachineSink.values.clear();

		// create a stream of custom elements and apply transformations
		env.fromElements(create("22:30", "start", "A"), create("22:31", "stop", "A"))
			.keyBy(LogRecord::getMachine)
			.flatMap(new EventMapper())
			.addSink(new OnlyOneMachineSink());

		// execute
		env.execute();

		// verify your results
		Task expectedTask = new Task("1234", "22:30", "A");
		assertEquals(expectedTask, OnlyOneMachineSink.values.get(expectedTask.getName()));
	}

	// create a testing sink
	private static class OnlyOneMachineSink implements SinkFunction<Task> {

		// must be static
		public static final Map<String, Task> values = new HashMap<>();

		@Override
		public synchronized void invoke(Task value) throws Exception {
			values.put(value.getName(), value);
		}
	}
}
