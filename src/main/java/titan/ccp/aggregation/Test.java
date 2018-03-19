package titan.ccp.aggregation;

import kieker.common.record.IMonitoringRecord;
import teetime.framework.Configuration;
import teetime.framework.ConfigurationBuilder;
import teetime.framework.Execution;
import teetime.stage.InstanceOfFilter;
import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class Test {

	public static void main(final String[] args) {

		// Read monitoring records (Send records)
		// Aggregate (Send AggregatedRecord(SensorClass,Timestamp,Statistics))
		// Store

		final SensorRegistry sensorRegistry = null; // TODO
		final SensorHistory sensorHistory = LastValueSensorHistory.createForMultipleThreads(); // TODO

		final KafkaReaderStage kafkaReader = new KafkaReaderStage();
		final InstanceOfFilter<IMonitoringRecord, PowerConsumptionRecord> instanceOfFilter = new InstanceOfFilter<>(
				PowerConsumptionRecord.class);
		final AggregatorStage aggregator = new AggregatorStage(sensorRegistry, sensorHistory);

		final Configuration configuration = ConfigurationBuilder.from(kafkaReader)
				.to(instanceOfFilter, s -> s.getInputPort(), s -> s.getMatchedOutputPort()).end(aggregator);

		final Execution<Configuration> execution = new Execution<>(configuration);
		execution.executeNonBlocking();

	}

}
