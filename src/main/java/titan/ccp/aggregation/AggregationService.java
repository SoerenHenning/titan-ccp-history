package titan.ccp.aggregation;

import teetime.framework.Configuration;
import teetime.framework.Execution;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class AggregationService {

	final SensorRegistry sensorRegistry = null; // TODO
	final SensorHistory sensorHistory = LastValueSensorHistory.createForMultipleThreads(); // TODO
	final Execution<Configuration> execution;

	public AggregationService() {
		final AggregationConfiguration configuration = new AggregationConfiguration(this.sensorRegistry,
				this.sensorHistory);
		this.execution = new Execution<>(configuration);
	}

	public void run() {
		// TODO request sensorRegistry
		// TODO request history for all sensors
		this.execution.executeNonBlocking();
	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
