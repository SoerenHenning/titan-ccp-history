package titan.ccp.aggregation.experimental.teetimebased;

import teetime.framework.Execution;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;

public class AggregationService {

	final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	final SensorHistory sensorHistory = LastValueSensorHistory.createForMultipleThreads();
	final Execution<AggregationConfiguration> execution;

	public AggregationService() {
		final AggregationConfiguration configuration = new AggregationConfiguration(this.sensorRegistry,
				this.sensorHistory);
		this.execution = new Execution<>(configuration);
	}

	public void run() {
		// TODO request sensorRegistry
		// sensorRegistry.setBackingSensorRegisty(backingSensorRegisty);
		// TODO request history for all sensors
		// sensorHistory.update(, );
		this.execution.executeNonBlocking();
		// TODO create Rest API
	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
