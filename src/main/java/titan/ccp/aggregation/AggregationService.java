package titan.ccp.aggregation;

import org.apache.kafka.streams.KafkaStreams;

import titan.ccp.model.sensorregistry.ProxySensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class AggregationService {

	private final SensorRegistryRequester sensorRegistryRequester = new SensorRegistryRequester("");
	private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	private final KafkaStreams kafkaStreams = new KafkaStreamsFactory().create();

	public AggregationService() {

	}

	public void run() {
		// TODO request sensorRegistry
		// sensorRegistry.setBackingSensorRegisty(backingSensorRegisty);
		// TODO handle unavailability
		final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
		this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

		// TODO request history for all sensors
		// sensorHistory.update(, );
		this.kafkaStreams.start();
		// TODO create Rest API
	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
