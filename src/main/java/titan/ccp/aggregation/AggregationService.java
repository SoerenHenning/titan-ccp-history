package titan.ccp.aggregation;

import org.apache.kafka.streams.KafkaStreams;

import titan.ccp.model.sensorregistry.ProxySensorRegistry;

public class AggregationService {

	final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	final KafkaStreams kafkaStreams = new KafkaStreamsFactory().create();

	public AggregationService() {

	}

	public void run() {
		// TODO request sensorRegistry
		// sensorRegistry.setBackingSensorRegisty(backingSensorRegisty);
		// TODO request history for all sensors
		// sensorHistory.update(, );
		this.kafkaStreams.start();
		// TODO create Rest API
	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
