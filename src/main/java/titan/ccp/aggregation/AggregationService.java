package titan.ccp.aggregation;

import org.apache.kafka.streams.KafkaStreams;

import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.model.sensorregistry.ExampleSensors;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;

public class AggregationService {

	private final SensorRegistryRequester sensorRegistryRequester = new SensorRegistryRequester("");
	private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	private final KafkaStreams kafkaStreams;
	// private final RestApiServer restApiServer;

	public AggregationService() {
		this.kafkaStreams = new KafkaStreamsBuilder().sensorRegistry(this.sensorRegistry).build();
		// this.restApiServer = new RestApiServer(session);
	}

	public void run() {
		// TODO request sensorRegistry
		this.sensorRegistry.setBackingSensorRegisty(ExampleSensors.registry());
		// sensorRegistry.setBackingSensorRegisty(backingSensorRegisty);
		// TODO handle unavailability
		// final SensorRegistry sensorRegistry =
		// this.sensorRegistryRequester.request().join();
		// this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

		// TODO request history for all sensors
		// sensorHistory.update(, );
		this.kafkaStreams.start();

		// Create Rest API
		final ClusterSession clusterSession = new SessionBuilder().contactPoint("localhost").port(9042)
				.keyspace("titanccp").build();
		final RestApiServer restApiServer = new RestApiServer(clusterSession.getSession());
		restApiServer.start();

	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
