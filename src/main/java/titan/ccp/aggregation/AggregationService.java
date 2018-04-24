package titan.ccp.aggregation;

import org.apache.kafka.streams.KafkaStreams;

import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.model.sensorregistry.ExampleSensors;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;

public class AggregationService {

	private static final int WEBSERVER_PORT = 8080; // TODO as parameter

	private static final String CASSANDRA_HOST = "localhost"; // TODO as parameter

	private static final int CASSANDRA_PORT = 9042; // TODO as parameter

	private final SensorRegistryRequester sensorRegistryRequester = new SensorRegistryRequester("");
	private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	// private final KafkaStreams kafkaStreams;
	// private final RestApiServer restApiServer;

	public AggregationService() {

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

		// Cassandra connect
		final ClusterSession clusterSession = new SessionBuilder().contactPoint(CASSANDRA_HOST).port(CASSANDRA_PORT)
				.keyspace("titanccp").build();
		// CompletableFuture.supplyAsync(() -> ... )
		// TODO stop missing

		// Create Kafka Streams Application
		final KafkaStreams kafkaStreams = new KafkaStreamsBuilder().sensorRegistry(this.sensorRegistry)
				.cassandraSession(clusterSession.getSession()).build();
		kafkaStreams.start();
		// TODO stop missing

		// Create Rest API
		final RestApiServer restApiServer = new RestApiServer(clusterSession.getSession(), WEBSERVER_PORT);
		restApiServer.start();
		// TODO stop missing

		// CompletableFuture<Void> stop = new CompletableFuture<>();
		// stop.thenRun(() -> clusterSession.getCluster().close());
		// stop.complete(null);

	}

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
