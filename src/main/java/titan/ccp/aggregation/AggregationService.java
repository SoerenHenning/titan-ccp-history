package titan.ccp.aggregation;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;

import titan.ccp.common.configuration.Configurations;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.model.sensorregistry.ExampleSensors;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.client.SensorRegistryRequester;

public class AggregationService {
	private final Configuration configuration = Configurations.create();
	private final SensorRegistryRequester sensorRegistryRequester;
	private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	// private final KafkaStreams kafkaStreams;
	// private final RestApiServer restApiServer;

	public AggregationService() {
		this.sensorRegistryRequester = new SensorRegistryRequester(this.configuration.getString("configuration.host"),
				this.configuration.getInt("configuration.port"));
		// this.restApiServer = new RestApiServer(session);
	}

	public void run() {
		// TODO request sensorRegistry
		this.sensorRegistry.setBackingSensorRegisty(ExampleSensors.registry());
		// sensorRegistry.setBackingSensorRegisty(backingSensorRegisty);
		// TODO handle unavailability
		final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
		this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

		// TODO request history for all sensors
		// sensorHistory.update(, );

		// Cassandra connect
		final ClusterSession clusterSession = new SessionBuilder()
				.contactPoint(this.configuration.getString("cassandra.host"))
				.port(this.configuration.getInt("cassandra.port"))
				.keyspace(this.configuration.getString("cassandra.keyspace")).build();
		// CompletableFuture.supplyAsync(() -> ... )
		// TODO stop missing

		// Create Kafka Streams Application
		final KafkaStreams kafkaStreams = new KafkaStreamsBuilder().sensorRegistry(this.sensorRegistry)
				.cassandraSession(clusterSession.getSession()).build();
		kafkaStreams.start();
		// TODO stop missing

		// Create Rest API
		// TODO use builder
		final RestApiServer restApiServer = new RestApiServer(clusterSession.getSession(),
				this.configuration.getInt("webserver.port"), this.configuration.getBoolean("webserver.cors"));
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
