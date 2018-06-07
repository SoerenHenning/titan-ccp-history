package titan.ccp.aggregation;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import titan.ccp.aggregation.api.RestApiServer;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.KafkaSubscriber;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.client.RetryingSensorRegistryRequester;
import titan.ccp.model.sensorregistry.client.SensorRegistryRequester;

public class AggregationService {

	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationService.class);

	private final Configuration configuration = Configurations.create();
	private final RetryingSensorRegistryRequester sensorRegistryRequester;
	private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
	// private final KafkaStreams kafkaStreams;
	// private final RestApiServer restApiServer;

	// private final CompletableFuture<Void> stopEvent = new CompletableFuture();

	public AggregationService() {
		this.sensorRegistryRequester = new RetryingSensorRegistryRequester(new SensorRegistryRequester(
				this.configuration.getString("configuration.host"), this.configuration.getInt("configuration.port")));
		// this.restApiServer = new RestApiServer(session);
	}

	public void run() {
		// this.sensorRegistry.setBackingSensorRegisty(ExampleSensors.registry());
		final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
		this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

		final KafkaSubscriber configEventSubscriber = new KafkaSubscriber(
				this.configuration.getString("kafka.bootstrap.servers"), "titan-ccp-aggregation",
				this.configuration.getString("configuration.kafka.topic"));
		configEventSubscriber.subscribe(Event.SENSOR_REGISTRY_CHANGED, data -> {
			this.sensorRegistry.setBackingSensorRegisty(SensorRegistry.fromJson(data));
			LOGGER.info("Received new sensor registry.");
		});
		configEventSubscriber.run();

		// Cassandra connect
		final ClusterSession clusterSession = new SessionBuilder()
				.contactPoint(this.configuration.getString("cassandra.host"))
				.port(this.configuration.getInt("cassandra.port"))
				.keyspace(this.configuration.getString("cassandra.keyspace")).build();
		// CompletableFuture.supplyAsync(() -> ... )
		// TODO stop missing

		// Create Kafka Streams Application
		final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
				.bootstrapServers(this.configuration.getString("kafka.bootstrap.servers"))
				.inputTopic(this.configuration.getString("kafka.input.topic"))
				.outputTopic(this.configuration.getString("kafka.output.topic")).sensorRegistry(this.sensorRegistry)
				.cassandraSession(clusterSession.getSession()).build();
		kafkaStreams.start();
		// TODO stop missing
		// this.stopEvent.thenRun(() -> kafkaStreams.close())

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

	// public void stop() {
	// this.stopEvent.complete(null);
	// }

	public static void main(final String[] args) {
		new AggregationService().run();
	}

}
