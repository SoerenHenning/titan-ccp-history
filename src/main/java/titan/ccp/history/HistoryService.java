package titan.ccp.history;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.KafkaSubscriber;
import titan.ccp.history.api.RestApiServer;
import titan.ccp.history.streamprocessing.KafkaStreamsBuilder;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.client.RetryingSensorRegistryRequester;
import titan.ccp.model.sensorregistry.client.SensorRegistryRequester;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 */
public class HistoryService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HistoryService.class);

  private final Configuration configuration = Configurations.create();
  private final RetryingSensorRegistryRequester sensorRegistryRequester;
  private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
  // private final KafkaStreams kafkaStreams;
  // private final RestApiServer restApiServer;

  // private final CompletableFuture<Void> stopEvent = new CompletableFuture();

  /**
   * Create an Aggregation service using a configuration via external parameters. These can be an
   * {@code application.properties} file or environment variables.
   */
  public HistoryService() {
    this.sensorRegistryRequester = new RetryingSensorRegistryRequester(new SensorRegistryRequester(
        this.configuration.getString(ConfigurationKeys.CONFIGURATION_HOST),
        this.configuration.getInt(ConfigurationKeys.CONFIGURATION_PORT)));
    // this.restApiServer = new RestApiServer(session);
  }

  /**
   * Start the service.
   */
  public void run() {
    // this.sensorRegistry.setBackingSensorRegisty(ExampleSensors.registry());
    final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
    this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

    final KafkaSubscriber configEventSubscriber =
        new KafkaSubscriber(this.configuration.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS),
            "titan-ccp-aggregation", // TODO group id
            this.configuration.getString(ConfigurationKeys.CONFIGURATION_KAFKA_TOPIC));
    configEventSubscriber.subscribe(Event.SENSOR_REGISTRY_CHANGED, data -> {
      this.sensorRegistry.setBackingSensorRegisty(SensorRegistry.fromJson(data));
      LOGGER.info("Received new sensor registry.");
    });
    configEventSubscriber.run();

    // Cassandra connect
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(this.configuration.getString(ConfigurationKeys.CASSANDRA_HOST))
        .port(this.configuration.getInt(ConfigurationKeys.CASSANDRA_PORT))
        .keyspace(this.configuration.getString(ConfigurationKeys.CASSANDRA_KEYSPACE)).build();
    // CompletableFuture.supplyAsync(() -> ... )
    // TODO stop missing

    // Create Kafka Streams Application
    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .bootstrapServers(this.configuration.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
        .inputTopic(this.configuration.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC))
        .outputTopic(this.configuration.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC))
        .sensorRegistry(this.sensorRegistry).cassandraSession(clusterSession.getSession()).build();
    kafkaStreams.start();
    // TODO stop missing
    // this.stopEvent.thenRun(() -> kafkaStreams.close())

    // Create Rest API
    // TODO use builder
    if (this.configuration.getBoolean(ConfigurationKeys.WEBSERVER_ENABLE)) {
      final RestApiServer restApiServer = new RestApiServer(clusterSession.getSession(),
          this.configuration.getInt(ConfigurationKeys.WEBSERVER_PORT),
          this.configuration.getBoolean(ConfigurationKeys.WEBSERVER_CORS));
      restApiServer.start();
      // TODO stop missing
    }

    // CompletableFuture<Void> stop = new CompletableFuture<>();
    // stop.thenRun(() -> clusterSession.getCluster().close());
    // stop.complete(null);

  }

  // public void stop() {
  // this.stopEvent.complete(null);
  // }

  public static void main(final String[] args) {
    new HistoryService().run();
  }

}
