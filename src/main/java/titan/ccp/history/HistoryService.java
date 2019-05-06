package titan.ccp.history;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.history.api.RestApiServer;
import titan.ccp.history.streamprocessing.KafkaStreamsBuilder;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 */
public class HistoryService {

  // private static final Logger LOGGER = LoggerFactory.getLogger(HistoryService.class);

  private final Configuration config = Configurations.create();
  // private final SensorRegistryRequester sensorRegistryRequester;
  // private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();
  // private final KafkaStreams kafkaStreams;
  // private final RestApiServer restApiServer;

  // private final CompletableFuture<Void> stopEvent = new CompletableFuture();

  /// **
  // * Create a History service using a configuration via external parameters. These can be an
  // * {@code application.properties} file or environment variables.
  // */
  // public HistoryService() {
  // this.sensorRegistryRequester =
  // new RetryingSensorRegistryRequester(new HttpSensorRegistryRequester(
  // this.config.getString(ConfigurationKeys.CONFIGURATION_HOST),
  // this.config.getInt(ConfigurationKeys.CONFIGURATION_PORT)));
  // this.restApiServer = new RestApiServer(session);
  // }

  /**
   * Start the service.
   */
  public void run() {
    // TODO
    // final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
    // this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

    // Cassandra connect
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(this.config.getString(ConfigurationKeys.CASSANDRA_HOST))
        .port(this.config.getInt(ConfigurationKeys.CASSANDRA_PORT))
        .keyspace(this.config.getString(ConfigurationKeys.CASSANDRA_KEYSPACE))
        .build();
    // CompletableFuture.supplyAsync(() -> ... )
    // TODO stop missing

    // Create Kafka Streams Application
    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .cassandraSession(clusterSession.getSession())
        .bootstrapServers(this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
        .inputTopic(this.config.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC))
        .outputTopic(this.config.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC))
        .configurationTopic(this.config.getString(ConfigurationKeys.CONFIGURATION_KAFKA_TOPIC))
        .numThreads(this.config.getInt(ConfigurationKeys.NUM_THREADS))
        .commitIntervalMs(this.config.getInt(ConfigurationKeys.COMMIT_INTERVAL_MS))
        .cacheMaxBytesBuffering(this.config.getInt(ConfigurationKeys.CACHE_MAX_BYTES_BUFFERING))
        // .registryRequester(this.sensorRegistryRequester)
        .build();
    kafkaStreams.start();
    // TODO stop missing
    // this.stopEvent.thenRun(() -> kafkaStreams.close())

    // Create Rest API
    // TODO use builder
    if (this.config.getBoolean(ConfigurationKeys.WEBSERVER_ENABLE)) {
      final RestApiServer restApiServer = new RestApiServer(
          clusterSession.getSession(),
          this.config.getInt(ConfigurationKeys.WEBSERVER_PORT),
          this.config.getBoolean(ConfigurationKeys.WEBSERVER_CORS));
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
