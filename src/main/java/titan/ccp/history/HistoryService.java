package titan.ccp.history;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.TimeWindows;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.configuration.ServiceConfigurations;
import titan.ccp.history.api.RestApiServer;
import titan.ccp.history.streamprocessing.KafkaStreamsBuilder;
import titan.ccp.history.streamprocessing.TimeWindowsConfiguration;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 */
public class HistoryService {

  private final Configuration config = ServiceConfigurations.createWithDefaults();

  private final CompletableFuture<Void> stopEvent = new CompletableFuture<>();

  /**
   * Start the service.
   *
   * @return {@link CompletableFuture} which is completed when the service is successfully started.
   */
  public CompletableFuture<Void> run() {
    final CompletableFuture<ClusterSession> clusterSessionStarter =
        CompletableFuture.supplyAsync(this::startCassandraSession);
    final CompletableFuture<Void> streamsStarter =
        clusterSessionStarter.thenAcceptAsync(this::createKafkaStreamsApplication);
    final CompletableFuture<Void> webserverStarter =
        clusterSessionStarter.thenAcceptAsync(this::startWebserver);
    return CompletableFuture.allOf(streamsStarter, webserverStarter);
  }

  /**
   * Connect to the database.
   *
   * @return the {@link ClusterSession} for the cassandra cluster.
   */
  private ClusterSession startCassandraSession() {
    // Cassandra connect
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(this.config.getString(ConfigurationKeys.CASSANDRA_HOST))
        .port(this.config.getInt(ConfigurationKeys.CASSANDRA_PORT))
        .keyspace(this.config.getString(ConfigurationKeys.CASSANDRA_KEYSPACE))
        .timeoutInMillis(this.config.getInt(ConfigurationKeys.CASSANDRA_INIT_TIMEOUT_MS)).build();
    this.stopEvent.thenRun(clusterSession.getSession()::close);
    return clusterSession;
  }

  /**
   * Build and start the underlying Kafka Streams application of the service.
   *
   * @param clusterSession the database session which the application should use.
   */
  private void createKafkaStreamsApplication(final ClusterSession clusterSession) {

    TemporalUnit unit = null;
    switch (this.config.getString("timeWindow.tenSec.unit")) {
      case "s":
        unit = ChronoUnit.SECONDS;
        break;
      case "m":
        unit = ChronoUnit.MINUTES;
        break;
      case "d":
        unit = ChronoUnit.DAYS;
        break;
      case "mo":
        unit = ChronoUnit.MONTHS;
        break;
      case "y":
        unit = ChronoUnit.YEARS;
        break;
      default:
        break;
    }

    final Duration duration = Duration.of(this.config.getLong("timeWindow.tenSec.time"), unit);

    final TimeWindowsConfiguration timeWindowsConfiguration = new TimeWindowsConfiguration(
        this.config.getString("timeWindow.tenSec.kafka"),
        this.config.getString("timeWindow.tenSec.cassandra"),
        TimeWindows.of(duration));

    final KafkaStreams kafkaStreams =
        new KafkaStreamsBuilder()
            .cassandraSession(clusterSession.getSession())
            .bootstrapServers(this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
            .inputTopic(this.config.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC))
            .outputTopic(this.config.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC))
            .timeWindowsConfiguration(timeWindowsConfiguration)
            .schemaRegistry(this.config.getString(ConfigurationKeys.SCHEMA_REGISTRY_URL))
            .numThreads(this.config.getInt(ConfigurationKeys.NUM_THREADS))
            .commitIntervalMs(this.config.getInt(ConfigurationKeys.COMMIT_INTERVAL_MS))
            .cacheMaxBytesBuffering(this.config.getInt(ConfigurationKeys.CACHE_MAX_BYTES_BUFFERING))
            .build();
    this.stopEvent.thenRun(kafkaStreams::close);
    kafkaStreams.start();
  }

  /**
   * Start the webserver of the service.
   *
   * @param clusterSession the database session which the server should use.
   */
  private void startWebserver(final ClusterSession clusterSession) {
    if (this.config.getBoolean(ConfigurationKeys.WEBSERVER_ENABLE)) {
      final RestApiServer restApiServer = new RestApiServer(
          clusterSession.getSession(),
          this.config.getInt(ConfigurationKeys.WEBSERVER_PORT),
          this.config.getBoolean(ConfigurationKeys.WEBSERVER_CORS),
          this.config.getBoolean(ConfigurationKeys.WEBSERVER_GZIP));
      this.stopEvent.thenRun(restApiServer::stop);
      restApiServer.start();
    }
  }

  /**
   * Stop the service.
   */
  public void stop() {
    this.stopEvent.complete(null);
  }

  public static void main(final String[] args) {
    new HistoryService().run().join();
  }

}
