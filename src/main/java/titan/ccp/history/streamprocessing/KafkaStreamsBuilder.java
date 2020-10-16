package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import titan.ccp.common.kafka.streams.PropertiesBuilder;

/**
 * Builder for the Kafka Streams configuration.
 */
public class KafkaStreamsBuilder {
  // private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private String applicationName = "titan-ccp-history"; // NOPMD
  private String applicationVersion = "dev"; // NOPMD
  private Session cassandraSession; // NOPMD
  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private List<TimeWindowsConfiguration> timeWindowsConfigurations; // NOPMD
  private String schemaRegistryUrl; // NOPMD
  private int numThreads = -1; // NOPMD
  private int commitIntervalMs = -1; // NOPMD
  private int cacheMaxBytesBuff = -1; // NOPMD

  /**
   * Sets the application name of the kafka streams application. Used for the ID.
   *
   * @param applicationName Name of the application
   * @return
   */
  public KafkaStreamsBuilder applicationName(final String applicationName) {
    // check if it is the not replaced name for the application (when executed with IDE)
    if ("@application.name@".equals(applicationName)) {
      return this;
    }
    this.applicationName = applicationName;
    return this;
  }

  /**
   * Sets the application version of the kafka streams application. Used for the ID.
   *
   * @param applicationVersion Version of the application
   * @return
   */
  public KafkaStreamsBuilder applicationVersion(final String applicationVersion) {
    // check if it is the not replaced version for the application (when executed with IDE)
    if ("@application.version@".equals(applicationVersion)) {
      return this;
    }
    this.applicationVersion = applicationVersion;
    return this;
  }

  public KafkaStreamsBuilder cassandraSession(final Session cassandraSession) {
    this.cassandraSession = cassandraSession;
    return this;
  }

  public KafkaStreamsBuilder inputTopic(final String inputTopic) {
    this.inputTopic = inputTopic;
    return this;
  }

  public KafkaStreamsBuilder outputTopic(final String outputTopic) {
    this.outputTopic = outputTopic;
    return this;
  }

  public KafkaStreamsBuilder timeWindowsConfigurations(
      final List<TimeWindowsConfiguration> timeWindowsConfigurations) {
    this.timeWindowsConfigurations = timeWindowsConfigurations;
    return this;
  }

  public KafkaStreamsBuilder schemaRegistry(final String url) {
    this.schemaRegistryUrl = url;
    return this;
  }

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the number of threads (num.stream.threads). Can be minus
   * one for using the default.
   */
  public KafkaStreamsBuilder numThreads(final int numThreads) {
    if (numThreads < -1 || numThreads == 0) {
      throw new IllegalArgumentException("Number of threads must be greater 0 or -1.");
    }
    this.numThreads = numThreads;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the frequency with which to save the position (offsets in
   * source topics) of tasks (commit.interval.ms). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder commitIntervalMs(final int commitIntervalMs) {
    if (commitIntervalMs < -1) {
      throw new IllegalArgumentException("Commit interval must be greater or equal -1.");
    }
    this.commitIntervalMs = commitIntervalMs;
    return this;
  }

  /**
   * Sets the Kafka Streams property for maximum number of memory bytes to be used for record caches
   * across all threads (cache.max.bytes.buffering). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder cacheMaxBytesBuffering(final int cacheMaxBytesBuffering) {
    if (cacheMaxBytesBuffering < -1) {
      throw new IllegalArgumentException("Cache max bytes buffering must be greater or equal -1.");
    }
    this.cacheMaxBytesBuff = cacheMaxBytesBuffering;
    return this;
  }

  /**
   * Builds the {@link KafkaStreams} instance.
   */
  public KafkaStreams build() {
    Objects.requireNonNull(this.inputTopic, "Input topic has not been set.");
    Objects.requireNonNull(this.outputTopic, "Output topic has not been set.");
    Objects.requireNonNull(this.cassandraSession, "Cassandra session has not been set.");
    // TODO log parameters
    final TopologyBuilder topologyBuilder = new TopologyBuilder(
        new Serdes(this.schemaRegistryUrl),
        this.inputTopic,
        this.outputTopic,
        this.timeWindowsConfigurations,
        this.cassandraSession);
    final Properties properties = PropertiesBuilder
        .bootstrapServers(this.bootstrapServers)
        .applicationId(this.applicationName + '-' + this.applicationVersion)
        .set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, this.numThreads, p -> p > 0)
        .set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, this.commitIntervalMs, p -> p >= 0)
        .set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, this.cacheMaxBytesBuff, p -> p >= 0)
        .build();
    return new KafkaStreams(topologyBuilder.build(), properties);
  }

}
