package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Builder for the Kafka Streams configuration.
 */
public class KafkaStreamsBuilder {

  private static final String APPLICATION_ID = "titanccp-aggregation-0.0.16";

  private static final int COMMIT_INTERVAL_MS = 1000;

  // private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private String configurationTopic; // NOPMD
  private Session cassandraSession; // NOPMD

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

  public KafkaStreamsBuilder configurationTopic(final String configurationTopic) {
    this.configurationTopic = configurationTopic;
    return this;
  }

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  /**
   * Builds the {@link KafkaStreams} instance.
   */
  public KafkaStreams build() {
    Objects.requireNonNull(this.inputTopic, "Input topic has not been set.");
    Objects.requireNonNull(this.outputTopic, "Output topic has not been set.");
    Objects.requireNonNull(this.configurationTopic, "Configuration topic has not been set.");
    Objects.requireNonNull(this.cassandraSession, "Cassandra session has not been set.");
    // TODO log parameters
    final TopologyBuilder topologyBuilder = new TopologyBuilder(
        this.inputTopic,
        this.outputTopic,
        this.configurationTopic,
        this.cassandraSession);
    return new KafkaStreams(topologyBuilder.build(), this.buildProperties());
  }

  private Properties buildProperties() {
    final Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // TODO as parameter
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS); // TODO as param.
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    return properties;
  }

}
