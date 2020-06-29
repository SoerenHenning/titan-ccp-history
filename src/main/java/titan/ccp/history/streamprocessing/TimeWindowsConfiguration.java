package titan.ccp.history.streamprocessing;

import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Class to initialize a configuration for a time window that should be observed and written to
 * Kafka and Cassandra.
 */
public class TimeWindowsConfiguration {

  private final String kafkaTopic;
  private final String cassandraTableName;
  private final TimeWindows timeWindows;

  /**
   * Creates a new {@code TimeWindowsConfiguration}.
   *
   * @param kafkTopic The kafka topic to write the aggregation to.
   * @param cassandraTableName Table name for cassandra.
   * @param timeWindows The time window that should be used for aggregation.
   */
  public TimeWindowsConfiguration(final String kafkTopic, final String cassandraTableName,
      final TimeWindows timeWindows) {
    this.kafkaTopic = kafkTopic;
    this.cassandraTableName = cassandraTableName;
    this.timeWindows = timeWindows;
  }

  public String getKafkaTopic() {
    return this.kafkaTopic;
  }

  public String getCassandraTableName() {
    return this.cassandraTableName;
  }

  public TimeWindows getTimeWindows() {
    return this.timeWindows;
  }
}
