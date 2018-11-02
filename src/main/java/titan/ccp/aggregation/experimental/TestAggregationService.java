package titan.ccp.aggregation.experimental;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.aggregation.ConfigurationKeys;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.model.sensorregistry.ProxySensorRegistry;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 * <p>
 * Will be soon renamed to HistoryService.
 * </p>
 *
 */
public class TestAggregationService {

  private final Configuration configuration = Configurations.create();
  private final ProxySensorRegistry sensorRegistry = new ProxySensorRegistry();

  /**
   * Create an Aggregation service using a configuration via external parameters. These can be an
   * {@code application.properties} file or environment variables.
   */
  public TestAggregationService() {}

  /**
   * Start the service.
   */
  public void run() {
    // this.sensorRegistry.setBackingSensorRegisty(ExampleSensors.registry());
    // final SensorRegistry sensorRegistry = this.sensorRegistryRequester.request().join();
    // this.sensorRegistry.setBackingSensorRegisty(sensorRegistry);

    // Cassandra connect
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(this.configuration.getString(ConfigurationKeys.CASSANDRA_HOST))
        .port(this.configuration.getInt(ConfigurationKeys.CASSANDRA_PORT))
        .keyspace(this.configuration.getString(ConfigurationKeys.CASSANDRA_KEYSPACE)).build();

    // Create Kafka Streams Application
    final KafkaStreams kafkaStreams = new TestKafkaStreamsBuilder()
        .bootstrapServers(this.configuration.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
        .inputTopic(this.configuration.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC))
        .outputTopic(this.configuration.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC))
        .sensorRegistry(this.sensorRegistry).cassandraSession(clusterSession.getSession()).build();
    kafkaStreams.start();

  }


  public static void main(final String[] args) {
    new TestAggregationService().run();
  }

}
