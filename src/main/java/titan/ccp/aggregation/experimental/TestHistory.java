package titan.ccp.aggregation.experimental;

import com.datastax.driver.core.Session;
import java.util.Objects;
import java.util.Properties;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import redis.clients.jedis.Jedis;
import titan.ccp.common.kieker.cassandra.CassandraWriter;
import titan.ccp.common.kieker.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.kieker.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;

/**
 * Test class o only write records into database.
 */
public final class TestHistory {

  private static final String REDIS_OUTPUT_COUNTER_KEY = "output_counter";

  private TestHistory() {}

  // private static final Logger LOGGER = LoggerFactory.getLogger(TestHistory.class);

  /**
   * Main method to start this simplified service.
   */
  public static void main(final String[] args) {
    final String cassandraHost =
        Objects.requireNonNullElse(System.getenv("CASSANDRA_HOST"), "localhost");
    final int cassandraPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("CASSANDRA_PORT"), "9042"));
    final String cassandraKeyspace =
        Objects.requireNonNullElse(System.getenv("CASSANDRA_KEYSPACE"), "titanccp");
    final String kafkaBootstrapServers =
        Objects.requireNonNullElse(System.getenv("KAFKA_BOOTSTRAP_SERVERS"), "localhost:9092");
    final String kafkaApplicationId = Objects.requireNonNullElse(
        System.getenv("KAFKA_APPLICATION_ID"), "titanccp-aggregation-test-0.0.1");
    final int kafkaCommitInterval = Integer
        .parseInt(Objects.requireNonNullElse(System.getenv("KAFKA_COMMIT_INTERVAL"), "1000"));
    final String kafkaInputTopic =
        Objects.requireNonNullElse(System.getenv("KAFKA_INPUT_TOPIC"), "input");
    final String redisHost = Objects.requireNonNullElse(System.getenv("REDIS_HOST"), "localhost");
    final int redisPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("REDIS_PORT"), "6379"));
    final Jedis jedis = new Jedis(redisHost, redisPort);

    final Properties settings = new Properties();
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaApplicationId);
    settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kafkaCommitInterval);
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

    final ClusterSession clusterSession = new SessionBuilder().contactPoint(cassandraHost)
        .port(cassandraPort).keyspace(cassandraKeyspace).build();
    final CassandraWriter cassandraWriter =
        buildCassandraWriter(clusterSession.getSession(), ActivePowerRecord.class);

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, ActivePowerRecord> input = builder.stream(kafkaInputTopic, Consumed
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // input.foreach((k, v) -> System.out.println(k + ": " + v));

    input.foreach((key, record) -> {
      // System.out.println("Write record: " + v);
      jedis.incrBy(REDIS_OUTPUT_COUNTER_KEY, 1);
      cassandraWriter.write(record);
    });

    /*
     * final KStream<String, Long> sensorCounts = input .groupBy((k, v) -> k,
     * Serialized.with(Serdes.String(), IMonitoringRecordSerde.serde(new
     * ActivePowerRecordFactory()))) .aggregate(() -> 0L, (ak, n, av) -> av + 1,
     * Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-test-1")
     * .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long())) .toStream();
     *
     * sensorCounts.foreach((k, v) -> System.out.println(k + ": " + v));
     *
     * sensorCounts.groupBy((k, v) -> "all", Serialized.with(Serdes.String(), Serdes.Long()))
     * .aggregate(() -> 0L, (ak, n, av) -> av + n, Materialized.<String, Long, KeyValueStore<Bytes,
     * byte[]>>as("aggregated-stream-store-test-2")
     * .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long())) .toStream().foreach((k, v) ->
     * System.out.println(k + ": " + v));
     */
    // .aggregate(v -> 0, (aggKey, newValue, aggValue) -> aggValue + 1, //
    // Materialized.as("aggregated-stream-store"));

    final Topology topology = builder.build();
    final TopologyDescription topologyDescription = topology.describe();
    System.out.println(topologyDescription); // NOPMD test
    final KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(settings));
    streams.start();
  }


  private static CassandraWriter buildCassandraWriter(final Session session,
      final Class<? extends IMonitoringRecord> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    return CassandraWriter.builder(session).excludeRecordType().excludeLoggingTimestamp()
        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
        .primaryKeySelectionStrategy(primaryKeySelectionStrategy).async().build();
  }


}
