package titan.ccp.aggregation.experimental; // NOPMD

import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import titan.ccp.aggregation.streamprocessing.AggregationHistory;
import titan.ccp.aggregation.streamprocessing.AggregationHistorySerde;
import titan.ccp.common.kieker.cassandra.CassandraWriter;
import titan.ccp.common.kieker.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.kieker.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * Builder for the Kafka Streams configuration.
 */
public class TestKafkaStreamsBuilder {

  // Start debugging
  private static final int TERMINATION_TIMEOUT_SECONDS = 10;
  private static final String REDIS_OUTPUT_COUNTER_KEY = "output_counter";
  // End debugging


  private static final String APPLICATION_ID = "titanccp-aggregation-0.0.7";

  private static final int COMMIT_INTERVAL_MS = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaStreamsBuilder.class);

  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private final String aggregationStoreName = "stream-store"; // NOPMD

  private SensorRegistry sensorRegistry; // NOPMD
  private Session cassandraSession; // NOPMD

  public TestKafkaStreamsBuilder sensorRegistry(final SensorRegistry sensorRegistry) {
    this.sensorRegistry = sensorRegistry;
    return this;
  }

  public TestKafkaStreamsBuilder cassandraSession(final Session cassandraSession) {
    this.cassandraSession = cassandraSession;
    return this;
  }

  public TestKafkaStreamsBuilder inputTopic(final String inputTopic) {
    this.inputTopic = inputTopic;
    return this;
  }

  public TestKafkaStreamsBuilder outputTopic(final String outputTopic) {
    this.outputTopic = outputTopic;
    return this;
  }

  public TestKafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public KafkaStreams build() {
    return new KafkaStreams(this.buildTopology(), this.buildStreamConfig());
  }

  private Topology buildTopology() {

    final StreamsBuilder builder = new StreamsBuilder();

    LOGGER.info("Listening for records at topic '{}'", this.inputTopic);

    final KStream<String, ActivePowerRecord> inputStream = builder.stream(this.inputTopic, Consumed
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    inputStream.foreach((k, v) -> LOGGER.info("Received record {}.", v)); // TODO Temporary

    final KStream<String, ActivePowerRecord> flatMapped =
        inputStream.flatMap((key, value) -> this.flatMap(value));

    final KGroupedStream<String, ActivePowerRecord> groupedStream = flatMapped.groupByKey(Serialized
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    final KTable<String, AggregationHistory> aggregated =
        groupedStream.aggregate(() -> new AggregationHistory(), (aggKey, newValue, aggValue2) -> {
          aggValue2.update(newValue);
          LOGGER.info("update history {}", aggValue2); // TODO
          return aggValue2;
        }, Materialized
            .<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
            .withKeySerde(Serdes.String()).withValueSerde(AggregationHistorySerde.serde()));

    aggregated.toStream().map((key, value) -> KeyValue.pair(key, value.toRecord(key)))
        .to(this.outputTopic, Produced.with(Serdes.String(),
            IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));

    // Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter cassandraWriter =
        this.buildCassandraWriter(AggregatedActivePowerRecord.class);
    builder
        .stream(this.outputTopic,
            Consumed.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .foreach((key, record) -> {
          LOGGER.info("write to cassandra {}", record); // NOCS
          cassandraWriter.write(record);
        });

    // Cassandra Writer for ActivePowerRecord
    final AtomicLong counter = new AtomicLong(0);
    final CassandraWriter cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);
    inputStream.foreach((key, record) -> {
      LOGGER.info("write to cassandra {}", record); // NOCS
      counter.incrementAndGet();
      cassandraWriterForNormal.write(record);
    });



    final String redisHost = Objects.requireNonNullElse(System.getenv("REDIS_HOST"), "localhost");
    final int redisPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("REDIS_PORT"), "6379"));
    final Jedis jedis = new Jedis(redisHost, redisPort);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {
      final long oldValue = counter.getAndSet(0);
      System.out.println(oldValue); // NOPMD
      jedis.incrBy(REDIS_OUTPUT_COUNTER_KEY, oldValue);
    }, 1, 1, TimeUnit.SECONDS);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      scheduler.shutdown();
      try {
        scheduler.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        throw new IllegalStateException(e);
      }
      final long oldValue = counter.getAndSet(0);
      System.out.println(oldValue); // NOPMD
      jedis.incrBy(REDIS_OUTPUT_COUNTER_KEY, oldValue);
      jedis.close();
    }));


    return builder.build();
  }

  private CassandraWriter buildCassandraWriter(
      final Class<? extends IMonitoringRecord> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    final CassandraWriter cassandraWriter =
        CassandraWriter.builder(this.cassandraSession).excludeRecordType().excludeLoggingTimestamp()
            .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
            .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

  private Iterable<KeyValue<String, ActivePowerRecord>> flatMap(final ActivePowerRecord record) {
    LOGGER.info("Flat map record: {}", record); // TODO Temporary
    final List<KeyValue<String, ActivePowerRecord>> result =
        this.sensorRegistry.getSensorForIdentifier(record.getIdentifier()).stream()
            .flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier())
            .map(i -> KeyValue.pair(i, record)).collect(Collectors.toList());
    LOGGER.info("Flat map result: {}", result); // TODO Temporary
    return result;
  }

  private StreamsConfig buildStreamConfig() {
    final Properties settings = new Properties();
    // TODO as parameter
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // TODO as parameter
    settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS); // TODO as parameter
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    return new StreamsConfig(settings);
  }

}
