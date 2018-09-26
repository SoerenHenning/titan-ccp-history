package titan.ccp.aggregation.streamprocessing; // NOPMD

import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Properties;
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
public class KafkaStreamsBuilder {

  private static final String APPLICATION_ID = "titanccp-aggregation-0.0.7";

  private static final int COMMIT_INTERVAL_MS = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private final String aggregationStoreName = "stream-store"; // NOPMD

  private SensorRegistry sensorRegistry; // NOPMD
  private Session cassandraSession; // NOPMD

  public KafkaStreamsBuilder sensorRegistry(final SensorRegistry sensorRegistry) {
    this.sensorRegistry = sensorRegistry;
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

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
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
    final CassandraWriter cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);
    inputStream.foreach((key, record) -> {
      LOGGER.info("write to cassandra {}", record); // NOCS
      cassandraWriterForNormal.write(record);
    });

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
