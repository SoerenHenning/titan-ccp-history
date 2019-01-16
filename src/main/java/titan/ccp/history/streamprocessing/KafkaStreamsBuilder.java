package titan.ccp.history.streamprocessing; // NOPMD

import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.KiekerDataAdapter;
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
    return new KafkaStreams(this.buildTopology(), this.buildProperties());
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    LOGGER.info("Listening for records at topic '{}'", this.inputTopic);

    final KStream<String, ActivePowerRecord> inputStream = builder.stream(this.inputTopic, Consumed
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    inputStream.foreach((k, v) -> LOGGER.debug("Received record {}.", v));

    final KStream<String, ActivePowerRecord> flatMapped =
        inputStream.flatMap((key, value) -> this.flatMap(value));

    final KGroupedStream<String, ActivePowerRecord> groupedStream = flatMapped.groupByKey(Grouped
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(
        () -> new AggregationHistory(this.sensorRegistry), (aggKey, newValue, aggValue) -> {
          aggValue.update(newValue);
          LOGGER.debug("update history {}", aggValue);
          return aggValue;
        },
        Materialized
            .<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
            .withKeySerde(Serdes.String())
            .withValueSerde(AggregationHistorySerde.serde(this.sensorRegistry)));

    aggregated.toStream().map((key, value) -> KeyValue.pair(key, value.toRecord(key)))
        .to(this.outputTopic, Produced.with(Serdes.String(),
            IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));

    // Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriter =
        this.buildCassandraWriter(AggregatedActivePowerRecord.class);
    builder
        .stream(this.outputTopic,
            Consumed.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .foreach((key, record) -> {
          LOGGER.debug("write to cassandra {}", record); // NOCS
          cassandraWriter.write(record);
        });

    // Cassandra Writer for ActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);
    inputStream.foreach((key, record) -> {
      LOGGER.debug("write to cassandra {}", record); // NOCS
      cassandraWriterForNormal.write(record);
    });

    return builder.build();
  }

  private CassandraWriter<IMonitoringRecord> buildCassandraWriter(
      final Class<? extends IMonitoringRecord> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    final CassandraWriter<IMonitoringRecord> cassandraWriter =
        CassandraWriter.builder(this.cassandraSession, new KiekerDataAdapter())
            .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
            .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

  private Iterable<KeyValue<String, ActivePowerRecord>> flatMap(final ActivePowerRecord record) {
    LOGGER.debug("Flat map record: {}", record); // TODO Temporary
    final List<KeyValue<String, ActivePowerRecord>> result =
        this.sensorRegistry.getSensorForIdentifier(record.getIdentifier()).stream()
            .flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier())
            .map(i -> KeyValue.pair(i, record)).collect(Collectors.toList());
    LOGGER.debug("Flat map result: {}", result); // TODO Temporary
    return result;
  }

  private Properties buildProperties() {
    final Properties properties = new Properties();
    // TODO as parameter
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // TODO as parameter
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS); // TODO as param.
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    return properties;
  }

}
