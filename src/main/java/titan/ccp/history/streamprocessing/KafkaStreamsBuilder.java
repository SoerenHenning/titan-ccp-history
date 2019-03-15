package titan.ccp.history.streamprocessing; // NOPMD

import com.datastax.driver.core.Session;
import java.util.Properties;
import java.util.Set;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.KiekerDataAdapter;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * Builder for the Kafka Streams configuration.
 */
public class KafkaStreamsBuilder {

  private static final String APPLICATION_ID = "titanccp-aggregation-0.0.13";

  private static final int COMMIT_INTERVAL_MS = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private final RecordAggregator recordAggregator = new RecordAggregator();

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

  public KafkaStreams build() {
    return new KafkaStreams(this.buildTopology(), this.buildProperties());
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    LOGGER.info("Listening for records at topic '{}'", this.inputTopic);

    final KStream<Event, String> configurationStream =
        builder.stream(this.configurationTopic, Consumed.with(EventSerde.serde(), Serdes.String()))
            .filter((key, value) -> key == Event.SENSOR_REGISTRY_CHANGED);

    // TODO DEBUG
    configurationStream.foreach((k, v) -> System.out.println(k + ": " + v));


    // === Child Parents Transformer
    final ChildParentsTransformerFactory childParentsTransformerFactory =
        new ChildParentsTransformerFactory();

    // register store
    builder.addStateStore(childParentsTransformerFactory.getStoreBuilder());

    final KTable<String, Set<String>> childParentsTable = configurationStream
        .mapValues(data -> SensorRegistry.fromJson(data))
        .transform(
            childParentsTransformerFactory.getTransformerSupplier(),
            childParentsTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(Serdes.String(), OptionalParentsSerde.serde()))
        .aggregate(
            () -> Set.<String>of(),
            (key, newValue, oldValue) -> newValue.orElse(null),
            Materialized.with(Serdes.String(), ParentsSerde.serde()));

    childParentsTable.toStream()
        .foreach((k, v) -> System.out.println("CPT: " + k + ':' + v));


    // === Transformer
    final JointFlatTransformerFactory jointFlatMapTransformerFactory =
        new JointFlatTransformerFactory();

    // register store
    builder.addStateStore(jointFlatMapTransformerFactory.getStoreBuilder());

    final KTable<String, ActivePowerRecord> inputTable = builder.table(this.inputTopic, Consumed
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // TODO Debug only
    inputTable.toStream().foreach((k, v) -> System.out.println("INPUT: " + k + ':' + v));

    final KTable<SensorParentKey, ActivePowerRecord> lastValueTable = inputTable
        .join(childParentsTable, (record, parents) -> new JointRecordParents(parents, record))
        .toStream()
        .transform(
            jointFlatMapTransformerFactory.getTransformerSupplier(),
            jointFlatMapTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(
            SensorParentKeySerde.serde(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .reduce(
            // TODO Also deduplicate here?
            (aggValue, newValue) -> newValue,
            Materialized.with(
                SensorParentKeySerde.serde(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // TODO Debug only
    lastValueTable
        .toStream()
        .foreach(
            (k, v) -> System.out.println("LVT: " + k + ':' + this.buildActivePowerRecordString(v)));

    final KStream<String, AggregatedActivePowerRecord> aggregations = lastValueTable
        .groupBy(
            (k, v) -> KeyValue.pair(k.getParent(), v),
            Grouped.with(
                Serdes.String(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .aggregate(
            () -> null, this.recordAggregator::add, this.recordAggregator::substract,
            Materialized.with(
                Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .toStream();

    // TODO DEBUG only
    aggregations.foreach((k, v) -> System.out.println("AGG: " + k + ';' + v.getSumInW()));

    aggregations.to(this.outputTopic, Produced.with(Serdes.String(),
        IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));

    // ...

    // ==================

    // final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(
    // () -> new AggregationHistory(this.sensorRegistry), (aggKey, newValue, aggValue) -> {
    // aggValue.update(newValue);
    // LOGGER.debug("update history {}", aggValue);
    // return aggValue;
    // },
    // Materialized
    // .<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
    // .withKeySerde(Serdes.String())
    // .withValueSerde(AggregationHistorySerde.serde(this.sensorRegistry)));
    //
    // aggregated
    // .toStream()
    // .map((key, value) -> KeyValue.pair(key, value.toRecord(key)))
    // .to(this.outputTopic, Produced.with(Serdes.String(),
    // IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));

    // Cassandra writing

    // Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriter =
        this.buildCassandraWriter(AggregatedActivePowerRecord.class);
    builder
        .stream(this.outputTopic, Consumed.with(
            Serdes.String(),
            IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .foreach((key, record) -> {
          LOGGER.debug("write to cassandra {}", record); // NOCS
          cassandraWriter.write(record);
        });

    // Cassandra Writer for ActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);
    inputTable.toStream()
        .foreach((key, record) -> {
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

  // TODO Temp
  private String buildActivePowerRecordString(final ActivePowerRecord record) {
    return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getValueInW()
        + '}';
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
