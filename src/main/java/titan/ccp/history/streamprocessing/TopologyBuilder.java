package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import java.util.Set;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
 * Builds Kafka Stream Topology for the History microservice.
 *
 * <p>
 * The History microservice is going to be divided into a History and an Aggregation component
 * (later microservice). After the restructuring, this class constructs the topology for the
 * Aggregation whereas the Cassandra storing parts are going to be moved to a dedicated builder.
 * </p>
 */
public class TopologyBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private final String inputTopic;
  private final String outputTopic;
  private final String configurationTopic;
  private final Session cassandraSession; // TODO Remove History parts

  private final StreamsBuilder builder = new StreamsBuilder();
  private final RecordAggregator recordAggregator = new RecordAggregator();


  /**
   * Create a new {@link TopologyBuilder} using the given topics.
   */
  public TopologyBuilder(final String inputTopic, final String outputTopic,
      final String configurationTopic, final Session cassandraSession) {
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.configurationTopic = configurationTopic;
    this.cassandraSession = cassandraSession;
  }

  /**
   * Build the {@link Topology} for the History microservice.
   */
  public Topology build() {
    // 1. Build Parent-Sensor Table
    final KTable<String, Set<String>> parentSensorTable = this.buildParentSensorTable();

    // 2. Build Input Table
    final KTable<String, ActivePowerRecord> inputTable = this.buildInputTable();

    // 3. Build Last Value Table from Input and Parent-Sensor Table
    final KTable<SensorParentKey, ActivePowerRecord> lastValueTable =
        this.buildLastValueTable(parentSensorTable, inputTable);

    // 4. Build Aggregations Stream
    final KStream<String, AggregatedActivePowerRecord> aggregations =
        this.buildAggregationStream(lastValueTable);

    // 5. Expose Aggregations Stream
    this.exposeOutputStream(aggregations);

    // Logging steps
    // inputTable.toStream().foreach(
    // (k, v) -> System.out.println("INPUT: " + k + ':' + this.buildActivePowerRecordString(v)));
    // lastValueTable
    // .toStream()
    // .foreach(
    // (k, v) -> System.out.println("LVT: " + k + ':' + this.buildActivePowerRecordString(v)));
    // aggregations.foreach((k, v) -> System.out.println("AGG: " + k + ';' + v.getSumInW()));

    // This is History, not Aggregation
    // Write records to Cassandra
    this.buildCassandraWriters(inputTable);

    return this.builder.build();
  }

  private KTable<String, ActivePowerRecord> buildInputTable() {
    return this.builder.table(this.inputTopic, Consumed.with(Serdes.String(),
        IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));
  }

  private KTable<String, Set<String>> buildParentSensorTable() {
    final KStream<Event, String> configurationStream = this.builder
        .stream(this.configurationTopic, Consumed.with(EventSerde.serde(), Serdes.String()))
        .filter((key, value) -> key == Event.SENSOR_REGISTRY_CHANGED
            || key == Event.SENSOR_REGISTRY_STATUS)
        .peek((k, v) -> System.out.println("!!!!!-----------!!!!!! " + k + v));


    final ChildParentsTransformerFactory childParentsTransformerFactory =
        new ChildParentsTransformerFactory();
    this.builder.addStateStore(childParentsTransformerFactory.getStoreBuilder());

    return configurationStream.mapValues(data -> SensorRegistry.fromJson(data))
        .flatTransform(childParentsTransformerFactory.getTransformerSupplier(),
            childParentsTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(Serdes.String(), OptionalParentsSerde.serde()))
        .aggregate(() -> Set.<String>of(), (key, newValue, oldValue) -> newValue.orElse(null),
            Materialized.with(Serdes.String(), ParentsSerde.serde()));
  }


  private KTable<SensorParentKey, ActivePowerRecord> buildLastValueTable(
      final KTable<String, Set<String>> parentSensorTable,
      final KTable<String, ActivePowerRecord> inputTable) {
    final JointFlatTransformerFactory jointFlatMapTransformerFactory =
        new JointFlatTransformerFactory();
    this.builder.addStateStore(jointFlatMapTransformerFactory.getStoreBuilder());

    return inputTable
        .join(parentSensorTable, (record, parents) -> new JointRecordParents(parents, record))
        .toStream()
        .flatTransform(jointFlatMapTransformerFactory.getTransformerSupplier(),
            jointFlatMapTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(SensorParentKeySerde.serde(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .reduce(
            // TODO Also deduplicate here?
            (aggValue, newValue) -> newValue, Materialized.with(SensorParentKeySerde.serde(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));
  }

  private KStream<String, AggregatedActivePowerRecord> buildAggregationStream(
      final KTable<SensorParentKey, ActivePowerRecord> lastValueTable) {
    return lastValueTable
        .groupBy((k, v) -> KeyValue.pair(k.getParent(), v),
            Grouped.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .aggregate(() -> null, this.recordAggregator::add, this.recordAggregator::substract,
            Materialized.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .toStream()
        // TODO TODO timestamp -1 indicates that this record is emitted by an substract event
        .filter((k, record) -> record.getTimestamp() != -1);
  }

  private void exposeOutputStream(final KStream<String, AggregatedActivePowerRecord> aggregations) {
    aggregations.to(this.outputTopic, Produced.with(Serdes.String(),
        IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));
  }

  // TODO this part is going to be outsourced
  private void buildCassandraWriters(final KTable<String, ActivePowerRecord> inputTable) {

    // Cassandra Writer for ActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);
    inputTable.toStream()
        // TODO Logging
        .peek((k, record) -> LOGGER.info("Write ActivePowerRecord to Cassandra {}", record))
        .foreach((key, record) -> cassandraWriterForNormal.write(record));

    // Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter<IMonitoringRecord> cassandraWriter =
        this.buildCassandraWriter(AggregatedActivePowerRecord.class);
    this.builder
        .stream(this.outputTopic,
            Consumed.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .peek(
            (k, record) -> LOGGER.info("Write AggregatedActivePowerRecord to Cassandra {}", record))
        .foreach((key, record) -> cassandraWriter.write(record));
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
  // private String buildActivePowerRecordString(final ActivePowerRecord record) {
  // return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getValueInW()
  // + '}';
  // }

}
