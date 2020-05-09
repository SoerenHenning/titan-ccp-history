package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import com.google.common.math.Stats;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.history.streamprocessing.util.StatsFactory;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.records.WindowedActivePowerRecord;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  // For the Cassandra writers
  private static final String IDENTIFIER_PARTITION_KEY = "identifier";

  private final Serdes serdes;
  private final String inputTopic;
  private final String outputTopic;
  private final List<TimeWindowsConfiguration> timeWindowsConfigurations;
  private final Session cassandraSession;

  private final StreamsBuilder builder = new StreamsBuilder();


  /**
   * Create a new {@link TopologyBuilder} using the given topics.
   */
  public TopologyBuilder(final Serdes serdes, final String inputTopic, final String outputTopic,
      final List<TimeWindowsConfiguration> timeWindowsConfigurations,
      final Session cassandraSession) {
    this.serdes = serdes;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.timeWindowsConfigurations = timeWindowsConfigurations;
    this.cassandraSession = cassandraSession;
  }

  /**
   * Build the {@link Topology} for the History microservice.
   */
  public Topology build() {

    // 1. Cassandra Writer for ActivePowerRecord
    final CassandraWriter<SpecificRecord> cassandraWriterForNormal =
        this.buildCassandraWriter(ActivePowerRecord.class);

    // 2. Build Input Stream
    final KStream<String, ActivePowerRecord> inputStream = this.buildInputStream();

    // 3. Write the ActivePowerRecords from Input Stream to Cassandra
    this.writeActivePowerRecordsToCassandra(cassandraWriterForNormal, inputStream);

    // 4. Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter<SpecificRecord> cassandraWriter =
        this.buildCassandraWriter(AggregatedActivePowerRecord.class);

    // 5. Build Aggregation Stream
    final KStream<String, AggregatedActivePowerRecord> aggregationStream =
        this.buildAggregationStream();

    // 6. Write the AggregatedActivePowerRecords from Input Stream to Cassandra
    this.writeAggregatedActivePowerRecordsToCassandra(cassandraWriter, aggregationStream);

    // 7. Build combined power stream
    final KStream<String, ActivePowerRecord> combinedActivePowerStream =
        this.buildRecordStream(inputStream, aggregationStream);

    // 8. Add the tumbling windows
    for (final TimeWindowsConfiguration twc : this.timeWindowsConfigurations) {
      this.addTumblingWindow(twc, combinedActivePowerStream);
    }

    return this.builder.build();
  }

  private <T extends SpecificRecord> CassandraWriter<SpecificRecord> buildCassandraWriter(
      final Class<T> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(
        recordClass.getSimpleName(),
        IDENTIFIER_PARTITION_KEY);
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    final CassandraWriter<SpecificRecord> cassandraWriter = CassandraWriter
        .builder(this.cassandraSession, new AvroDataAdapter())
        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
        .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

  private KStream<String, ActivePowerRecord> buildInputStream() {
    return this.builder
        .stream(
            this.inputTopic,
            Consumed.with(
                this.serdes.string(),
                this.serdes.activePowerRecordValues()));
  }

  private void writeActivePowerRecordsToCassandra(
      final CassandraWriter<SpecificRecord> cassandraWriterForNormal,
      final KStream<String, ActivePowerRecord> inputStream) {
    inputStream
        // TODO Logging
        .peek((k, record) -> LOGGER.info("Write ActivePowerRecord to Cassandra {}",
            this.buildActivePowerRecordString(record)))
        .foreach((key, record) -> cassandraWriterForNormal.write(record));
  }

  private KStream<String, AggregatedActivePowerRecord> buildAggregationStream() {
    return this.builder
        .stream(
            this.outputTopic,
            Consumed.with(
                this.serdes.string(),
                this.serdes.aggregatedActivePowerRecordValues()));
  }

  private void writeAggregatedActivePowerRecordsToCassandra(
      final CassandraWriter<SpecificRecord> cassandraWriter,
      final KStream<String, AggregatedActivePowerRecord> aggregationStream) {
    aggregationStream
        // TODO Logging
        .peek((k, record) -> LOGGER.info("Write AggregatedActivePowerRecord to Cassandra {}",
            this.buildAggActivePowerRecordString(record)))
        .foreach((key, record) -> cassandraWriter.write(record));
  }

  // TODO Temp
  private String buildActivePowerRecordString(final ActivePowerRecord record) {
    return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getValueInW()
        + '}';
  }

  // TODO Temp
  private String buildAggActivePowerRecordString(final AggregatedActivePowerRecord record) {
    return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getSumInW()
        + '}';
  }

  private KStream<String, ActivePowerRecord> buildRecordStream(
      final KStream<String, ActivePowerRecord> activePowerStream,
      final KStream<String, AggregatedActivePowerRecord> aggrActivePowerStream) {
    final KStream<String, ActivePowerRecord> activePowerStreamAggr = aggrActivePowerStream
        .mapValues(
            aggrAvro -> new ActivePowerRecord(
                aggrAvro.getIdentifier(),
                aggrAvro.getTimestamp(),
                aggrAvro.getSumInW()));

    return activePowerStream.merge(activePowerStreamAggr);
  }

  /**
   * Adds a tumbling window aggregation to given stream and writes result back to Kafka and
   * Cassandra.
   *
   * @param timeWindowsConfiguration The configuration for the tumbling window.
   * @param combinedActivePowerStream The stream to perform the aggregation on.
   */
  private void addTumblingWindow(final TimeWindowsConfiguration timeWindowsConfiguration,
      final KStream<String, ActivePowerRecord> combinedActivePowerStream) {

    // Create a cassandra writer for this tumbling Window
    final CassandraWriter<SpecificRecord> windowedCassandraWriter =
        this.buildWindowedCassandraWriter(timeWindowsConfiguration.getCassandraTableName());

    // Create tumbling window stream with the aggregations
    final KStream<String, WindowedActivePowerRecord> windowedStream =
        this.buildWindowedStream(combinedActivePowerStream,
            timeWindowsConfiguration.getTimeWindows());

    // Write tumbling window to kafka and Cassandra
    this.exposeTumblingWindow(timeWindowsConfiguration.getKafkaTopic(), windowedStream,
        windowedCassandraWriter);
  }

  private <T extends SpecificRecord> CassandraWriter<SpecificRecord> buildWindowedCassandraWriter(
      final String tableName) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();

    primaryKeySelectionStrategy
        .registerPartitionKeys(tableName, IDENTIFIER_PARTITION_KEY);
    primaryKeySelectionStrategy.registerClusteringColumns(tableName,
        "startTimestamp");

    final CassandraWriter<SpecificRecord> cassandraWriter = CassandraWriter
        .builder(this.cassandraSession, new AvroDataAdapter())
        .tableNameMapper(c -> tableName)
        .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

  private KStream<String, WindowedActivePowerRecord> buildWindowedStream(
      final KStream<String, ActivePowerRecord> combinedActivePowerStream,
      final TimeWindows timeWindows) {
    return combinedActivePowerStream
        .groupByKey(Grouped.with(this.serdes.string(), this.serdes.activePowerRecordValues()))
        .windowedBy(timeWindows)
        .aggregate(
            () -> Stats.of(),
            (k, record, stats) -> StatsFactory.accumulate(stats, record.getValueInW()),
            Materialized.with(this.serdes.string(), this.serdes.stats()))
        .toStream()
        .map((windowedKey, stats) -> KeyValue.pair(
            windowedKey.key(),
            WindowedActivePowerRecordFactory.create(windowedKey, stats)));
  }

  private void exposeTumblingWindow(final String topic,
      final KStream<String, WindowedActivePowerRecord> windowedStream,
      final CassandraWriter<SpecificRecord> cassandraWriter) {
    windowedStream.to(
        topic,
        Produced.with(
            this.serdes.string(),
            this.serdes.windowedActivePowerValues()));

    windowedStream.foreach((k, record) -> cassandraWriter.write(record));
  }
}
