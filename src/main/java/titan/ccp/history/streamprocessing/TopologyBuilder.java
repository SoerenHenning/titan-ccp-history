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
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.history.streamprocessing.util.StatsFactory;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.records.WindowedActivePowerRecord;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private final Serdes serdes;
  private final String inputTopic;
  private final String outputTopic;
  private final List<TimeWindowsConfiguration> timeWindowsConfigurations;
  private final CassandraWriterFactory writerFactory;

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
    this.writerFactory = new CassandraWriterFactory(cassandraSession);
  }

  /**
   * Build the {@link Topology} for the History microservice.
   */
  public Topology build() {
    // 1. Build Input Stream
    final KStream<String, ActivePowerRecord> inputStream = this.buildInputStream();

    // 2. Write the ActivePowerRecords from Input Stream to Cassandra
    this.writeActivePowerRecordsToCassandra(inputStream);

    // 3. Build Aggregation Stream
    final KStream<String, AggregatedActivePowerRecord> aggregationStream =
        this.buildAggregationStream();

    // 4. Write the AggregatedActivePowerRecords from Input Stream to Cassandra
    this.writeAggregatedActivePowerRecordsToCassandra(aggregationStream);

    // 5. Build combined power stream
    final KStream<String, ActivePowerRecord> combinedActivePowerStream =
        this.buildRecordStream(inputStream, aggregationStream);

    // 6. Add the tumbling windows
    for (final TimeWindowsConfiguration twc : this.timeWindowsConfigurations) {
      this.addTumblingWindow(twc, combinedActivePowerStream);
    }

    return this.builder.build();
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
      final KStream<String, ActivePowerRecord> inputStream) {
    // Cassandra Writer for ActivePowerRecord
    final CassandraWriter<SpecificRecord> cassandraWriter =
        this.writerFactory.buildUnwindowed(ActivePowerRecord.class);

    inputStream
        // TODO Logging
        .peek((k, rec) -> LOGGER.info("Write ActivePowerRecord to Cassandra {}", rec))
        .foreach((key, record) -> cassandraWriter.write(record));
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
      final KStream<String, AggregatedActivePowerRecord> aggregationStream) {
    // Cassandra Writer for AggregatedActivePowerRecord
    final CassandraWriter<SpecificRecord> cassandraWriter =
        this.writerFactory.buildUnwindowed(AggregatedActivePowerRecord.class);

    aggregationStream
        // TODO Logging
        .peek((k, rec) -> LOGGER.info("Write AggregatedActivePowerRecord to Cassandra {}", rec))
        .foreach((key, record) -> cassandraWriter.write(record));
  }

  private KStream<String, ActivePowerRecord> buildRecordStream(
      final KStream<String, ActivePowerRecord> activePowerStream,
      final KStream<String, AggregatedActivePowerRecord> aggrActivePowerStream) {
    final KStream<String, ActivePowerRecord> activePowerStreamAggr = aggrActivePowerStream
        .mapValues(
            aggr -> new ActivePowerRecord(
                aggr.getIdentifier(),
                aggr.getTimestamp(),
                aggr.getSumInW()));

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
        this.writerFactory
            .buildWindowed(timeWindowsConfiguration.getCassandraTableName(),
                timeWindowsConfiguration.getTtl());

    // Create tumbling window stream with the aggregations
    final KStream<String, WindowedActivePowerRecord> windowedStream =
        this.buildWindowedStream(combinedActivePowerStream,
            timeWindowsConfiguration.getTimeWindows());

    // Write tumbling window to kafka and Cassandra
    this.exposeTumblingWindow(timeWindowsConfiguration.getKafkaTopic(), windowedStream,
        windowedCassandraWriter);
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
