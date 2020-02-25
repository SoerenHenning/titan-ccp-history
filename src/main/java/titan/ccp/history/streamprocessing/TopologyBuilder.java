package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private final Serdes serdes;
  private final String inputTopic;
  private final String outputTopic;
  private final Session cassandraSession;

  private final StreamsBuilder builder = new StreamsBuilder();


  /**
   * Create a new {@link TopologyBuilder} using the given topics.
   */
  public TopologyBuilder(final Serdes serdes, final String inputTopic, final String outputTopic,
      final Session cassandraSession) {
    this.serdes = serdes;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
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

    // 5. Build Output Stream
    final KStream<String, AggregatedActivePowerRecord> outputStream = this.buildOutputStream();

    // 6. Write the AggregatedActivePowerRecords from Input Stream to Cassandra
    this.writeAggregatedActivePowerRecordsToCassandra(cassandraWriter, outputStream);

    return this.builder.build();
  }

  private <T extends SpecificRecord> CassandraWriter<SpecificRecord> buildCassandraWriter(
      final Class<T> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
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

  private KStream<String, AggregatedActivePowerRecord> buildOutputStream() {
    return this.builder
        .stream(
            this.outputTopic,
            Consumed.with(
                this.serdes.string(),
                this.serdes.aggregatedActivePowerRecordValues()));
  }

  private void writeAggregatedActivePowerRecordsToCassandra(
      final CassandraWriter<SpecificRecord> cassandraWriter,
      final KStream<String, AggregatedActivePowerRecord> outputStream) {
    outputStream
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

}
