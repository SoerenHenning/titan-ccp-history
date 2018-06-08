package titan.ccp.aggregation;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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

import com.datastax.driver.core.Session;

import titan.ccp.common.kieker.cassandra.CassandraWriter;
import titan.ccp.common.kieker.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.kieker.cassandra.PredefinedTableNameMappers;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.PowerConsumptionRecord;
import titan.ccp.models.records.serialization.kafka.RecordSerdes;

public class KafkaStreamsBuilder {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

	private String bootstrapServers;
	private String inputTopic;
	private String outputTopic;
	private final String aggregationStoreName = "stream-store"; // TODO

	private SensorRegistry sensorRegistry;
	private Session cassandraSession;

	public KafkaStreamsBuilder() {
	}

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

		final KStream<String, PowerConsumptionRecord> inputStream = builder.stream(this.inputTopic,
				Consumed.with(Serdes.String(), RecordSerdes.forPowerConsumptionRecord()));
		// final KStream<String, ActivePowerRecord> inputStream2 =
		// builder.stream(this.inputTopic,
		// Consumed.with(Serdes.String(), IMonitoringRecordSerde.serde(new
		// ActivePowerRecordFactory())));

		inputStream.foreach((k, v) -> LOGGER.info("received record {}", v)); // TODO Temporary;

		final KStream<String, PowerConsumptionRecord> flatMapped = inputStream
				.flatMap((key, value) -> this.flatMap(value));

		final KGroupedStream<String, PowerConsumptionRecord> groupedStream = flatMapped
				.groupByKey(Serialized.with(Serdes.String(), RecordSerdes.forPowerConsumptionRecord()));

		final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(() -> {
			return new AggregationHistory();
		}, (aggKey, newValue, aggValue2) -> {
			// System.out.println(".");
			// System.out.println("__");
			// System.out.println("O: " + aggKey + ": " + aggValue2.getLastValues());
			// System.out.println("O: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			// System.out.println("new: " + newValue.getIdentifier() + ": " +
			// newValue.getPowerConsumptionInWh());
			// System.out.println("New record in aggregation."); // TODO
			aggValue2.update(newValue);
			// System.out.println("N: " + aggKey + ": " + aggValue2.getLastValues());
			// System.out.println("N: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			// System.out.println("P: " + aggValue2.getTimestamp());
			LOGGER.info("update history {}", aggValue2); // TODO
			return aggValue2;
			// return aggValue2.update(newValue);
		}, Materialized.<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
				.withKeySerde(Serdes.String()).withValueSerde(AggregationHistorySerde.serde()));

		// aggregated.toStream().foreach((key, value) -> {
		// System.out.println("A: " + value.getTimestamp());
		// System.out.println("A: " + key + ": " + value.getSummaryStatistics());
		// }); // TODO

		aggregated.toStream().map((key, value) -> KeyValue.pair(key, value.toRecord(key))).to(this.outputTopic,
				Produced.with(Serdes.String(), RecordSerdes.forAggregatedPowerConsumptionRecord()));

		// Cassandra Writer
		final CassandraWriter cassandraWriter = this.buildCassandraWriter();
		builder.stream(this.outputTopic,
				Consumed.with(Serdes.String(), RecordSerdes.forAggregatedPowerConsumptionRecord()))
				.foreach((key, record) -> {
					LOGGER.info("write to cassandra {}", record);
					cassandraWriter.write(record);
					// System.out.println("New written"); // TODO
				});
		// End Cassandra Writer

		// Cassandra Writer for PowerConsumptionRecords
		final CassandraWriter cassandraWriterForNormal = this.buildCassandraWriterForNormal();
		inputStream.foreach((key, record) -> {
			cassandraWriterForNormal.write(record);
			// System.out.println("New written"); // TODO
		});
		// End Cassandra Writer

		return builder.build();
	}

	private CassandraWriter buildCassandraWriter() {
		final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy = new ExplicitPrimaryKeySelectionStrategy();
		// TODO change to ActivePower
		primaryKeySelectionStrategy.registerPartitionKeys(AggregatedPowerConsumptionRecord.class.getSimpleName(),
				"identifier");
		// TODO change to ActivePower
		primaryKeySelectionStrategy.registerClusteringColumns(AggregatedPowerConsumptionRecord.class.getSimpleName(),
				"timestamp");

		final CassandraWriter cassandraWriter = CassandraWriter.builder(this.cassandraSession).excludeRecordType()
				.excludeLoggingTimestamp().tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
				.primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

		return cassandraWriter;
	}

	// BETTER refine name
	private CassandraWriter buildCassandraWriterForNormal() {
		final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy = new ExplicitPrimaryKeySelectionStrategy();
		// TODO change to ActivePower
		primaryKeySelectionStrategy.registerPartitionKeys(PowerConsumptionRecord.class.getSimpleName(), "identifier");
		// TODO change to ActivePower
		primaryKeySelectionStrategy.registerClusteringColumns(PowerConsumptionRecord.class.getSimpleName(),
				"timestamp");

		final CassandraWriter cassandraWriter = CassandraWriter.builder(this.cassandraSession).excludeRecordType()
				.excludeLoggingTimestamp().tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
				.primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

		return cassandraWriter;
	}

	private StreamsConfig buildStreamConfig() {
		final Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "titanccp-aggregation-0.0.6"); // TODO as parameter
		settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // TODO as parameter
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		return new StreamsConfig(settings);
	}

	// TODO change to ActivePower
	private Iterable<KeyValue<String, PowerConsumptionRecord>> flatMap(final PowerConsumptionRecord record) {
		LOGGER.info("Flat map record: {}", record); // TODO Temporary
		final List<KeyValue<String, PowerConsumptionRecord>> result = this.sensorRegistry
				.getSensorForIdentifier(record.getIdentifier()).stream().flatMap(s -> s.getParents().stream())
				.map(s -> s.getIdentifier()).map(i -> KeyValue.pair(i, record)).collect(Collectors.toList());
		LOGGER.info("Flat map result: {}", result); // TODO Temporary
		return result; // TODO Temporary
	}

}
