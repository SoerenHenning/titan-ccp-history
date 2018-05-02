package titan.ccp.aggregation;

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

import com.datastax.driver.core.Session;

import titan.ccp.common.kieker.cassandra.CassandraWriter;
import titan.ccp.common.kieker.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.kieker.cassandra.PredefinedTableNameMappers;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.PowerConsumptionRecord;
import titan.ccp.models.records.serialization.kafka.RecordSerdes;

public class KafkaStreamsBuilder {

	private final String inputTopicName = "input"; // TODO
	private final String outputTopicName = "output"; // TODO
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

	public KafkaStreams build() {
		return new KafkaStreams(this.buildTopology(), this.buildStreamConfig());
	}

	private Topology buildTopology() {
		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, PowerConsumptionRecord> inputStream = builder.stream(this.inputTopicName,
				Consumed.with(Serdes.String(), RecordSerdes.forPowerConsumptionRecord()));

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
			aggValue2.update(newValue);
			// System.out.println("N: " + aggKey + ": " + aggValue2.getLastValues());
			// System.out.println("N: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			// System.out.println("P: " + aggValue2.getTimestamp());
			return aggValue2;
			// return aggValue2.update(newValue);
		}, Materialized.<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
				.withKeySerde(Serdes.String()).withValueSerde(AggregationHistorySerde.create()));

		// aggregated.toStream().foreach((key, value) -> {
		// System.out.println("A: " + value.getTimestamp());
		// System.out.println("A: " + key + ": " + value.getSummaryStatistics());
		// }); // TODO

		aggregated.toStream().map((key, value) -> KeyValue.pair(key, value.toRecord(key))).to(this.outputTopicName,
				Produced.with(Serdes.String(), RecordSerdes.forAggregatedPowerConsumptionRecord()));

		// Cassandra Writer
		final CassandraWriter cassandraWriter = this.buildCassandraWriter();

		builder.stream(this.outputTopicName,
				Consumed.with(Serdes.String(), RecordSerdes.forAggregatedPowerConsumptionRecord()))
				.foreach((key, record) -> cassandraWriter.write(record));

		// End Cassandra Writer

		return builder.build();
	}

	private CassandraWriter buildCassandraWriter() {
		final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy = new ExplicitPrimaryKeySelectionStrategy();
		primaryKeySelectionStrategy.registerPartitionKeys(AggregatedPowerConsumptionRecord.class.getSimpleName(),
				"identifier");
		primaryKeySelectionStrategy.registerClusteringColumns(AggregatedPowerConsumptionRecord.class.getSimpleName(),
				"timestamp");

		final CassandraWriter cassandraWriter = CassandraWriter.builder(this.cassandraSession).excludeRecordType()
				.excludeLoggingTimestamp().tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
				.primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

		return cassandraWriter;
	}

	private StreamsConfig buildStreamConfig() {
		final Properties settings = loadProperties();
		return new StreamsConfig(settings);
	}

	private Iterable<KeyValue<String, PowerConsumptionRecord>> flatMap(final PowerConsumptionRecord record) {
		return this.sensorRegistry.getSensorForIdentifier(record.getIdentifier()).stream()
				.flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier()).map(i -> KeyValue.pair(i, record))
				.collect(Collectors.toList());
	}

	private static Properties loadProperties() {
		// Cloudkarafka configuration
		final String kafkaBootstrapServer = System.getenv("CLOUDKARAFKA_BROKERS");
		final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		final String password = System.getenv("CLOUDKARAFKA_PASSWORD");
		final Properties kafkaProperties = new Properties();
		// final String jaasTemplate =
		// "org.apache.kafka.common.security.scram.ScramLoginModule required
		// username=\"%s\" password=\"%s\";";
		// final String jaasCfg = String.format(jaasTemplate, username, password);
		// kafkaProperties.put("security.protocol", "SASL_SSL");
		// kafkaProperties.put("sasl.mechanism", "SCRAM-SHA-256");
		// kafkaProperties.put("sasl.jaas.config", jaasCfg);

		kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-0.0.5");
		kafkaProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // TODO
		// kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
		// kafkaBootstrapServer);
		kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		return kafkaProperties;
	}

	private static String loadInputTopicName() {
		return "input";
		// final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		// return username + "-default";
	}

	private static String loadOutputTopicName() {
		return "output";
		// final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		// return username + "-aggregated";
	}

	private static String loadAggregatedStreamStoreTopicName() {
		return "stream-store";
		// final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		// return username + "-stream-store";
	}

}
