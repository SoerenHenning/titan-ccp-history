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

import titan.ccp.model.sensorregistry.ExampleSensors;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.PowerConsumptionRecord;
import titan.ccp.models.records.serialization.kafka.RecordSerdes;

public class KafkaStreamsFactory {

	private static final String AGGREGATED_STREAM_STORE_TOPIC = "aggregated-stream-store-for-test-topic-18040319";
	private static final String INPUT_TOPIC = "test-topic-18040319";
	private static final String OUTPUT_TOPIC = "test-out-topic-18040319";

	public KafkaStreams create() {

		final String inputTopic = loadInputTopicName();
		final String aggregatedStreamStoreTopic = loadAggregatedStreamStoreTopicName(); // is not a topic
		final String outputTopic = loadOutputTopicName();

		final StreamsBuilder builder = new StreamsBuilder(); // when using the DSL

		final KStream<String, PowerConsumptionRecord> inputStream = builder.stream(inputTopic,
				Consumed.with(Serdes.String(), RecordSerdes.forPowerConsumptionRecord()));

		final KStream<String, PowerConsumptionRecord> flatMapped = inputStream
				.flatMap((key, value) -> this.flatMap(value));

		final KGroupedStream<String, PowerConsumptionRecord> groupedStream = flatMapped
				.groupByKey(Serialized.with(Serdes.String(), RecordSerdes.forPowerConsumptionRecord()));

		final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(() -> {
			return new AggregationHistory();
		}, (aggKey, newValue, aggValue2) -> {
			// System.out.println("__");
			// System.out.println("O: " + aggKey + ": " + aggValue2.getLastValues());
			// System.out.println("O: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			// System.out.println("new: " + newValue.getIdentifier() + ": " +
			// newValue.getPowerConsumptionInWh());
			// aggValue2.update(newValue);
			// System.out.println("N: " + aggKey + ": " + aggValue2.getLastValues());
			// System.out.println("N: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			// return aggValue2;
			return aggValue2.update(newValue);
		}, Materialized.<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(aggregatedStreamStoreTopic)
				.withKeySerde(Serdes.String()).withValueSerde(AggregationHistorySerde.create()));

		// aggregated.toStream().foreach((key, value) -> System.out.println(key + ": " +
		// value.getSummaryStatistics())); // TODO
		aggregated.toStream()
				.map((key, value) -> KeyValue.pair(key, value.toRecord(key)))
				.to(outputTopic, Produced.with(Serdes.String(), RecordSerdes.forAggregatedPowerConsumptionRecord()));

		final Topology topology = builder.build();

		final Properties settings = loadProperties();
		final StreamsConfig config = new StreamsConfig(settings);

		return new KafkaStreams(topology, config);
	}

	private Iterable<KeyValue<String, PowerConsumptionRecord>> flatMap(final PowerConsumptionRecord record) {
		final SensorRegistry sensorRegistry = ExampleSensors.registry(); // TODO

		return sensorRegistry
				.getSensorForIdentifier(record.getIdentifier())
				.stream()
				.flatMap(s -> s.getParents().stream())
				.map(s -> s.getIdentifier())
				.map(i -> KeyValue.pair(i, record))
				.collect(Collectors.toList());
	}

	private static Properties loadProperties() {
		// Cloudkarafka configuration
		final String kafkaBootstrapServer = System.getenv("CLOUDKARAFKA_BROKERS");
		final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		final String password = System.getenv("CLOUDKARAFKA_PASSWORD");
		final Properties kafkaProperties = new Properties();
		final String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		final String jaasCfg = String.format(jaasTemplate, username, password);
		kafkaProperties.put("security.protocol", "SASL_SSL");
		kafkaProperties.put("sasl.mechanism", "SCRAM-SHA-256");
		kafkaProperties.put("sasl.jaas.config", jaasCfg);

		kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-0.0.4");
		kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);

		return kafkaProperties;
	}

	private static String loadInputTopicName() {
		final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		return username + "-default";
	}

	private static String loadOutputTopicName() {
		final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		return username + "-aggregated";
	}

	private static String loadAggregatedStreamStoreTopicName() {
		final String username = System.getenv("CLOUDKARAFKA_USERNAME");
		return username + "-stream-store";
	}

}
