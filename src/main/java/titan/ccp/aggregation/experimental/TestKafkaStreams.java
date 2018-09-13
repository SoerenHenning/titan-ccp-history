package titan.ccp.aggregation.experimental;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;

public class TestKafkaStreams {

	private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaStreams.class);

	private final String inputTopic = "test";
	private final String aggregationStoreName = "stream-store"; // TODO

	public static void main(final String[] args) {
		final Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "titanccp-aggregation-test-0.0.7"); // TODO as parameter
		settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // TODO as parameter
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, ActivePowerRecord> input = builder.stream("input",
				Consumed.with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

		input.foreach((k, v) -> System.out.println(k + ": " + v));

		final KStream<String, Long> sensorCounts = input
				.groupBy((k, v) -> k,
						Serialized.with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
				.aggregate(() -> 0L, (ak, n, av) -> av + 1,
						Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-test-1")
								.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
				.toStream();

		sensorCounts.foreach((k, v) -> System.out.println(k + ": " + v));

		sensorCounts.groupBy((k, v) -> "all", Serialized.with(Serdes.String(), Serdes.Long()))
				.aggregate(() -> 0L, (ak, n, av) -> av + n,
						Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-test-2")
								.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
				.toStream().foreach((k, v) -> System.out.println(k + ": " + v));

		// .aggregate(v -> 0, (aggKey, newValue, aggValue) -> aggValue + 1,
		// Materialized.as("aggregated-stream-store"));

		final KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(settings));
		streams.start();
	}

}
