package titan.ccp.aggregation.experimental.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaStreamsTest {

	public static void main(final String[] args) {
		// TODO Auto-generated method stub

		// Use the builders to define the actual processing topology, e.g. to specify
		// from which input topics to read, which stream operations (filter, map, etc.)
		// should be called, and so on. We will cover this in detail in the subsequent
		// sections of this Developer Guide.

		final StreamsBuilder builder = new StreamsBuilder(); // when using the DSL

		final KStream<String, Long> wordCounts = builder.stream("word-counts-input-topic", /* input topic */
				Consumed.with(Serdes.String(), /* key serde */
						Serdes.Long() /* value serde */
				));
		final KGroupedStream<String, Long> groupedStream = wordCounts.groupByKey();

		final KTable<String, Long> aggregated = groupedStream.aggregate(() -> null, /* initializer */
				(aggKey, newValue, aggValue) -> newValue, /* adder */
				Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
						"aggregated-stream-store" /* state store name */).withKeySerde(Serdes.String()) /* key serde */
						.withValueSerde(Serdes.Long())); /* serde for aggregate value */

		final Topology topology = builder.build();

		// Use the configuration to tell your application where the Kafka cluster is,
		// which Serializers/Deserializers to use by default, to specify security
		// settings,
		// and so on.
		final Properties settings = new Properties();
		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application");
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Any further settings
		// settings.put(... , ...);
		final StreamsConfig config = new StreamsConfig(settings);

		final KafkaStreams streams = new KafkaStreams(topology, config);

		// Start the Kafka Streams threads
		streams.start();

	}

}
