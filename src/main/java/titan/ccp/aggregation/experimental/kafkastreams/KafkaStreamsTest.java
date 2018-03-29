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

		final KStream<String, String> wordCounts = builder.stream("input-topic-2", /* input topic */
				Consumed.with(Serdes.String(), /* key serde */
						Serdes.String() /* value serde */
				));
		final KGroupedStream<String, String> groupedStream = wordCounts.groupByKey();

		final KTable<String, Long> aggregated = groupedStream.aggregate(() -> null, /* initializer */
				(aggKey, newValue, aggValue) -> {
					try {
						System.out.println(aggKey + ":" + newValue);
						return Long.parseLong(newValue);
					} catch (final NumberFormatException e) {
						return -1l;
					}
				}, /* adder */
				Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-2") // state store
																											// name
						.withKeySerde(Serdes.String()) /* key serde */
						.withValueSerde(Serdes.Long())); /* serde for aggregate value */

		final Topology topology = builder.build();

		// Use the configuration to tell your application where the Kafka cluster is,
		// which Serializers/Deserializers to use by default, to specify security
		// settings,
		// and so on.
		final Properties settings = new Properties();
		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-0.0.1");
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Any further settings
		// settings.put(... , ...);
		final StreamsConfig config = new StreamsConfig(settings);

		final KafkaStreams streams = new KafkaStreams(topology, config);

		// Start the Kafka Streams threads
		streams.start();

		// while (true) {
		// try {
		// System.out.println("----------");
		// final ReadOnlyKeyValueStore<String, Long> store =
		// streams.store("aggregated-stream-store-2",
		// QueryableStoreTypes.<String, Long>keyValueStore());
		// for (final KeyValue<String, Long> x : (Iterable<KeyValue<String, Long>>) ()
		// -> store.all()) {
		// System.out.println("key: " + x.key + " val: " + x.value);
		// }
		// System.out.println(".");
		// final Collection<StreamsMetadata> allMetadataForStore = streams
		// .allMetadataForStore("aggregated-stream-store-2");
		// System.out.println(allMetadataForStore.toString());
		// for (final StreamsMetadata streamsMetadata : allMetadataForStore) {
		// System.out.println(streamsMetadata.toString());
		// }
		// try {
		// Thread.sleep(5000);
		// } catch (final InterruptedException e) {
		// e.printStackTrace();
		// }
		// } catch (final InvalidStateStoreException e) {
		// e.printStackTrace();
		// }
		// }

	}

}
