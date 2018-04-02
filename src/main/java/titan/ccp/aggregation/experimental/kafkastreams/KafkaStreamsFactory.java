package titan.ccp.aggregation.experimental.kafkastreams;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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
import org.apache.kafka.streams.state.KeyValueStore;

import kieker.common.record.io.BinaryValueDeserializer;
import kieker.common.record.io.BinaryValueSerializer;
import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class KafkaStreamsFactory {

	public KafkaStreams create() {
		final StreamsBuilder builder = new StreamsBuilder(); // when using the DSL

		final KStream<String, PowerConsumptionRecord> wordCounts = builder.stream("partition-topic", /* input topic */
				Consumed.with(Serdes.String(), createPowerConsumptionSerde()));

		final KStream<String, PowerConsumptionRecord> flatMapped = wordCounts
				.flatMap((key, value) -> this.flatMap(value));

		final KGroupedStream<String, PowerConsumptionRecord> groupedStream = flatMapped.groupByKey();

		final KTable<String, Long> aggregated = groupedStream.aggregate(() -> {
			// new AggregatedSensorHistory();
			return null;
		}, /* initializer */
				(aggKey, newValue, aggValue) -> {
					System.out.println("Agg: " + aggKey + ":" + newValue);
					// AggregatedSensorHistory aggValue2;
					// aggValue2.update(aggKey, newValue.getPowerConsumptionInWh());
					return (long) newValue.getPowerConsumptionInWh();
				}, /* adder */
				Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-for-partition") // state
																														// store
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
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-0.0.2");
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Any further settings
		// settings.put(... , ...);
		final StreamsConfig config = new StreamsConfig(settings);

		return new KafkaStreams(topology, config);
	}

	private Iterable<KeyValue<String, PowerConsumptionRecord>> flatMap(final PowerConsumptionRecord record) {
		final SensorRegistry sensorRegistry = null;
		final Optional<MachineSensor> sensor = sensorRegistry.getSensorForIdentifier(record.getIdentifier().toString()); // TODO
																															// temp
		return sensor.stream().flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier())
				.map(i -> KeyValue.pair(i, record)).collect(Collectors.toList());
	}

	private static final Serde<PowerConsumptionRecord> createPowerConsumptionSerde() {
		return Serdes.serdeFrom(new PowerConsumptionRecordSerializer(), new PowerConsumptionRecordDeserializer());
	}

	private static class PowerConsumptionRecordDeserializer implements Deserializer<PowerConsumptionRecord> {

		private final ByteBufferDeserializer byteBufferDeserializer = new ByteBufferDeserializer();

		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			this.byteBufferDeserializer.configure(configs, isKey);
		}

		@Override
		public PowerConsumptionRecord deserialize(final String topic, final byte[] data) {
			final ByteBuffer buffer = this.byteBufferDeserializer.deserialize(topic, data);

			final int stringLength = buffer.getInt();
			final byte[] stringBytes = new byte[stringLength];
			buffer.get(stringBytes);
			final String identifier = buffer.toString(); // TODO this is wrong and just temp
			final long timestamp = buffer.getLong();
			final int powerConsumption = buffer.getInt();

			return new PowerConsumptionRecord(identifier.getBytes(Charset.forName("UTF-8")), timestamp,
					powerConsumption);
		}

		@Override
		public void close() {
			this.byteBufferDeserializer.close();
		}

	}

	private static class PowerConsumptionRecordSerializer implements Serializer<PowerConsumptionRecord> {

		private final ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			this.byteBufferSerializer.configure(configs, isKey);
		}

		@Override
		public byte[] serialize(final String topic, final PowerConsumptionRecord record) {
			final String identifier = record.getIdentifier().toString(); // TODO Identifier will be String

			final ByteBuffer buffer = ByteBuffer.allocateDirect(65536); // TODO
			final byte[] stringBytes = identifier.getBytes(Charset.forName("UTF-8")); // TODO do this more effiently
			buffer.putInt(stringBytes.length);
			buffer.put(stringBytes);
			buffer.putLong(record.getTimestamp());
			buffer.putInt(record.getPowerConsumptionInWh());

			return this.byteBufferSerializer.serialize(topic, buffer);
		}

		@Override
		public void close() {
			this.byteBufferSerializer.close();
		}

	}

	private static class RegistrylessBinaryValueDeserializer extends BinaryValueDeserializer {

		protected RegistrylessBinaryValueDeserializer(final ByteBuffer buffer) {
			super(buffer, null);
		}

	}

	private static class RegistrylessBinaryValueSerializer extends BinaryValueSerializer {

		private final ByteBuffer buffer;

		private RegistrylessBinaryValueSerializer(final ByteBuffer buffer) {
			super(buffer, null);
			this.buffer = buffer;
		}

		public static RegistrylessBinaryValueSerializer create() {
			return new RegistrylessBinaryValueSerializer(ByteBuffer.allocateDirect(65536));
		}

		@Override
		public void putString(final String value) {
			final byte[] bytes = value.getBytes(Charset.forName("UTF-8")); // TODO do this more effiently
			this.buffer.putInt(bytes.length);
			this.buffer.put(bytes);
		}

		public ByteBuffer getByteBuffer() {
			return this.buffer;
		}

	}

	private static class AggregatedSensorHistory {

		private final Map<String, Long> lastValues = new HashMap<>();

		public void update(final String identifier, final long newValue) {
			this.lastValues.put(identifier, newValue);
		}

	}

}
