package titan.ccp.aggregation;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
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
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.ExampleSensors;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class KafkaStreamsFactory {

	private static final String AGGREGATED_STREAM_STORE_TOPIC = "aggregated-stream-store-for-test-topic-18040319";
	private static final String INPUT_TOPIC = "test-topic-18040319";

	public KafkaStreams create() {
		final StreamsBuilder builder = new StreamsBuilder(); // when using the DSL

		final KStream<String, PowerConsumptionRecord> inputStream = builder.stream(INPUT_TOPIC,
				Consumed.with(Serdes.String(), createPowerConsumptionSerde()));

		final KStream<String, PowerConsumptionRecord> flatMapped = inputStream
				.flatMap((key, value) -> this.flatMap(value));

		final KGroupedStream<String, PowerConsumptionRecord> groupedStream = flatMapped
				.groupByKey(Serialized.with(Serdes.String(), createPowerConsumptionSerde()));

		final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(() -> {
			return new AggregationHistory();
		}, (aggKey, newValue, aggValue2) -> {
			System.out.println("__");
			System.out.println("O: " + aggKey + ": " + aggValue2.getLastValues());
			System.out.println("O: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			System.out.println("new: " + newValue.getIdentifier() + ": " + newValue.getPowerConsumptionInWh());
			aggValue2.update(newValue);
			System.out.println("N: " + aggKey + ": " + aggValue2.getLastValues());
			System.out.println("N: " + aggKey + ": " + aggValue2.getSummaryStatistics());
			return aggValue2;
			// return aggValue2.update(newValue);
		}, Materialized.<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(AGGREGATED_STREAM_STORE_TOPIC)
				.withKeySerde(Serdes.String()).withValueSerde(AggregationHistorySerde.create()));

		aggregated.toStream().foreach((key, value) -> System.out.println(key + ": " + value.getSummaryStatistics())); // TODO
		// aggregated.toStream().to("", Produced.with(null, null));

		final Topology topology = builder.build();

		// Use the configuration to tell your application where the Kafka cluster is,
		// which Serializers/Deserializers to use by default, to specify security
		// settings,
		// and so on.
		final Properties settings = new Properties();
		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-0.0.4");
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
		// Serdes.String().getClass().getName());
		// settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,createPowerConsumptionSerde().getClass().getName());
		// Any further settings
		// settings.put(... , ...);
		final StreamsConfig config = new StreamsConfig(settings);

		return new KafkaStreams(topology, config);
	}

	private Iterable<KeyValue<String, PowerConsumptionRecord>> flatMap(final PowerConsumptionRecord record) {
		final SensorRegistry sensorRegistry = ExampleSensors.registry(); // TODO

		return sensorRegistry.getSensorForIdentifier(record.getIdentifier()).stream()
				.flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier()).map(i -> KeyValue.pair(i, record))
				.collect(Collectors.toList());
	}

	//
	//
	//
	//
	//
	// PowerConsumption Serdes

	private static final Serde<PowerConsumptionRecord> createPowerConsumptionSerde() {
		return Serdes.serdeFrom(new PowerConsumptionRecordSerializer(), new PowerConsumptionRecordDeserializer());
	}

	public static class PowerConsumptionRecordDeserializer implements Deserializer<PowerConsumptionRecord> {

		private final ByteBufferDeserializer byteBufferDeserializer = new ByteBufferDeserializer();
		private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

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
			final String identifier = new String(stringBytes, DEFAULT_CHARSET);
			final long timestamp = buffer.getLong();
			final int powerConsumption = buffer.getInt();

			return new PowerConsumptionRecord(identifier, timestamp, powerConsumption);
		}

		@Override
		public void close() {
			this.byteBufferDeserializer.close();
		}

	}

	public static class PowerConsumptionRecordSerializer implements Serializer<PowerConsumptionRecord> {

		private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
		private static final int BYTE_BUFFER_CAPACITY = 65536; // Is only virtual memory

		private final ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			this.byteBufferSerializer.configure(configs, isKey);
		}

		@Override
		public byte[] serialize(final String topic, final PowerConsumptionRecord record) {
			final ByteBuffer buffer = ByteBuffer.allocateDirect(BYTE_BUFFER_CAPACITY);

			final byte[] identifierBytes = record.getIdentifier().getBytes(DEFAULT_CHARSET);
			buffer.putInt(identifierBytes.length);
			buffer.put(identifierBytes);
			buffer.putLong(record.getTimestamp());
			buffer.putInt(record.getPowerConsumptionInWh());

			return this.byteBufferSerializer.serialize(topic, buffer);
		}

		@Override
		public void close() {
			this.byteBufferSerializer.close();
		}

	}

}
