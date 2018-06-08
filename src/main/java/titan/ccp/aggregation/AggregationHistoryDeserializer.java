package titan.ccp.aggregation;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

public class AggregationHistoryDeserializer implements Deserializer<AggregationHistory> {

	private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	private final ByteBufferDeserializer byteBufferDeserializer = new ByteBufferDeserializer();

	@Override
	public void configure(final Map<String, ?> configs, final boolean isKey) {
		this.byteBufferDeserializer.configure(configs, isKey);
	}

	@Override
	public AggregationHistory deserialize(final String topic, final byte[] data) {
		final ByteBuffer buffer = this.byteBufferDeserializer.deserialize(topic, data);

		if (buffer == null) {
			return new AggregationHistory();
		}

		final long timestamp = buffer.getLong();

		final Map<String, Double> map = new HashMap<>();
		final int size = buffer.getInt();
		for (int i = 0; i < size; i++) {
			final int keyLength = buffer.getInt();
			final byte[] keyBytes = new byte[keyLength];
			buffer.get(keyBytes);
			final String key = new String(keyBytes, DEFAULT_CHARSET);
			final double value = buffer.getDouble();

			map.put(key, value);
		}

		return new AggregationHistory(map, timestamp);
	}

	@Override
	public void close() {
		this.byteBufferDeserializer.close();
	}

}
