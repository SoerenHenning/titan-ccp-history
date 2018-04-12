package titan.ccp.aggregation;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Serializer;

public class AggregationHistorySerializer implements Serializer<AggregationHistory> {

	private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
	private static final int BYTE_BUFFER_CAPACITY = 65536; // Is only virtual memory

	private final ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

	@Override
	public void configure(final Map<String, ?> configs, final boolean isKey) {
		this.byteBufferSerializer.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(final String topic, final AggregationHistory data) {
		final ByteBuffer buffer = ByteBuffer.allocateDirect(BYTE_BUFFER_CAPACITY);

		buffer.putLong(data.getTimestamp());
		buffer.putInt(data.getLastValues().size());
		for (final Entry<String, Integer> entry : data.getLastValues().entrySet()) {
			final byte[] key = entry.getKey().getBytes(DEFAULT_CHARSET);
			buffer.putInt(key.length);
			buffer.put(key);
			buffer.putInt(entry.getValue());
		}

		return this.byteBufferSerializer.serialize(topic, buffer);
	}

	@Override
	public void close() {
		this.byteBufferSerializer.close();
	}

}