package titan.ccp.aggregation.streamprocessing;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * {@link Serde} for {@link AggregationHistory}.
 */
public final class AggregationHistorySerde {

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  private AggregationHistorySerde() {}

  public static Serde<AggregationHistory> serde() {
    return Serdes.serdeFrom(new AggregationHistorySerializer(),
        new AggregationHistoryDeserializer());
  }

  public static Serializer<AggregationHistory> serializer() {
    return new AggregationHistorySerializer();
  }

  public static Deserializer<AggregationHistory> deserializer() {
    return new AggregationHistoryDeserializer();
  }

  private static class AggregationHistorySerializer implements Serializer<AggregationHistory> {

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
      for (final Entry<String, Double> entry : data.getLastValues().entrySet()) {
        final byte[] key = entry.getKey().getBytes(AggregationHistorySerde.DEFAULT_CHARSET);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putDouble(entry.getValue());
      }

      buffer.flip();
      return this.byteBufferSerializer.serialize(topic, buffer);
    }

    @Override
    public void close() {
      this.byteBufferSerializer.close();
    }

  }

  private static class AggregationHistoryDeserializer implements Deserializer<AggregationHistory> {

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
        final String key = new String(keyBytes, AggregationHistorySerde.DEFAULT_CHARSET);
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
}
