package titan.ccp.history.streamprocessing;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * {@link Serde} factory for an optional {@link Set} of parent identifiers.
 */
public final class OptionalParentsSerde {

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  private OptionalParentsSerde() {}

  public static Serde<Optional<Set<String>>> serde() {
    return Serdes.serdeFrom(new ParentsSerializer(), new ParentsDeserializer());
  }

  private static class ParentsSerializer implements Serializer<Optional<Set<String>>> {

    private static final int BYTE_BUFFER_CAPACITY = 65536; // Is only virtual memory

    private final ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      this.byteBufferSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Optional<Set<String>> data) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(BYTE_BUFFER_CAPACITY);

      if (data.isPresent()) {
        buffer.put((byte) 1);
        final Set<String> parents = data.get();
        buffer.putInt(parents.size());
        for (final String parent : parents) {
          final byte[] bytes = parent.getBytes(DEFAULT_CHARSET);
          buffer.putInt(bytes.length);
          buffer.put(bytes);
        }
      } else {
        buffer.put((byte) 0);
      }

      buffer.flip();
      return this.byteBufferSerializer.serialize(topic, buffer);
    }

    @Override
    public void close() {
      this.byteBufferSerializer.close();
    }

  }

  private static class ParentsDeserializer implements Deserializer<Optional<Set<String>>> {

    private final ByteBufferDeserializer byteBufferDeserializer = new ByteBufferDeserializer();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      this.byteBufferDeserializer.configure(configs, isKey);
    }

    @Override
    public Optional<Set<String>> deserialize(final String topic, final byte[] data) {
      final ByteBuffer buffer = this.byteBufferDeserializer.deserialize(topic, data);

      if (buffer == null) {
        // TODO return null
        return Optional.empty();
      }

      if (buffer.get() == 0) {
        return Optional.empty();
      }

      final int size = buffer.getInt();
      final Set<String> parents = new HashSet<>(size);
      for (int i = 0; i < size; i++) {
        final int bytesLength = buffer.getInt();
        final byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        final String parent = new String(bytes, DEFAULT_CHARSET);
        parents.add(parent);
      }

      return Optional.of(parents);
    }

    @Override
    public void close() {
      this.byteBufferDeserializer.close();
    }

  }

}
