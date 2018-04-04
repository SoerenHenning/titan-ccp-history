package titan.ccp.aggregation.experimental.kieker;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import kieker.common.record.io.BinaryValueDeserializer;
import kieker.common.record.io.BinaryValueSerializer;

// Prototype, not sure yet if later necessary
public class KiekerSerdes {

	public static class RegistrylessBinaryValueDeserializer extends BinaryValueDeserializer {

		protected RegistrylessBinaryValueDeserializer(final ByteBuffer buffer) {
			super(buffer, null);
		}

	}

	public static class RegistrylessBinaryValueSerializer extends BinaryValueSerializer {

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

}
