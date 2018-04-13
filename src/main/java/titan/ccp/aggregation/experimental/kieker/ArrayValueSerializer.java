package titan.ccp.aggregation.experimental.kieker;

import java.util.function.Function;

import kieker.common.record.io.IValueSerializer;

/**
 * An {@link IValueSerializer} that serializes a record into a given
 * {@link Object} array. This array has to be large enough to fit in all values.
 * Otherwise, an {@link IllegalArgumentException} is thrown when the first value
 * should be serialized that does not fit in the array.
 *
 * @author Sören Henning
 *
 */
public class ArrayValueSerializer implements IValueSerializer {

	private final Object[] array;

	private final Function<Enum<?>, Object> enumStrategy;

	private int nextIndex = 0;

	public ArrayValueSerializer(final Object[] array) {
		this(array, EnumStrategies.TO_ORDINAL);
	}

	public ArrayValueSerializer(final Object[] array, final Function<Enum<?>, Object> enumStrategy) {
		this.array = array;
		this.enumStrategy = enumStrategy;
	}

	@Override
	public void putBoolean(final boolean value) {
		this.putAny(value);
	}

	@Override
	public void putByte(final byte value) {
		this.putAny(value);
	}

	@Override
	public void putChar(final char value) {
		this.putAny(value);
	}

	@Override
	public void putShort(final short value) {
		this.putAny(value);
	}

	@Override
	public void putInt(final int value) {
		this.putAny(value);
	}

	@Override
	public void putLong(final long value) {
		this.putAny(value);
	}

	@Override
	public void putFloat(final float value) {
		this.putAny(value);
	}

	@Override
	public void putDouble(final double value) {
		this.putAny(value);
	}

	@Override
	public <T extends Enum<T>> void putEnumeration(final T value) {
		this.putAny(this.enumStrategy.apply(value));
	}

	@Override
	public void putBytes(final byte[] value) {
		this.putAny(value);
	}

	@Override
	public void putString(final String value) {
		this.putAny(value);
	}

	public void putAny(final Object value) {
		if (this.nextIndex < this.array.length) {
			this.array[this.nextIndex] = value;
			this.nextIndex++;
		}
		throw new IllegalArgumentException("The given array is not large enough to serialize all fields of this record.");
	}

	public static final class EnumStrategies {

		public static final Function<Enum<?>, Object> TO_ORDINAL = v -> v.ordinal();

		public static final Function<Enum<?>, Object> TO_STRING = v -> v.toString();

		public static final Function<Enum<?>, Object> TO_NAME = v -> v.name();

	}

}
