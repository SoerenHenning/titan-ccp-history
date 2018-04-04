package titan.ccp.aggregation.experimental.teetimebased;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.io.IValueSerializer;

public class Scratch {

	public Scratch() {

	}

	private void testSerialization(final IMonitoringRecord monitoringRecord) {

		monitoringRecord.serialize(new IValueSerializer() {

			@Override
			public void putString(final String value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putShort(final short value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putLong(final long value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putInt(final int value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putFloat(final float value) {
				// TODO Auto-generated method stub

			}

			@Override
			public <T extends Enum<T>> void putEnumeration(final T value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putDouble(final double value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putChar(final char value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putBytes(final byte[] value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putByte(final byte value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void putBoolean(final boolean value) {
				// TODO Auto-generated method stub

			}
		});

		final String name = monitoringRecord.getClass().getName();
		final String[] valueNames = monitoringRecord.getValueNames();
		final Class<?>[] valueTypes = monitoringRecord.getValueTypes();
		final Object[] array = monitoringRecord.toArray();
		System.out.println(monitoringRecord);
	}

	public static void main(final String[] args) {

		// new Scratch().run();
	}

}
