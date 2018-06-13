package titan.ccp.aggregation.experimental.teetimebased;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import titan.ccp.model.sensorregistry.MachineSensor;

public class LastValueSensorHistory implements SensorHistory {

	private final Map<String, LastValue> lastValues;

	private LastValueSensorHistory(final Map<String, LastValue> backingMap) {
		this.lastValues = backingMap;
	}

	@Override
	public double getOrZero(final MachineSensor machineSensor) {
		final double lastValue = this.lastValues.get(machineSensor.getIdentifier()).value;
		return this.lastValues != null ? lastValue : 0;
	}

	@Override
	public void update(final MachineSensor machineSensor, final double value) {
		this.update(machineSensor, value, Instant.MIN);
	}

	@Override
	public void update(final MachineSensor machineSensor, final double value, final Instant time) {
		// update value for sensor iff sensor has current value or passed value is newer
		// (or equal new)
		final String identifier = machineSensor.getIdentifier();
		this.lastValues.compute(identifier,
				(k, old) -> old == null || !old.time.isAfter(time) ? new LastValue(value, time) : null);
	}

	private static class LastValue {

		private final double value;

		private final Instant time;

		private LastValue(final double value, final Instant time) {
			this.value = value;
			this.time = time;
		}
	}

	public static LastValueSensorHistory createForSingleThread() {
		return new LastValueSensorHistory(new HashMap<>());
	}

	public static LastValueSensorHistory createForMultipleThreads() {
		return new LastValueSensorHistory(new ConcurrentHashMap<>());
	}

}
