package titan.ccp.aggregation;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import titan.ccp.model.sensorregistry.MachineSensor;

public class LastValueSensorHistory implements SensorHistory {

	private final Map<Long, LastValue> lastValues = new HashMap<>();

	public LastValueSensorHistory() {
	}

	@Override
	public long getOrZero(final MachineSensor machineSensor) {
		final long lastValue = this.lastValues.get(machineSensor.getIdentifier()).value;
		return this.lastValues != null ? lastValue : 0;
	}

	@Override
	public void update(final MachineSensor machineSensor, final long value) {
		this.update(machineSensor, value, Instant.MIN);
	}

	@Override
	public void update(final MachineSensor machineSensor, final long value, final Instant time) {
		final LastValue lastValue = this.lastValues.get(machineSensor.getIdentifier());
		if (lastValue == null || !lastValue.time.isAfter(time)) {
			this.lastValues.put(machineSensor.getIdentifier(), new LastValue(value, time));
		}
	}

	private static class LastValue {

		private final long value;

		private final Instant time;

		private LastValue(final long value, final Instant time) {
			this.value = value;
			this.time = time;
		}
	}

}
