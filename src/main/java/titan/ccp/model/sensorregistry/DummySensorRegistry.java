package titan.ccp.model.sensorregistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DummySensorRegistry implements SensorRegistry {

	private final Map<String, DummyMachineSensor> dummySensors = new HashMap<>();

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return Optional.of(this.dummySensors.computeIfAbsent(identifier, i -> new DummyMachineSensor(i)));
	}

	@Override
	public AggregatedSensor getTopLevelSensors() {
		return EMPTY_TOP_LEVEL_SENSOR;
	}

	private static final AggregatedSensor EMPTY_TOP_LEVEL_SENSOR = new AggregatedSensorImpl(""); // TODO move

	private static class DummyMachineSensor implements MachineSensor {

		private final String identifier;

		private DummyMachineSensor(final String identifier) {
			this.identifier = identifier;
		}

		@Override
		public Optional<AggregatedSensorImpl> getParent() {
			return Optional.empty();
		}

		@Override
		public String getIdentifier() {
			return this.identifier;
		}

	}
}
