package titan.ccp.model.sensorregistry;

import java.util.Collection;
import java.util.Collections;
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
	public AggregatedSensor getTopLevelSensor() {
		return EMPTY_TOP_LEVEL_SENSOR;
	}

	@Override
	public Collection<MachineSensor> getMachineSensors() {
		return Collections.emptyList();
	}

	private static final AggregatedSensor EMPTY_TOP_LEVEL_SENSOR = new EmptyTopLevelSensor();

	// TODO move
	private static class EmptyTopLevelSensor implements AggregatedSensor {

		private EmptyTopLevelSensor() {
		}

		@Override
		public Optional<AggregatedSensor> getParent() {
			return Optional.empty();
		}

		@Override
		public String getIdentifier() {
			return "";
		}

		@Override
		public Collection<Sensor> getChildren() {
			return Collections.emptyList();
		}

	}

	private static class DummyMachineSensor implements MachineSensor {

		private final String identifier;

		private DummyMachineSensor(final String identifier) {
			this.identifier = identifier;
		}

		@Override
		public Optional<AggregatedSensor> getParent() {
			return Optional.empty();
		}

		@Override
		public String getIdentifier() {
			return this.identifier;
		}

	}

}
