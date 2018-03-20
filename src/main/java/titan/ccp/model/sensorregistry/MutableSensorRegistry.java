package titan.ccp.model.sensorregistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MutableSensorRegistry implements SensorRegistry {

	// TODO HashMap for efficient access to machine sensors
	private final Map<String, MachineSensorImpl> machineSensors = new HashMap<>();

	// TODO maybe access to root
	private final AggregatedSensor topLevelSensor = new AggregatedSensorImpl(""); // TODO perhaps own class

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return Optional.ofNullable(this.machineSensors.get(identifier));
	}

	// TODO return read only
	@Override
	public AggregatedSensor getTopLevelSensors() {
		return this.topLevelSensor;
	}

	// TODO remove
	Map<String, MachineSensorImpl> getMap() { // default
		return this.machineSensors;
	}

	public static SensorRegistry load() {
		// TODO load and parse from json etc.
		return new MutableSensorRegistry();
	}

}
