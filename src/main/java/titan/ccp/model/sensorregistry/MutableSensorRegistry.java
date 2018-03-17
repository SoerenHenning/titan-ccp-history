package titan.ccp.model.sensorregistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MutableSensorRegistry implements SensorRegistry {

	// TODO HashMap for efficient access to machine sensors
	private final Map<Long, MachineSensor> machineSensors = new HashMap<>();

	// TODO maybe access to root
	private final AggregatedSensor topLevelSensor = new AggregatedSensor();

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final Long identifier) {
		// TODO
		return Optional.ofNullable(this.machineSensors.get(identifier));
		// return this.machineSensors.get(identifier);
	}

	// TODO return read only
	@Override
	public AggregatedSensor getTopLevelSensors() {
		return this.topLevelSensor;
	}

	// TODO remove
	Map<Long, MachineSensor> getMap() { // default
		return this.machineSensors;
	}

	public static SensorRegistry load() {
		// TODO load and parse from json etc.
		return new MutableSensorRegistry();
	}

}
