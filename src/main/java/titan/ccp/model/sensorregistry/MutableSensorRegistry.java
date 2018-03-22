package titan.ccp.model.sensorregistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MutableSensorRegistry implements SensorRegistry {

	// TODO HashMap for efficient access to machine sensors
	private final Map<String, MutableMachineSensor> machineSensors = new HashMap<>();

	// TODO maybe access to root
	private final MutableAggregatedSensor topLevelSensor = new MutableAggregatedSensor(this, ""); // TODO perhaps
																									// own class

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return Optional.ofNullable(this.machineSensors.get(identifier));
	}

	@Override
	public MutableAggregatedSensor getTopLevelSensor() {
		return this.topLevelSensor;
	}

	protected boolean register(final MutableMachineSensor machineSensor) {
		final Object oldValue = this.machineSensors.putIfAbsent(machineSensor.getIdentifier(), machineSensor);
		return oldValue == null;

	}

}
