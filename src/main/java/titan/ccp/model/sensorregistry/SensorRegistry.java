package titan.ccp.model.sensorregistry;

import java.util.Optional;

public interface SensorRegistry {

	public Optional<MachineSensor> getSensorForIdentifier(final String identifier);

	// TODO return read only
	public AggregatedSensor getTopLevelSensors();

}