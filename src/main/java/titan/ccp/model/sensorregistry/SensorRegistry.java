package titan.ccp.model.sensorregistry;

import java.util.Collection;
import java.util.Optional;

public interface SensorRegistry {

	public Optional<MachineSensor> getSensorForIdentifier(final String identifier);

	public AggregatedSensor getTopLevelSensor();

	public Collection<MachineSensor> getMachineSensors();

	/**
	 * Converts this sensor registry into a json string.
	 *
	 * Per default a copy of this sensor registry is created to ensure that proper
	 * (de)serializers exist. If subclasses have appropriate serdes, they should
	 * override this method.
	 */
	public default String toJson() {
		final ImmutableSensorRegistry immutableSensorRegistry = ImmutableSensorRegistry.copyOf(this);
		return immutableSensorRegistry.toJson();
	}

	public static SensorRegistry fromJson(final String json) {
		return ImmutableSensorRegistry.fromJson(json);
	}

}