package titan.ccp.model.sensorregistry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import titan.ccp.model.sensorregistry.serialization.AggregatedSensorSerializer;
import titan.ccp.model.sensorregistry.serialization.MachineSensorSerializer;
import titan.ccp.model.sensorregistry.serialization.SensorRegistrySerializer;

public class MutableSensorRegistry implements SensorRegistry {

	private static final Gson GSON = new GsonBuilder()
			.registerTypeAdapter(MutableSensorRegistry.class, new SensorRegistrySerializer())
			.registerTypeAdapter(MutableAggregatedSensor.class, new AggregatedSensorSerializer())
			.registerTypeAdapter(MutableMachineSensor.class, new MachineSensorSerializer()).create();

	// TODO HashMap for efficient access to machine sensors
	private final Map<String, MutableMachineSensor> machineSensors = new HashMap<>();

	// TODO maybe access to root
	private final MutableAggregatedSensor topLevelSensor;

	public MutableSensorRegistry(final String topLevelSensorIdentifier) {
		this.topLevelSensor = new MutableAggregatedSensor(this, topLevelSensorIdentifier);
	}

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return Optional.ofNullable(this.machineSensors.get(identifier));
	}

	@Override
	public MutableAggregatedSensor getTopLevelSensor() {
		return this.topLevelSensor;
	}

	@Override
	public Collection<MachineSensor> getMachineSensors() {
		return Collections.unmodifiableCollection(this.machineSensors.values());
	}

	protected boolean register(final MutableMachineSensor machineSensor) {
		final Object oldValue = this.machineSensors.putIfAbsent(machineSensor.getIdentifier(), machineSensor);
		return oldValue == null;
	}

	@Override
	public String toJson() {
		return GSON.toJson(this);
	}

}
