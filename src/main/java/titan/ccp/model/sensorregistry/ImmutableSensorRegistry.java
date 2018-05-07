package titan.ccp.model.sensorregistry;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import titan.ccp.model.sensorregistry.serialization.AggregatedSensorSerializer;
import titan.ccp.model.sensorregistry.serialization.MachineSensorSerializer;
import titan.ccp.model.sensorregistry.serialization.SensorRegistryDeserializer;
import titan.ccp.model.sensorregistry.serialization.SensorRegistrySerializer;

public final class ImmutableSensorRegistry implements SensorRegistry {

	private static final Gson GSON = new GsonBuilder()
			.registerTypeAdapter(ImmutableSensorRegistry.class, new SensorRegistrySerializer())
			.registerTypeAdapter(ImmutableSensorRegistry.ImmutableAggregatatedSensor.class,
					new AggregatedSensorSerializer())
			.registerTypeAdapter(ImmutableSensorRegistry.ImmutableMachineSensor.class, new MachineSensorSerializer())
			.registerTypeAdapter(SensorRegistry.class, new SensorRegistryDeserializer()).create();

	private final ImmutableMap<String, MachineSensor> machineSensors;
	private final AggregatedSensor topLevelSensor;

	private ImmutableSensorRegistry(final SensorRegistry sensorRegistry) {
		final ImmutableMap.Builder<String, MachineSensor> mapBuilder = ImmutableMap.builder();
		this.topLevelSensor = new ImmutableAggregatatedSensor(null, sensorRegistry.getTopLevelSensor(), mapBuilder);
		this.machineSensors = mapBuilder.build();
	}

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return Optional.ofNullable(this.machineSensors.get(identifier));
	}

	@Override
	public AggregatedSensor getTopLevelSensor() {
		return this.topLevelSensor;
	}

	@Override
	public Collection<MachineSensor> getMachineSensors() {
		return this.machineSensors.values();
	}

	@Override
	public String toJson() {
		// Necessary method. Deletion would cause SensorRegistry.toJson() to fail.
		return GSON.toJson(this);
	}

	public static ImmutableSensorRegistry copyOf(final SensorRegistry sensorRegistry) {
		return new ImmutableSensorRegistry(sensorRegistry);
	}

	public static SensorRegistry fromJson(final String json) {
		return GSON.fromJson(json, SensorRegistry.class);
	}

	private static class AbstractImmutableSensor implements Sensor {

		private final AggregatedSensor parent;
		private final String identifier;

		private AbstractImmutableSensor(final AggregatedSensor newParent, final Sensor sensorToCopy) {
			this.parent = newParent;
			this.identifier = sensorToCopy.getIdentifier();
		}

		@Override
		public Optional<AggregatedSensor> getParent() {
			return Optional.ofNullable(this.parent);
		}

		@Override
		public String getIdentifier() {
			return this.identifier;
		}

	}

	// TODO visibility
	public static final class ImmutableAggregatatedSensor extends AbstractImmutableSensor implements AggregatedSensor {

		private final ImmutableList<Sensor> children;

		private ImmutableAggregatatedSensor(final AggregatedSensor newParent, final AggregatedSensor sensorToCopy,
				final ImmutableMap.Builder<String, MachineSensor> sensorRegistryMapBuilder) {
			super(newParent, sensorToCopy);
			final Builder<Sensor> childrenBuilder = ImmutableList.builder();
			for (final Sensor children : sensorToCopy.getChildren()) {
				if (children instanceof MachineSensor) {
					final MachineSensor newChild = new ImmutableMachineSensor(this, (MachineSensor) children);
					childrenBuilder.add(newChild);
					sensorRegistryMapBuilder.put(newChild.getIdentifier(), newChild);
				} else if (children instanceof AggregatedSensor) {
					final AggregatedSensor newChild = new ImmutableAggregatatedSensor(this, (AggregatedSensor) children,
							sensorRegistryMapBuilder);
					childrenBuilder.add(newChild);
				}
			}
			this.children = childrenBuilder.build();
		}

		@Override
		public Collection<Sensor> getChildren() {
			return this.children;
		}

	}

	// TODO visibility
	public static final class ImmutableMachineSensor extends AbstractImmutableSensor implements MachineSensor {

		private ImmutableMachineSensor(final AggregatedSensor newParent, final MachineSensor SensorToCopy) {
			super(newParent, SensorToCopy);
		}

	}

}
