package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MutableAggregatedSensor extends AbstractSensor implements AggregatedSensor {

	private final List<Sensor> children = new ArrayList<>();
	private final MutableSensorRegistry sensorRegistry;

	protected MutableAggregatedSensor(final MutableSensorRegistry registry, final String identifier) {
		super(null, identifier);
		this.sensorRegistry = registry;
	}

	protected MutableAggregatedSensor(final MutableAggregatedSensor parent, final String identifier) {
		super(parent, identifier);
		this.sensorRegistry = parent.sensorRegistry;
	}

	@Override
	public Collection<Sensor> getChildren() {
		return this.children;
	}

	public MutableAggregatedSensor addChildAggregatedSensor(final String identifier) {
		final MutableAggregatedSensor aggregatedSensor = new MutableAggregatedSensor(this, identifier);
		this.children.add(aggregatedSensor);
		return aggregatedSensor;
	}

	public MachineSensor addChildMachineSensor(final String identifier) {
		final MutableMachineSensor machineSensor = new MutableMachineSensor(this, identifier);
		final boolean registerResult = this.sensorRegistry.register(machineSensor);
		if (!registerResult) {
			throw new IllegalArgumentException("Sensor width identifier " + identifier + " is already registered.");
		}
		this.children.add(machineSensor);
		return machineSensor;
	}

}
