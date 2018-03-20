package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AggregatedSensorImpl extends AbstractSensor implements AggregatedSensor {

	private final List<Sensor> children = new ArrayList<>();

	protected AggregatedSensorImpl(final String identifier) {
		super(identifier);
	}

	protected AggregatedSensorImpl(final String identifier, final AggregatedSensorImpl parent) {
		super(identifier, parent);
	}

	@Override
	public Collection<Sensor> getChildren() {
		return this.children;
	}

	// TODO
	void addChild(final Sensor sensor) { // package-private
		this.children.add(sensor);
	}

}
