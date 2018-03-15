package titan.ccp.model.sensorregistry;

import java.util.Optional;

public abstract class AbstractSensor implements Sensor {

	private final Optional<AggregatedSensor> parent;

	protected AbstractSensor() {
		this.parent = Optional.empty();
	}

	protected AbstractSensor(final AggregatedSensor parent) {
		parent.addChild(this);
		this.parent = Optional.of(parent);
	}

	public Optional<AggregatedSensor> getParent() {
		return this.parent;
	}

}
