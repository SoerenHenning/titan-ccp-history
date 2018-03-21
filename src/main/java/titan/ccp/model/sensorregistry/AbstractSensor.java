package titan.ccp.model.sensorregistry;

import java.util.Optional;

public abstract class AbstractSensor implements Sensor {

	private final Optional<AggregatedSensor> parent;

	private final String identifier;

	protected AbstractSensor(final String identifier) {
		this.parent = Optional.empty();
		this.identifier = identifier;
	}

	protected AbstractSensor(final String identifier, final AggregatedSensorImpl parent) {
		parent.addChild(this);
		this.parent = Optional.of(parent);
		this.identifier = identifier;
	}

	@Override
	public Optional<AggregatedSensor> getParent() {
		return this.parent;
	}

	@Override
	public String getIdentifier() {
		return this.identifier;
	}

}
