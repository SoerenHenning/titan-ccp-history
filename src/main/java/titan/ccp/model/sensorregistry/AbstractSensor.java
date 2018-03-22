package titan.ccp.model.sensorregistry;

import java.util.Optional;

abstract class AbstractSensor implements Sensor {

	private final AggregatedSensor parent;

	private final String identifier;

	protected AbstractSensor(final AggregatedSensor parent, final String identifier) {
		this.parent = parent;
		this.identifier = identifier;
	}

	@Override
	public Optional<AggregatedSensor> getParent() {
		return Optional.of(this.parent);
	}

	@Override
	public String getIdentifier() {
		return this.identifier;
	}

}
