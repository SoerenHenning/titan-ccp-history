package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MachineSensor extends AbstractSensor {

	private long lastValue;

	protected MachineSensor() {
		super();
	}

	protected MachineSensor(final AggregatedSensor parent) {
		super(parent);
	}

	public List<AggregatedSensor> getParents() {
		Optional<AggregatedSensor> parent = this.getParent();
		final List<AggregatedSensor> parents = new ArrayList<>();
		while (parent.isPresent()) {
			parents.add(parent.get());
			parent = parent.get().getParent();
		}
		return parents;
	}

	public long getLastValue() {
		return lastValue;
	}

	public void setLastValue(long lastValue) {
		this.lastValue = lastValue;
	}

}
