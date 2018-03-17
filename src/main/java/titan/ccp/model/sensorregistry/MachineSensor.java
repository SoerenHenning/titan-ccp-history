package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MachineSensor extends AbstractSensor {

	private final long identifier;

	protected MachineSensor(final long identifier) {
		super();
		this.identifier = identifier;
	}

	protected MachineSensor(final long identifier, final AggregatedSensor parent) {
		super(parent);
		this.identifier = identifier;
	}

	// TODO in interface default implementation
	public List<AggregatedSensor> getParents() {
		Optional<AggregatedSensor> parent = this.getParent();
		final List<AggregatedSensor> parents = new ArrayList<>();
		while (parent.isPresent()) {
			parents.add(parent.get());
			parent = parent.get().getParent();
		}
		return parents;
	}

	public long getIdentifier() {
		return identifier;
	}

}
