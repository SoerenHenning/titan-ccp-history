package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public interface MachineSensor extends Sensor {

	public default List<AggregatedSensor> getParents() {
		Optional<AggregatedSensor> parent = this.getParent();
		final List<AggregatedSensor> parents = new ArrayList<>();
		while (parent.isPresent()) {
			parents.add(parent.get());
			parent = parent.get().getParent();
		}
		return parents;
	}

}