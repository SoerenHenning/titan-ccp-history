package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public interface MachineSensor extends Sensor {

	public default List<AggregatedSensorImpl> getParents() {
		Optional<AggregatedSensorImpl> parent = this.getParent();
		final List<AggregatedSensorImpl> parents = new ArrayList<>();
		while (parent.isPresent()) {
			parents.add(parent.get());
			parent = parent.get().getParent();
		}
		return parents;
	}

}