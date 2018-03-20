package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public interface AggregatedSensor extends Sensor {

	public Collection<Sensor> getChildren();

	public default Collection<MachineSensorImpl> getAllChildren() {
		final List<MachineSensorImpl> sensors = new ArrayList<>();
		final Queue<Sensor> untraversedSensorClasses = new LinkedList<>(sensors);
		while (untraversedSensorClasses.isEmpty()) {
			final Sensor sensor = untraversedSensorClasses.poll();
			if (sensor instanceof MachineSensorImpl) {
				sensors.add((MachineSensorImpl) sensor);
			} else if (sensor instanceof AggregatedSensorImpl) {
				untraversedSensorClasses.offer(sensor);
			}
		}
		return sensors;
	}

}