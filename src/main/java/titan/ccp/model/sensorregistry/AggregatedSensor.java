package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class AggregatedSensor extends AbstractSensor {

	private final List<Sensor> children = new ArrayList<>();

	protected AggregatedSensor() {
		super();
	}

	protected AggregatedSensor(final AggregatedSensor parent) {
		super(parent);
	}

	public Collection<Sensor> getChildren() {
		return this.children;
	}

	void addChild(final Sensor sensor) { // package-private
		this.children.add(sensor);
	}

	// TODO in interface default implementation
	public Collection<MachineSensor> getAllChildren() {
		final List<MachineSensor> sensors = new ArrayList<>();
		final Queue<Sensor> untraversedSensorClasses = new LinkedList<>(sensors);
		while (untraversedSensorClasses.isEmpty()) {
			final Sensor sensor = untraversedSensorClasses.poll();
			if (sensor instanceof MachineSensor) {
				sensors.add((MachineSensor) sensor);
			} else if (sensor instanceof AggregatedSensor) {
				untraversedSensorClasses.offer(sensor);
			}
		}
		return sensors;
	}

}
