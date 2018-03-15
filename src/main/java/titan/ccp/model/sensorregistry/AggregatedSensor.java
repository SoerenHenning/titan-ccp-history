package titan.ccp.model.sensorregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.LongSummaryStatistics;

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

	public Collection<MachineSensor> getAllChildren() {
		final List<MachineSensor> sensors = new ArrayList<>();
		final ListIterator<Sensor> untraversedSensorClasses = this.children.listIterator();
		while (untraversedSensorClasses.hasNext()) {
			final Sensor sensor = untraversedSensorClasses.next();
			if (sensor instanceof MachineSensor) {
				sensors.add((MachineSensor) sensor);
			} else if (sensor instanceof AggregatedSensor) {
				untraversedSensorClasses.add(sensor);
			}
		}
		return sensors;
	}

	// TODO not required
	public long getTotal() {
		return this.getAllChildren().stream().mapToLong(s -> s.getLastValue()).sum();
	}

	// TODO remove form here
	public LongSummaryStatistics getStatistics() {
		return this.getAllChildren().stream().mapToLong(s -> s.getLastValue()).summaryStatistics();
	}

}
