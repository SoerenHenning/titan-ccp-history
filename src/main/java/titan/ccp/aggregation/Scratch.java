package titan.ccp.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.LongSummaryStatistics;
import java.util.Optional;

import titan.ccp.model.PowerConsumptionRecord;

public class Scratch {

	public void process(PowerConsumptionRecord record) {
		
		Identifier identifier = null; // Optain from record
		
		MachineSensor sensor = getSensorForIdentifier(identifier);
		
		// Update last sensor value
		
		List<SensorClass> affectedSensors = getParents(sensor); // Stored in bottom up fashion
		
		for (Sensor affectedSensor : affectedSensors) {
			// Recalculate affected Sensor
		}
		
	}
	
	// Member of MachineSensor
	public List<SensorClass> getParents(MachineSensor sensor) {
		Optional<SensorClass> parent = sensor.getParent();
		List<SensorClass> parents = new ArrayList<>();
		while (parent.isPresent()) {
			parents.add(parent.get());
			parent = parent.get().getParent();
		}
		return parents;
	}
	
	
	public MachineSensor getSensorForIdentifier(Identifier identifier) {
		
		
		return null;
	}
	
	public static interface Sensor {
		
		
		public Optional<SensorClass> getParent(); 
		
	}
	
	public static abstract class AbstractSensor implements Sensor  {
		
		
		public Optional<SensorClass> getParent() {
			return null;
		} 
		
	}
	
	public static class MachineSensor extends AbstractSensor {
		
		public long getLastValue() {
			return 0; //TODO
		}
		
	}
	
	//Aggregated Sensor
	public static class SensorClass extends AbstractSensor {
		
		private List<Sensor> children;
		
		private long total; // Same for avg, median, etc.
		
		public Collection<Sensor> getChildren() {
			return children;
		}
		
		public Collection<MachineSensor> getAllChildren() {
			final List<MachineSensor> sensors = new ArrayList<>();
			final ListIterator<Sensor> untraversedSensorClasses = this.children.listIterator();
			while (untraversedSensorClasses.hasNext()) {
				Sensor sensor = untraversedSensorClasses.next();
				if (sensor instanceof MachineSensor) {
					sensors.add((MachineSensor) sensor);
				} else if (sensor instanceof SensorClass) {
					untraversedSensorClasses.add(sensor);
				}
			}
			return sensors;
		}
		
		public long getTotal() {
			return this.getAllChildren().stream().mapToLong(s -> s.getLastValue()).sum();
		}
		
		public LongSummaryStatistics getStatistics() {
			return this.getAllChildren().stream().mapToLong(s -> s.getLastValue()).summaryStatistics();
		}
		
		
	}
	
	//Dummy class for identifier
	public static class Identifier {
		
	}
	
}
