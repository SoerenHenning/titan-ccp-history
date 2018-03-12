package titan.ccp.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.LongSummaryStatistics;
import java.util.Optional;

import titan.ccp.model.PowerConsumptionRecord;

public class Scratch {

	// Create SensorRegistry by querying it from corresponding server
	private final SensorRegistry sensorRegistry = SensorRegistry.load();
	
	public void process(PowerConsumptionRecord record) {
		
		
		byte[] identifier2 = record.getIdentifier();
		Identifier identifier = null; // Obtain from record
		MachineSensor sensor = this.sensorRegistry.getSensorForIdentifier(identifier);
		
		// Update last sensor value
		sensor.setLastValue(0); //TODO
		
		// Get all affected sensor classes
		List<SensorClass> affectedSensors = sensor.getParents(); // Stored in bottom up fashion
		
		for (SensorClass affectedSensor : affectedSensors) {
			// Recalculate affected sensor class
			affectedSensor.getStatistics();

			//TODO update
		}
		
	}
	
	
	public static class SensorRegistry {
		
		//TODO HashMap for efficient access to machine sensors
		//TODO maybe access to root (but perhaps better not)
		
		public MachineSensor getSensorForIdentifier(Identifier identifier) {
			//TODO 
			return null;
		}
	
		public static SensorRegistry load() {
			//TODO load and parse from json etc.
			return new SensorRegistry();
		}
		
	}
	
	public static interface Sensor {
				
		public Optional<SensorClass> getParent(); 

	}
	
	public static abstract class AbstractSensor implements Sensor  {
		
		private final Optional<SensorClass> parent = Optional.empty();
		
		public Optional<SensorClass> getParent() {
			return this.parent;
		} 
		
	}
	
	public static class MachineSensor extends AbstractSensor {
		
		private long lastValue;
		
		public List<SensorClass> getParents() {
			Optional<SensorClass> parent = this.getParent();
			List<SensorClass> parents = new ArrayList<>();
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
	
	//Aggregated Sensor
	public static class SensorClass extends AbstractSensor {
		
		private List<Sensor> children;
		
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
