package titan.ccp.aggregation;

import java.util.List;

import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class Scratch {

	// Create SensorRegistry by querying it from corresponding server
	private final SensorRegistry sensorRegistry = SensorRegistry.load(); //TODO
	
	public void process(PowerConsumptionRecord record) {
		
		
		byte[] identifier2 = record.getIdentifier();
		Identifier identifier = Identifier.getForBytes(identifier2); // Obtain from record
		MachineSensor sensor = this.sensorRegistry.getSensorForIdentifier(identifier);
		
		// Update last sensor value
		long powerConsumption = record.getPowerConsumptionInWh();
		sensor.setLastValue(powerConsumption); //TODO record.value
		
		// Get all affected sensor classes
		List<AggregatedSensor> affectedSensors = sensor.getParents(); // Stored in bottom up fashion
		
		for (AggregatedSensor affectedSensor : affectedSensors) {
			// Recalculate affected sensor class
			affectedSensor.getStatistics();

			//TODO update store
		}
		
	}
	
	
	//Dummy class for identifier
	public static interface Identifier {
		
		public static Identifier getForBytes(byte[] bytes) {
			return new Identifier() {};
		} 
		
	}
	
}
