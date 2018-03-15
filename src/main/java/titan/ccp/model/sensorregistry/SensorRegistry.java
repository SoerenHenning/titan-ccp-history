package titan.ccp.model.sensorregistry;

import java.util.HashMap;
import java.util.Map;

import titan.ccp.aggregation.Scratch.Identifier;

public class SensorRegistry {
	
	//TODO HashMap for efficient access to machine sensors
	private final Map<Identifier, MachineSensor> machineSensors = new HashMap<>();
	
	//TODO maybe access to root
	
	
	
	public MachineSensor getSensorForIdentifier(Identifier identifier) {
		//TODO 
		//Optional.ofNullable(machineSensors.get(identifier));
		return machineSensors.get(identifier);
	}

	public static SensorRegistry load() {
		//TODO load and parse from json etc.
		return new SensorRegistry();
	}
	
}
