package titan.ccp.aggregation.experimental.teetimebased;

import java.time.Instant;

import titan.ccp.model.sensorregistry.MachineSensor;

public interface SensorHistory {

	public double getOrZero(MachineSensor machineSensor);

	// public Long getOrNull(MachineSensor machineSensor);

	public void update(MachineSensor machineSensor, double value);

	public void update(MachineSensor machineSensor, double value, Instant time);

}