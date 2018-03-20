package titan.ccp.aggregation;

import java.time.Instant;

import titan.ccp.model.sensorregistry.MachineSensor;

public interface SensorHistory {

	public long getOrZero(MachineSensor machineSensor);

	// public Long getOrNull(MachineSensor machineSensor);

	public void update(MachineSensor machineSensor, long value);

	public void update(MachineSensor machineSensor, long value, Instant time);

}