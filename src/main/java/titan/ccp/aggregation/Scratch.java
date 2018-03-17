package titan.ccp.aggregation;

import java.time.Instant;
import java.util.List;

import com.google.common.primitives.Longs;

import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class Scratch {

	// Create SensorRegistry by querying it from corresponding server
	private final SensorRegistry sensorRegistry = MutableSensorRegistry.load(); // TODO

	private final SensorHistory lastValues = new LastValueSensorHistory();

	public Scratch() {
		// TODO fill map of last values (by async call to other service)
	}

	public void process(final PowerConsumptionRecord record) {

		final byte[] identifierAsBytes = record.getIdentifier();
		final Long identifier = Longs.fromByteArray(identifierAsBytes);
		final MachineSensor sensor = this.sensorRegistry.getSensorForIdentifier(identifier).get(); // TODO bad

		// Update last sensor value
		final long powerConsumption = record.getPowerConsumptionInWh();
		final Instant time = Instant.ofEpochMilli(record.getTimestamp()); // TODO
		this.lastValues.update(sensor, powerConsumption, time);

		// Get all affected sensor classes
		final List<AggregatedSensor> affectedSensors = sensor.getParents();

		for (final AggregatedSensor affectedSensor : affectedSensors) {
			// Recalculate affected sensor class
			affectedSensor.getAllChildren().stream().mapToLong(s -> this.lastValues.getOrZero(s))
					.summaryStatistics();

			// TODO update store
		}

	}

}
