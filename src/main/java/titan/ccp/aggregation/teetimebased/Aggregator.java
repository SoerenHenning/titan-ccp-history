package titan.ccp.aggregation.teetimebased;

import java.time.Instant;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.function.Consumer;

import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class Aggregator {

	// Create SensorRegistry by querying it from corresponding server
	private final SensorRegistry sensorRegistry; // TODO

	private final SensorHistory sensorHistory; // TODO fill map of last values (by async call to other service)

	private final Consumer<AggregationResult> resultHandler;

	public Aggregator(final SensorRegistry sensorRegistry, final SensorHistory sensorHistory,
			final Consumer<AggregationResult> resultHandler) {
		this.sensorRegistry = sensorRegistry;
		this.sensorHistory = sensorHistory;
		this.resultHandler = resultHandler;
	}

	public void process(final PowerConsumptionRecord record) {

		// TODO rework this
		final String identifier = record.getIdentifier();
		final MachineSensor sensor = this.sensorRegistry.getSensorForIdentifier(identifier).get(); // TODO bad

		// Update last sensor value
		final long powerConsumption = record.getPowerConsumptionInWh();
		final Instant time = Instant.ofEpochMilli(record.getTimestamp()); // TODO
		this.sensorHistory.update(sensor, powerConsumption, time);

		// Get all affected sensor classes
		final List<AggregatedSensor> affectedSensors = sensor.getParents();
		// Recalculate affected sensor class
		for (final AggregatedSensor affectedSensor : affectedSensors) {
			final LongSummaryStatistics statistics = affectedSensor.getAllChildren().stream()
					.mapToLong(s -> this.sensorHistory.getOrZero(s)).summaryStatistics();
			// .mapToLong(s -> this.sensorHistory.getOrNull(s)).filter(Objects::nonNull)

			final AggregationResult aggregationResult = new AggregationResult(affectedSensor, time, statistics);

			this.resultHandler.accept(aggregationResult);
		}

	}

}
