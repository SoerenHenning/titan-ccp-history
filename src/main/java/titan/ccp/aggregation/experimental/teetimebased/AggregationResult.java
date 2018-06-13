package titan.ccp.aggregation.experimental.teetimebased;

import java.time.Instant;
import java.util.DoubleSummaryStatistics;

import titan.ccp.model.sensorregistry.AggregatedSensor;

public class AggregationResult {

	private final AggregatedSensor aggregatedSensor;

	private final Instant timestamp;

	private final DoubleSummaryStatistics statistics;

	public AggregationResult(final AggregatedSensor sensorClass, final Instant timestamp,
			final DoubleSummaryStatistics statistics) {
		this.aggregatedSensor = sensorClass;
		this.timestamp = timestamp;
		this.statistics = statistics;
	}

	public AggregatedSensor getSensorClass() {
		return this.aggregatedSensor;
	}

	public Instant getTimestamp() {
		return this.timestamp;
	}

	public DoubleSummaryStatistics getStatistics() {
		return this.statistics;
	}

}
