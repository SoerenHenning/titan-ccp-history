package titan.ccp.aggregation.teetimebased;

import java.time.Instant;
import java.util.LongSummaryStatistics;

import titan.ccp.model.sensorregistry.AggregatedSensor;

public class AggregationResult {

	private final AggregatedSensor aggregatedSensor;
	
	private final Instant timestamp;
	
	private final LongSummaryStatistics statistics;

	public AggregationResult(AggregatedSensor sensorClass, Instant timestamp, LongSummaryStatistics statistics) {
		this.aggregatedSensor = sensorClass;
		this.timestamp = timestamp;
		this.statistics = statistics;
	}

	public AggregatedSensor getSensorClass() {
		return aggregatedSensor;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public LongSummaryStatistics getStatistics() {
		return statistics;
	}
	

	
}
