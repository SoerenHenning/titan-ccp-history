package titan.ccp.aggregation.streamprocessing;

import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;

import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

public class AggregationHistory {
	private final Map<String, Double> lastValues;
	private long timestamp;

	public AggregationHistory() {
		this.lastValues = new HashMap<>();
	}

	public AggregationHistory(final Map<String, Double> lastValues, final long timestamp) {
		this.lastValues = new HashMap<>(lastValues);
		this.timestamp = timestamp;
	}

	public AggregationHistory update(final ActivePowerRecord activePowerRecord) {
		this.lastValues.put(activePowerRecord.getIdentifier(), activePowerRecord.getValueInW());
		this.timestamp = activePowerRecord.getTimestamp();
		return this;
	}

	public Map<String, Double> getLastValues() {
		return Collections.unmodifiableMap(this.lastValues);
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public DoubleSummaryStatistics getSummaryStatistics() {
		return this.lastValues.values().stream().mapToDouble(v -> v).summaryStatistics();
	}

	public AggregatedActivePowerRecord toRecord(final String identifier) {
		final DoubleSummaryStatistics summaryStatistics = this.getSummaryStatistics();
		return new AggregatedActivePowerRecord(identifier, this.timestamp, summaryStatistics.getMin(),
				summaryStatistics.getMax(), summaryStatistics.getCount(), summaryStatistics.getSum(),
				summaryStatistics.getAverage());
	}

	@Override
	public String toString() {
		return this.timestamp + ": " + this.lastValues;
	}
}
