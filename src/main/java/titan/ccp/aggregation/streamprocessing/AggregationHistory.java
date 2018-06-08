package titan.ccp.aggregation.streamprocessing;

import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;

import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.PowerConsumptionRecord;

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

	public AggregationHistory update(final PowerConsumptionRecord powerConsumptionRecord) {
		this.lastValues.put(powerConsumptionRecord.getIdentifier(),
				(double) powerConsumptionRecord.getPowerConsumptionInWh());
		this.timestamp = powerConsumptionRecord.getTimestamp();
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

	public AggregatedPowerConsumptionRecord toRecord(final String identifier) {
		final DoubleSummaryStatistics summaryStatistics = this.getSummaryStatistics();
		// TODO change to AggregatedActivePowerRecord
		return new AggregatedPowerConsumptionRecord(identifier, this.timestamp, (int) summaryStatistics.getMin(),
				(int) summaryStatistics.getMax(), summaryStatistics.getCount(), (long) summaryStatistics.getSum(),
				summaryStatistics.getAverage());
	}

	@Override
	public String toString() {
		return this.timestamp + ": " + this.lastValues;
	}
}
