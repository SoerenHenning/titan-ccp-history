package titan.ccp.aggregation;

import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;

import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.PowerConsumptionRecord;

public class AggregationHistory {
	private final Map<String, Integer> lastValues;
	private long timestamp;

	public AggregationHistory() {
		this.lastValues = new HashMap<>();
	}

	public AggregationHistory(final Map<String, Integer> lastValues, final long timestamp) {
		this.lastValues = new HashMap<>(lastValues);
		this.timestamp = timestamp;
	}

	public AggregationHistory update(final PowerConsumptionRecord powerConsumptionRecord) {
		this.lastValues.put(powerConsumptionRecord.getIdentifier(), powerConsumptionRecord.getPowerConsumptionInWh());
		this.timestamp = powerConsumptionRecord.getTimestamp();
		return this;
	}

	public Map<String, Integer> getLastValues() {
		return Collections.unmodifiableMap(this.lastValues);
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public IntSummaryStatistics getSummaryStatistics() {
		return this.lastValues.values().stream().mapToInt(v -> v).summaryStatistics();
	}

	public AggregatedPowerConsumptionRecord toRecord(final String identifier) {
		final IntSummaryStatistics summaryStatistics = this.getSummaryStatistics();
		return new AggregatedPowerConsumptionRecord(identifier, this.timestamp, summaryStatistics.getMin(), summaryStatistics.getMax(),
				summaryStatistics.getCount(), summaryStatistics.getSum(), summaryStatistics.getAverage());
	}
}
