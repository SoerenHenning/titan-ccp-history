package titan.ccp.aggregation;

import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;

import titan.ccp.model.PowerConsumptionRecord;

public class AggregationHistory {
	private final Map<String, Integer> lastValues;

	public AggregationHistory() {
		this.lastValues = new HashMap<>();
	}

	public AggregationHistory(final Map<String, Integer> lastValues) {
		this.lastValues = new HashMap<>(lastValues);
	}

	public AggregationHistory update(final PowerConsumptionRecord powerConsumptionRecord) {
		this.lastValues.put(powerConsumptionRecord.getIdentifier(), powerConsumptionRecord.getPowerConsumptionInWh());
		return this;
	}

	public Map<String, Integer> getLastValues() {
		return Collections.unmodifiableMap(this.lastValues);
	}

	public IntSummaryStatistics getSummaryStatistics() {
		return this.lastValues.values().stream().mapToInt(v -> v).summaryStatistics();
	}
}
