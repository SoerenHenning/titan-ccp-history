package titan.ccp.history.streamprocessing;

import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.records.WindowedActivePowerRecord;


/**
 * Updates an {@link AggregatedActivePowerRecord} by a new {@link ActivePowerRecord}.
 */
public class RecordAggregator {

  /**
   * Adds an {@link ActivePowerRecord} to an {@link AggregatedActivePowerRecord}.
   */
  public WindowedActivePowerRecord add(final String key, final ActivePowerRecord record,
      final WindowedActivePowerRecord aggregated) {

    final long timestamp = record.getTimestamp();
    final double valueInW = record.getValueInW();

    final String identifier =
        aggregated == null ? record.getIdentifier() : aggregated.getIdentifier();
    final long startTimestamp = aggregated == null ? timestamp
        : Math.min(aggregated.getStartTimestamp(), timestamp);
    final long endTimestamp = aggregated == null ? timestamp
        : Math.max(aggregated.getStartTimestamp(), timestamp);
    final long count = (aggregated == null ? 0 : aggregated.getCount()) + 1;
    // final double mean = count == 0 ? 0.0 : (aggregated.getMean() + valueInW) / count;
    final double mean = 0.0;
    final double populationVariance = aggregated == null ? 0.0 : 0.0;
    final double min = aggregated == null ? valueInW : Math.min(aggregated.getMin(), valueInW);
    final double max = aggregated == null ? valueInW : Math.max(aggregated.getMin(), valueInW);

    return new WindowedActivePowerRecord(identifier, startTimestamp, endTimestamp, count, mean,
        populationVariance, min, max);
  }

}
