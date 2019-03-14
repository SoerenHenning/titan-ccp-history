package titan.ccp.history.streamprocessing;

import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

public class RecordAggregator {

  public AggregatedActivePowerRecord add(final String identifier,
      final ActivePowerRecord record, final AggregatedActivePowerRecord aggregated) {
    final long count = (aggregated == null ? 0 : aggregated.getCount()) + 1;
    final double sum = (aggregated == null ? 0.0 : aggregated.getSumInW()) + record.getValueInW();
    return new AggregatedActivePowerRecord(
        identifier, record.getTimestamp(),
        0.0, 0.0, count, sum, sum / count);
  }

  public AggregatedActivePowerRecord substract(final String identifier,
      final ActivePowerRecord record, final AggregatedActivePowerRecord aggregated) {
    final long count = aggregated.getCount() - 1;
    final double sum = aggregated.getSumInW() - record.getValueInW();
    return new AggregatedActivePowerRecord(
        identifier, aggregated.getTimestamp(),
        0.0, 0.0, count, sum, sum / count);
  }

}
