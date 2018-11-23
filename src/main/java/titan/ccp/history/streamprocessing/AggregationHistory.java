package titan.ccp.history.streamprocessing;

import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

/**
 * Manages the last value for an aggregation, i.e., a set of different sensors.
 */
public class AggregationHistory {
  private final Map<String, Double> lastValues;
  private long timestamp;
  private final SensorRegistry sensorRegistry;

  public AggregationHistory(final SensorRegistry sensorRegistry) {
    this.sensorRegistry = sensorRegistry;
    this.lastValues = new HashMap<>();
  }

  private AggregationHistory(final SensorRegistry sensorRegistry,
      final Map<String, Double> lastValues, final long timestamp) {
    this.sensorRegistry = sensorRegistry;
    this.lastValues = new HashMap<>(lastValues);
    this.timestamp = timestamp;
  }

  /**
   * Update the associated last value of a sensor with the passed new record.
   */
  public AggregationHistory update(final ActivePowerRecord activePowerRecord) {
    if (this.sensorRegistry.getSensorForIdentifier(activePowerRecord.getIdentifier()).isPresent()) {
      this.lastValues.put(activePowerRecord.getIdentifier(), activePowerRecord.getValueInW());
      this.timestamp = activePowerRecord.getTimestamp();
    }
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

  /**
   * Converts this {@link AggregationHistory} into an {@link AggregatedActivePowerRecord}.
   */
  public AggregatedActivePowerRecord toRecord(final String identifier) {
    final DoubleSummaryStatistics summaryStatistics = this.getSummaryStatistics();
    return new AggregatedActivePowerRecord(identifier, this.timestamp, summaryStatistics.getMin(),
        summaryStatistics.getMax(), summaryStatistics.getCount(), summaryStatistics.getSum(),
        summaryStatistics.getAverage());
  }

  public SensorRegistry getSensorRegistry() {
    return this.sensorRegistry;
  }

  @Override
  public String toString() {
    return this.timestamp + ": " + this.lastValues;
  }

  /**
   * Create an {@link AggregationHistory} from its raw values using the additional hash of an old
   * sensor registry.
   *
   * @param sensorRegistry The associated {@link SensorRegistry} for the {@link AggregationHistory}.
   * @param lastValues Map of last measured values keyed by the sensor's identifier.
   * @param timestamp Timestamp of the last measured value.
   * @param oldSensorRegistryHash Hash of a previous version of the {@link AggregationHistory}.
   * @return
   */
  public static final AggregationHistory createFromRawData(final SensorRegistry sensorRegistry,
      final Map<String, Double> lastValues, final long timestamp, final int oldSensorRegistryHash) {
    Map<String, Double> lastValuesFiltered = lastValues;
    // We expect that the sensor registry has not changed if the hash code remains unchanged.
    if (sensorRegistry.hashCode() != oldSensorRegistryHash) {
      lastValuesFiltered = lastValues.entrySet().stream()
          .filter(x -> sensorRegistry.getSensorForIdentifier(x.getKey()).isPresent())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    return new AggregationHistory(sensorRegistry, lastValuesFiltered, timestamp);
  }

}
