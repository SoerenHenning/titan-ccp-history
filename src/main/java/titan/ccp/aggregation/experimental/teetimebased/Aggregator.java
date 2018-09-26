package titan.ccp.aggregation.experimental.teetimebased;

import java.time.Instant;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.function.Consumer;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;

public class Aggregator {

  // Create SensorRegistry by querying it from corresponding server
  private final SensorRegistry sensorRegistry; // TODO

  private final SensorHistory sensorHistory; // TODO fill map of last values (by async call to other
                                             // service)

  private final Consumer<AggregationResult> resultHandler;

  public Aggregator(final SensorRegistry sensorRegistry, final SensorHistory sensorHistory,
      final Consumer<AggregationResult> resultHandler) {
    this.sensorRegistry = sensorRegistry;
    this.sensorHistory = sensorHistory;
    this.resultHandler = resultHandler;
  }

  public void process(final ActivePowerRecord record) {

    // TODO rework this
    final String identifier = record.getIdentifier();
    final MachineSensor sensor = this.sensorRegistry.getSensorForIdentifier(identifier).get(); // TODO
                                                                                               // bad

    // Update last sensor value
    final double powerConsumption = record.getValueInW();
    final Instant time = Instant.ofEpochMilli(record.getTimestamp()); // TODO
    this.sensorHistory.update(sensor, powerConsumption, time);

    // Get all affected sensor classes
    final List<AggregatedSensor> affectedSensors = sensor.getParents();
    // Recalculate affected sensor class
    for (final AggregatedSensor affectedSensor : affectedSensors) {
      final DoubleSummaryStatistics statistics = affectedSensor.getAllChildren().stream()
          .mapToDouble(s -> this.sensorHistory.getOrZero(s)).summaryStatistics();
      // .mapToLong(s -> this.sensorHistory.getOrNull(s)).filter(Objects::nonNull)

      final AggregationResult aggregationResult =
          new AggregationResult(affectedSensor, time, statistics);

      this.resultHandler.accept(aggregationResult);
    }

  }

}
