package titan.ccp.aggregation.experimental.teetimebased;

import teetime.stage.basic.AbstractTransformation;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;

public class AggregatorStage extends AbstractTransformation<ActivePowerRecord, AggregationResult> {

  private final Aggregator aggregator;

  public AggregatorStage(final SensorRegistry sensorRegistry, final SensorHistory sensorHistory) {
    this.aggregator = new Aggregator(sensorRegistry, sensorHistory, this.getOutputPort()::send);
  }

  @Override
  protected void execute(final ActivePowerRecord powerConsumptionRecord) throws Exception {
    this.aggregator.process(powerConsumptionRecord);
  }

}
