package titan.ccp.aggregation.experimental.teetimebased;

import teetime.stage.basic.AbstractTransformation;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.PowerConsumptionRecord;

public class AggregatorStage extends AbstractTransformation<PowerConsumptionRecord, AggregationResult> {

	private final Aggregator aggregator;

	public AggregatorStage(final SensorRegistry sensorRegistry, final SensorHistory sensorHistory) {
		this.aggregator = new Aggregator(sensorRegistry, sensorHistory, this.getOutputPort()::send);
	}

	@Override
	protected void execute(final PowerConsumptionRecord powerConsumptionRecord) throws Exception {
		this.aggregator.process(powerConsumptionRecord);
	}

}
