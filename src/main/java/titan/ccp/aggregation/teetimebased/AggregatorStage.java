package titan.ccp.aggregation.teetimebased;

import teetime.stage.basic.AbstractTransformation;
import titan.ccp.model.PowerConsumptionRecord;
import titan.ccp.model.sensorregistry.SensorRegistry;

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
