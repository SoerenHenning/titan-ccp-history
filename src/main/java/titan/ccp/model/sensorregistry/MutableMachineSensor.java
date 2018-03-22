package titan.ccp.model.sensorregistry;

// Not really mutable but belongs to the others, consider renaming
public class MutableMachineSensor extends AbstractSensor implements MachineSensor {

	protected MutableMachineSensor(final AggregatedSensor parent, final String identifier) {
		super(parent, identifier);
	}

}
