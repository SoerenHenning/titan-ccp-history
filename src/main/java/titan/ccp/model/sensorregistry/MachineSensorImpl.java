package titan.ccp.model.sensorregistry;

public class MachineSensorImpl extends AbstractSensor implements MachineSensor {

	protected MachineSensorImpl(final String identifier) {
		super(identifier);
	}

	protected MachineSensorImpl(final String identifier, final AggregatedSensorImpl parent) {
		super(identifier, parent);
	}

}
