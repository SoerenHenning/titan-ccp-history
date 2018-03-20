package titan.ccp.model.sensorregistry;

import java.util.Optional;

//TODO need to handle synchronization?
public class ProxySensorRegistry implements SensorRegistry {

	private SensorRegistry backingSensorRegisty;

	public ProxySensorRegistry() {
		this.backingSensorRegisty = new DummySensorRegistry();
	}

	public ProxySensorRegistry(final SensorRegistry backingSensorRegisty) {
		this.backingSensorRegisty = backingSensorRegisty;
	}

	@Override
	public Optional<MachineSensor> getSensorForIdentifier(final String identifier) {
		return this.backingSensorRegisty.getSensorForIdentifier(identifier);
	}

	@Override
	public AggregatedSensor getTopLevelSensors() {
		return this.backingSensorRegisty.getTopLevelSensors();
	}

	public void setBackingSensorRegisty(final SensorRegistry backingSensorRegisty) {
		this.backingSensorRegisty = backingSensorRegisty;
	}

}
