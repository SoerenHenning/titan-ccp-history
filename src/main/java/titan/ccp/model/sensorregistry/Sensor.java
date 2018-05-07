package titan.ccp.model.sensorregistry;

import java.util.Optional;

public interface Sensor {

	public Optional<AggregatedSensor> getParent();

	public String getIdentifier();

}
