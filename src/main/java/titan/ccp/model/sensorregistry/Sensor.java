package titan.ccp.model.sensorregistry;

import java.util.Optional;

public interface Sensor {

	public Optional<AggregatedSensorImpl> getParent();

	public String getIdentifier();
}
