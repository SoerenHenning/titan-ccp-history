package titan.ccp.model.sensorregistry;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class SensorRegistrySerializer implements JsonSerializer<MutableSensorRegistry> {

	@Override
	public JsonElement serialize(final MutableSensorRegistry sensorRegistry, final Type type,
			final JsonSerializationContext context) {
		final JsonArray topLevelArray = new JsonArray();

		for (final Sensor sensor : sensorRegistry.getTopLevelSensors()) {
			final JsonElement jsonSensorElement = this.serializeSensor(sensor, context);
			if (jsonSensorElement != null) {
				topLevelArray.add(jsonSensorElement);
			}
		}
		return topLevelArray;
	}

	private JsonElement serializeSensor(final Sensor sensor, final JsonSerializationContext context) {
		if (sensor instanceof AggregatedSensor) {
			return context.serialize(sensor, AggregatedSensor.class);
		} else if (sensor instanceof MachineSensor) {
			return context.serialize(sensor, MachineSensor.class);
		} else {
			return null;
		}
	}

}
