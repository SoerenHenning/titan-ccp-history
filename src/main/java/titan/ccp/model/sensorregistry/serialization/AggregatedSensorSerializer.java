package titan.ccp.model.sensorregistry.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import titan.ccp.model.sensorregistry.AggregatedSensor;

public class AggregatedSensorSerializer implements JsonSerializer<AggregatedSensor> {

	@Override
	public JsonElement serialize(final AggregatedSensor sensor, final Type type,
			final JsonSerializationContext context) {
		final JsonObject jsonSensorObject = new JsonObject();
		jsonSensorObject.addProperty("identifier", sensor.getIdentifier());
		// TODO map further properties
		jsonSensorObject.add("children", context.serialize(sensor.getChildren()));
		return jsonSensorObject;
	}

}
