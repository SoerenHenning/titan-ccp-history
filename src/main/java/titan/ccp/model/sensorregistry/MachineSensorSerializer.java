package titan.ccp.model.sensorregistry;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class MachineSensorSerializer implements JsonSerializer<MachineSensor> {

	@Override
	public JsonElement serialize(final MachineSensor sensor, final Type type, final JsonSerializationContext context) {
		final JsonObject jsonSensorObject = new JsonObject();
		// TODO map properties
		return jsonSensorObject;
	}

}
