package titan.ccp.model.sensorregistry;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class MachineSensorSerializer implements JsonSerializer<MutableMachineSensor> {

	@Override
	public JsonElement serialize(final MutableMachineSensor sensor, final Type type, final JsonSerializationContext context) {
		final JsonObject jsonSensorObject = new JsonObject();
		// TODO map properties
		return jsonSensorObject;
	}

}
