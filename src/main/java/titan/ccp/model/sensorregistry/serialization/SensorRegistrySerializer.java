package titan.ccp.model.sensorregistry.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import titan.ccp.model.sensorregistry.SensorRegistry;

public final class SensorRegistrySerializer implements JsonSerializer<SensorRegistry> {

	@Override
	public JsonElement serialize(final SensorRegistry sensorRegistry, final Type type,
			final JsonSerializationContext context) {
		return context.serialize(sensorRegistry.getTopLevelSensor());
	}

}
