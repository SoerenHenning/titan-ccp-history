package titan.ccp.model.sensorregistry.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import titan.ccp.model.sensorregistry.MutableAggregatedSensor;
import titan.ccp.model.sensorregistry.MutableMachineSensor;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

public final class SensorRegistrySerializer implements JsonSerializer<SensorRegistry> {

	@Override
	public JsonElement serialize(final SensorRegistry sensorRegistry, final Type type,
			final JsonSerializationContext context) {

		// return new JsonArray();
		return context.serialize(sensorRegistry.getTopLevelSensor());

		/*
		 * for (final Sensor sensor : sensorRegistry.getTopLevelSensors()) { final
		 * JsonElement jsonSensorElement = this.serializeSensor(sensor, context); if
		 * (jsonSensorElement != null) { topLevelArray.add(jsonSensorElement); } }
		 */
		// return topLevelArray;
	}

	private JsonElement serializeSensor(final Sensor sensor, final JsonSerializationContext context) {
		if (sensor instanceof MutableAggregatedSensor) {
			return context.serialize(sensor, MutableAggregatedSensor.class);
		} else if (sensor instanceof MutableMachineSensor) {
			return context.serialize(sensor, MutableMachineSensor.class);
		} else {
			return null;
		}
	}

}
