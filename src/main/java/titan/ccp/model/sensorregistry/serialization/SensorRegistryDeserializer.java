package titan.ccp.model.sensorregistry.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

import titan.ccp.model.sensorregistry.ImmutableSensorRegistry;
import titan.ccp.model.sensorregistry.MutableAggregatedSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

public final class SensorRegistryDeserializer implements JsonDeserializer<SensorRegistry> {

	private static final String IDENTIFIER_KEY = "identifier";

	private static final String CHILDREN_KEY = "children";

	@Override
	public SensorRegistry deserialize(final JsonElement jsonElement, final Type type,
			final JsonDeserializationContext context) throws JsonParseException {
		final MutableSensorRegistry sensorRegistry = this.transformTopLevelSensor(jsonElement);

		return ImmutableSensorRegistry.copyOf(sensorRegistry);
	}

	private MutableSensorRegistry transformTopLevelSensor(final JsonElement jsonElement) {
		final SensorParseResult parseResult = this.parseSensor(jsonElement); //
		if (parseResult == null) {
			// create empty registry
			return new MutableSensorRegistry("");
		} else {
			// create registry from result
			final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry(parseResult.identifier);
			if (parseResult.children != null) {
				for (final JsonElement childJsonElement : parseResult.children) {
					this.addSensor(childJsonElement, sensorRegistry.getTopLevelSensor());
				}
			}
			return sensorRegistry;
		}
	}

	private void addSensor(final JsonElement jsonElement, final MutableAggregatedSensor parentSensor) {
		final SensorParseResult parseResult = this.parseSensor(jsonElement);
		if (parseResult != null) {
			// create child sensor from result
			if (parseResult.children == null) {
				// create MachineSensor
				parentSensor.addChildMachineSensor(parseResult.identifier);
			} else {
				// create Aggregated Sensor
				final MutableAggregatedSensor sensor = parentSensor.addChildAggregatedSensor(parseResult.identifier);
				for (final JsonElement childJsonElement : parseResult.children) {
					this.addSensor(childJsonElement, sensor);
				}
			}
		}
	}

	// returns null if invalid JsonElement
	private SensorParseResult parseSensor(final JsonElement jsonElement) {
		if (jsonElement.isJsonObject()) {
			final JsonObject jsonObject = jsonElement.getAsJsonObject();
			if (jsonObject.has(IDENTIFIER_KEY)) {
				final JsonElement identifierJsonElement = jsonObject.get(IDENTIFIER_KEY);
				if (identifierJsonElement.isJsonPrimitive()) {
					final JsonPrimitive identfierJsonPrimitive = identifierJsonElement.getAsJsonPrimitive();
					if (identfierJsonPrimitive.isString()) {
						final String identfierString = identfierJsonPrimitive.getAsString();
						final JsonArray childrenJsonArray = this.parseChildren(jsonObject);
						return new SensorParseResult(identfierString, childrenJsonArray);
					}
				}
			}
		}
		return null;
	}

	// returns null if JsonObject does not have children or children is not an array
	private JsonArray parseChildren(final JsonObject parentJsonObject) {
		if (parentJsonObject.has(CHILDREN_KEY)) {
			final JsonElement childrenJsonElement = parentJsonObject.get(CHILDREN_KEY);
			if (childrenJsonElement.isJsonArray()) {
				final JsonArray childrenJsonArray = childrenJsonElement.getAsJsonArray();
				return childrenJsonArray;
			}
		}
		return null;
	}

	private static class SensorParseResult {
		public final String identifier;
		public final JsonArray children;

		public SensorParseResult(final String identifier, final JsonArray children) {
			this.identifier = identifier;
			this.children = children;
		}
	}

}
