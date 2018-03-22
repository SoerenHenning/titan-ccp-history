package titan.ccp.model.sensorregistry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import titan.ccp.model.sensorregistry.serialization.AggregatedSensorSerializer;
import titan.ccp.model.sensorregistry.serialization.SensorRegistrySerializer;

public class TestFactory {

	public static void main(final String[] args) {

		final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry();
		final MutableAggregatedSensor topLevel = sensorRegistry.getTopLevelSensor();
		final MutableAggregatedSensor comcent = topLevel.addChildAggregatedSensor("comcent");
		final MutableAggregatedSensor server1 = comcent.addChildAggregatedSensor("comcent.server1");
		final MachineSensor server1pw1 = server1.addChildMachineSensor("comcent.server1.pw1");
		final MachineSensor server1pw2 = server1.addChildMachineSensor("comcent.server1.pw2");
		final MachineSensor server1pw3 = server1.addChildMachineSensor("comcent.server1.pw3");
		final MutableAggregatedSensor server2 = comcent.addChildAggregatedSensor("comcent.server2");
		final MachineSensor server2pw1 = server2.addChildMachineSensor("comcent.server2.pw1");
		final MachineSensor server2pw2 = server2.addChildMachineSensor("comcent.server2.pw2");

		//

		sensorRegistry.toString();

		final ImmutableSensorRegistry immutableSensorRegistry = ImmutableSensorRegistry.copyOf(sensorRegistry);

		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(ImmutableSensorRegistry.class, new SensorRegistrySerializer())
				.registerTypeAdapter(ImmutableSensorRegistry.ImmutableAggregatatedSensor.class,
						new AggregatedSensorSerializer())
				.registerTypeAdapter(ImmutableSensorRegistry.ImmutableMachineSensor.class,
						new MachineSensorSerializer())
				.create();

		final String json1 = gson.toJson(immutableSensorRegistry);

		System.out.println(json1);

		// String json = gson.toJson(machineSensor);
		// System.out.println(json);

		//
		// final String json = "[{\"id\": \"abc\", \"name\": \"My Name\", \"children\":
		// [{},{}]},{}]";
		//
		// final JsonParser parser = new JsonParser();
		// final JsonArray array = parser.parse(json).getAsJsonArray();
		// for (final JsonElement jsonElement : array) {
		// System.out.println(jsonElement);
		// if (jsonElement.isJsonObject()) {
		// final JsonObject jsonObject = jsonElement.getAsJsonObject();
		// final JsonElement identifier = jsonObject.get("identifier");
		// if (identifier != null && identifier.isJsonPrimitive()) {
		// final String identifierAsPrimitiv = identifier.getAsString();
		//
		// }
		// }
		// }
		//
		// // parsed.toString();

	}

}
