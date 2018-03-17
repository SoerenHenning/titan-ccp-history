package titan.ccp.model.sensorregistry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TestFactory {

	public static void main(final String[] args) {

		final AggregatedSensor aggregatedSensor = new AggregatedSensor();
		final MachineSensor machineSensor = new MachineSensor(1l, aggregatedSensor);
		final SensorRegistry sensorRegistry = new MutableSensorRegistry();

		//

		final Gson gson = new Gson();
		// String json = gson.toJson(machineSensor);
		// System.out.println(json);

		final String json = "[{\"id\": \"abc\", \"name\": \"My Name\", \"children\": [{},{}]},{}]";

		final JsonParser parser = new JsonParser();
		final JsonArray array = parser.parse(json).getAsJsonArray();
		for (final JsonElement jsonElement : array) {
			System.out.println(jsonElement);
			if (jsonElement.isJsonObject()) {
				final JsonObject jsonObject = jsonElement.getAsJsonObject();
				final JsonElement identifier = jsonObject.get("identifier");
				if (identifier != null && identifier.isJsonPrimitive()) {
					final String identifierAsPrimitiv = identifier.getAsString();

				}
			}
		}

		// parsed.toString();

	}

}
