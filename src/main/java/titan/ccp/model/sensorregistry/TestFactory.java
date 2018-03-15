package titan.ccp.model.sensorregistry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import titan.ccp.model.sensorregistry.simple.SimpleAggregatedSensor;
import titan.ccp.model.sensorregistry.simple.SimpleMachineSensor;
import titan.ccp.model.sensorregistry.simple.SimpleSensor;

public class TestFactory {

	public static void main(String[] args) {
		
		AggregatedSensor aggregatedSensor = new AggregatedSensor();
		MachineSensor machineSensor = new MachineSensor(aggregatedSensor);
		
		Gson gson = new Gson();
		//String json = gson.toJson(machineSensor);
		//System.out.println(json);
		
		String json = "[{\"id\": \"abc\", \"name\": \"My Name\", \"children\": [{},{}]},{}]";
		
		JsonParser parser = new JsonParser();
		JsonArray array = parser.parse(json).getAsJsonArray();
		for (JsonElement jsonElement : array) {
			System.out.println(jsonElement);
			if (jsonElement.isJsonObject()) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();
				JsonElement identifier = jsonObject.get("identifier");
				if (identifier != null && identifier.isJsonPrimitive()) {
					String identifierAsPrimitiv = identifier.getAsString();
					
				}
			}
		}
		
		
		
		parsed.toString();
		
		
	}
	
}
