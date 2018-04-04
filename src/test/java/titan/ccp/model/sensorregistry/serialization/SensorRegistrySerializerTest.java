package titan.ccp.model.sensorregistry.serialization;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import titan.ccp.model.sensorregistry.MutableAggregatedSensor;
import titan.ccp.model.sensorregistry.MutableMachineSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;

public class SensorRegistrySerializerTest {

	private Gson gson;

	@Before
	public void setUp() throws Exception {
		this.gson = new GsonBuilder().registerTypeAdapter(MutableSensorRegistry.class, new SensorRegistrySerializer())
				.registerTypeAdapter(MutableAggregatedSensor.class, new AggregatedSensorSerializer())
				.registerTypeAdapter(MutableMachineSensor.class, new MachineSensorSerializer()).create();
	}

	@After
	public void tearDown() throws Exception {
		this.gson = null;
	}

	@Test
	public void testEmptySensorRegistry() {
		final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry("root");

		final String json = this.gson.toJson(sensorRegistry);
		assertEquals(json, "{\"identifier\":\"root\",\"children\":[]}");
	}

	@Test
	public void testEmptySensorRegistryWithChildren() {
		final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry("root");
		final MutableAggregatedSensor topLevel = sensorRegistry.getTopLevelSensor();
		topLevel.addChildMachineSensor("child-1");
		topLevel.addChildMachineSensor("child-2");

		final String json = this.gson.toJson(sensorRegistry);
		assertEquals(json,
				"{\"identifier\":\"root\",\"children\":[{\"identifier\":\"child-1\"},{\"identifier\":\"child-2\"}]}");
	}

	@Test
	public void testEmptySensorRegistryWithGrandChildren() {
		final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry("root");
		final MutableAggregatedSensor topLevel = sensorRegistry.getTopLevelSensor();
		final MutableAggregatedSensor aggregatedSensor = topLevel.addChildAggregatedSensor("child-1");
		aggregatedSensor.addChildMachineSensor("child-1-1");
		aggregatedSensor.addChildMachineSensor("child-1-2");
		topLevel.addChildMachineSensor("child-2");

		final String json = this.gson.toJson(sensorRegistry);
		assertEquals(json,
				"{\"identifier\":\"root\",\"children\":[{\"identifier\":\"child-1\",\"children\":[{\"identifier\":\"child-1-1\"},{\"identifier\":\"child-1-2\"}]},{\"identifier\":\"child-2\"}]}");
	}

}
