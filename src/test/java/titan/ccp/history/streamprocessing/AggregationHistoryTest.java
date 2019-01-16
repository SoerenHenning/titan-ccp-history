package titan.ccp.history.streamprocessing;

import static org.junit.Assert.assertEquals;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import org.junit.Test;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;

public class AggregationHistoryTest {

  private static final String CHILD_Sensor_ID_1 = "child1";
  private static final String CHILD_Sensor_ID_2 = "child2";
  private static final String CHILD_Sensor_ID_3 = "child3";

  @Test
  public void testConsecutiveUpdates() {
    final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry("top");
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_1);
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_2);
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_3);

    final AggregationHistory aggregationHistory = new AggregationHistory(sensorRegistry);
    final ActivePowerRecord recordForChild1 = new ActivePowerRecord(CHILD_Sensor_ID_1, 12345, 30);
    aggregationHistory.update(recordForChild1);
    final ActivePowerRecord recordForChild2 = new ActivePowerRecord(CHILD_Sensor_ID_2, 12346, 50);
    aggregationHistory.update(recordForChild2);
    final ActivePowerRecord recordForChild3 = new ActivePowerRecord(CHILD_Sensor_ID_3, 12347, 40);
    aggregationHistory.update(recordForChild3);

    final long timestamp = aggregationHistory.getTimestamp();
    assertEquals(12347, timestamp);
    final DoubleSummaryStatistics summaryStatistics = aggregationHistory.getSummaryStatistics();
    assertEquals(40, summaryStatistics.getAverage(), 0.01);
    assertEquals(3, summaryStatistics.getCount());
    assertEquals(50, summaryStatistics.getMax(), 0.01);
    assertEquals(30, summaryStatistics.getMin(), 0.01);
    assertEquals(120, summaryStatistics.getSum(), 0.01);
  }

  @Test
  public void testChangingSensorRegistry() {
    // Build first sensor registry
    final MutableSensorRegistry sensorRegistry = new MutableSensorRegistry("top");
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_1);
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_2);
    sensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_3);

    // Set initial history
    final AggregationHistory aggregationHistory = new AggregationHistory(sensorRegistry);
    final ActivePowerRecord recordForChild1 = new ActivePowerRecord(CHILD_Sensor_ID_1, 12345, 30);
    aggregationHistory.update(recordForChild1);
    final ActivePowerRecord recordForChild2 = new ActivePowerRecord(CHILD_Sensor_ID_2, 12346, 40);
    aggregationHistory.update(recordForChild2);
    final ActivePowerRecord recordForChild3 = new ActivePowerRecord(CHILD_Sensor_ID_3, 12347, 50);
    aggregationHistory.update(recordForChild3);

    // Serialize
    final Map<String, Double> lastValues = aggregationHistory.getLastValues();
    final long timestamp = aggregationHistory.getTimestamp();
    final int hashCode = aggregationHistory.getSensorRegistry().hashCode();

    // Build second sensor registry
    final MutableSensorRegistry changedSensorRegistry = new MutableSensorRegistry("top");
    changedSensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_1);
    changedSensorRegistry.getTopLevelSensor().addChildMachineSensor(CHILD_Sensor_ID_2);

    // Deserialize
    final AggregationHistory deserializedSensorRegistry = AggregationHistory
        .createFromRawData(changedSensorRegistry, lastValues, timestamp, hashCode);

    final DoubleSummaryStatistics summaryStatistics =
        deserializedSensorRegistry.getSummaryStatistics();
    assertEquals(35, summaryStatistics.getAverage(), 0.01);
    assertEquals(2, summaryStatistics.getCount());
    assertEquals(40, summaryStatistics.getMax(), 0.01);
    assertEquals(30, summaryStatistics.getMin(), 0.01);
    assertEquals(70, summaryStatistics.getSum(), 0.01);

  }

}
