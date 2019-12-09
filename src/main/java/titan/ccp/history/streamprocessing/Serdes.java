package titan.ccp.history.streamprocessing;

import com.google.common.math.Stats;
import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.common.kafka.GenericSerde;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.model.records.WindowedActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

final class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;
  // private final SensorRegistry sensorRegistry;

  // public Serdes(final String schemaRegistryUrl, final SensorRegistry sensorRegistry) {
  // this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  // this.sensorRegistry = sensorRegistry;
  // }
  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<ActivePowerRecord> activePower() {
    return IMonitoringRecordSerde.serde(new ActivePowerRecordFactory());
  }

  public Serde<AggregatedActivePowerRecord> aggrActivePower() {
    return IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory());
  }

  public Serde<WindowedActivePowerRecord> windowedActivePowerValues() {
    return this.avroSerdeFactory.forKeys();
  }

  public Serde<WindowedActivePowerRecord> windowedActivePowerKeys() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<titan.ccp.model.records.AggregatedActivePowerRecord> aggregatedActivePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<Stats> stats() {
    return GenericSerde.from(Stats::toByteArray, Stats::fromByteArray);
  }

  // public Serde<AggregationHistory> aggregationHistory() {
  // return AggregationHistorySerde.serde(this.sensorRegistry);
  // }


}
