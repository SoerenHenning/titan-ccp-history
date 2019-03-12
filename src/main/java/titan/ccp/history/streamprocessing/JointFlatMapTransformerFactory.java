package titan.ccp.history.streamprocessing;

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import titan.ccp.models.records.ActivePowerRecord;

public class JointFlatMapTransformerFactory {

  private static final String STORE_NAME = "JOINT-FLAT-MAP-TRANSFORM-STATE";

  public TransformerSupplier<String, Pair<String, ActivePowerRecord>, KeyValue<String, ActivePowerRecord>> getTransformerSupplier() {
    return new TransformerSupplier<>() {
      @Override
      public JointFlatMapTransformer get() {
        return new JointFlatMapTransformer(STORE_NAME);
      }
    };
  }

  public StoreBuilder<KeyValueStore<String, String>> getStoreBuilder() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STORE_NAME),
        Serdes.String(),
        Serdes.String())
        .withLoggingEnabled(Map.of());
  }

  public String getStoreName() {
    return STORE_NAME;
  }

}
