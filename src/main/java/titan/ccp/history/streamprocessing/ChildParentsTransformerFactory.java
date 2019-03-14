package titan.ccp.history.streamprocessing;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import titan.ccp.configuration.events.Event;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class ChildParentsTransformerFactory {

  private static final String STORE_NAME = "CHILD-PARENTS-TRANSFORM-STATE";

  public TransformerSupplier<Event, SensorRegistry, KeyValue<String, Optional<Set<String>>>> getTransformerSupplier() {
    return new TransformerSupplier<>() {
      @Override
      public ChildParentsTransformer get() {
        return new ChildParentsTransformer(STORE_NAME);
      }
    };
  }

  public StoreBuilder<KeyValueStore<String, Set<String>>> getStoreBuilder() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STORE_NAME),
        Serdes.String(),
        ParentsSerde.serde())
        .withLoggingEnabled(Map.of());
  }

  public String getStoreName() {
    return STORE_NAME;
  }

}
