package titan.ccp.history.streamprocessing;

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
import titan.ccp.model.sensorregistry.client.SensorRegistryRequester;

/**
 * Factory class configuration required by {@link ChildParentsTransformer}.
 */
public class ChildParentsTransformerFactory {

  private static final String STORE_NAME = "CHILD-PARENTS-TRANSFORM-STATE";

  private final SensorRegistryRequester registryRequester;

  public ChildParentsTransformerFactory(final SensorRegistryRequester registryRequester) {
    this.registryRequester = registryRequester;
  }

  /**
   * Returns a {@link TransformerSupplier} for {@link ChildParentsTransformer}.
   */
  public TransformerSupplier<Event, SensorRegistry, KeyValue<String, Optional<Set<String>>>> getTransformerSupplier() { // NOCS
    return new TransformerSupplier<>() {
      @Override
      public ChildParentsTransformer get() {
        return new ChildParentsTransformer(STORE_NAME,
            ChildParentsTransformerFactory.this.registryRequester);
      }
    };
  }

  /**
   * Returns a {@link StoreBuilder} for {@link ChildParentsTransformer}.
   */
  public StoreBuilder<KeyValueStore<String, Set<String>>> getStoreBuilder() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STORE_NAME),
        Serdes.String(),
        ParentsSerde.serde())
        .withLoggingDisabled();
    // .withLoggingEnabled(Map.of());
  }

  /**
   * Returns the store name for {@link ChildParentsTransformer}.
   */
  public String getStoreName() {
    return STORE_NAME;
  }

}
