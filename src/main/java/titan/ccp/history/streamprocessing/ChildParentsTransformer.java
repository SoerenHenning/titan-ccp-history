package titan.ccp.history.streamprocessing;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.configuration.events.Event;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.client.SensorRegistryRequester;

/**
 * Transforms a {@link SensorRegistry} into key value pairs of Sensor identifiers and their parents'
 * sensor identifiers. All pairs whose sensor's parents have changed since last iteration are
 * forwarded. A mapping of an identifier to <code>null</code> means that the corresponding sensor
 * does not longer exists in the sensor registry.
 */
public class ChildParentsTransformer implements
    Transformer<Event, SensorRegistry, KeyValue<String, Optional<Set<String>>>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChildParentsTransformer.class);

  private final String stateStoreName;
  private final SensorRegistryRequester registryRequester;

  private ProcessorContext context;
  private KeyValueStore<String, Set<String>> state;

  private Cancellable initialRegistryRequester;

  public ChildParentsTransformer(final String stateStoreName,
      final SensorRegistryRequester registryRequester) {
    this.stateStoreName = stateStoreName;
    this.registryRequester = registryRequester;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    this.state = (KeyValueStore<String, Set<String>>) context.getStateStore(this.stateStoreName);

    this.initialRegistryRequester =
        context.schedule(Duration.ofMillis(0), PunctuationType.WALL_CLOCK_TIME, t -> {
          // Currently block until sensor registry is available. This can later be changed to
          // request
          // the registry asynchronously and to abort the request when a new change event arrives
          // before.
          // However, we have to be careful with starting, stopping and moving the application. In
          // particular, aborting the request also on close would be required.
          LOGGER.info("Request sensor registry");
          final SensorRegistry registry = this.registryRequester.request().join();
          final Map<String, Set<String>> childParentsPairs =
              this.constructChildParentsPairs(registry);
          this.updateState(childParentsPairs);
          this.forwardChildParentsPairs(childParentsPairs);
          this.initialRegistryRequester.cancel();
        });
  }

  @Override
  public KeyValue<String, Optional<Set<String>>> transform(final Event event,
      final SensorRegistry registry) {

    // Values may later be null for deleting a sensor
    final Map<String, Set<String>> childParentsPairs = this.constructChildParentsPairs(registry);

    this.updateChildParentsPairs(childParentsPairs);

    this.updateState(childParentsPairs);

    this.forwardChildParentsPairs(childParentsPairs);

    // Flat map results forwarded before
    return null;
  }

  @Override
  public void close() {
    // Do nothing
  }

  private Map<String, Set<String>> constructChildParentsPairs(final SensorRegistry registry) {
    return registry
        .getMachineSensors()
        .stream()
        .collect(Collectors.toMap(
            child -> child.getIdentifier(),
            child -> child.getParents()
                .stream()
                .map(Sensor::getIdentifier)
                .collect(Collectors.toSet())));
  }

  private void updateChildParentsPairs(final Map<String, Set<String>> childParentsPairs) {
    final KeyValueIterator<String, Set<String>> oldChildParentsPairs = this.state.all();
    while (oldChildParentsPairs.hasNext()) {
      final KeyValue<String, Set<String>> oldChildParentPair = oldChildParentsPairs.next();
      final String identifier = oldChildParentPair.key;
      final Set<String> oldParents = oldChildParentPair.value;
      final Set<String> newParents = childParentsPairs.get(identifier); // null if not exists
      if (newParents == null) {
        // Sensor was deleted
        childParentsPairs.put(identifier, null);
      } else if (newParents.equals(oldParents)) {
        // No changes
        childParentsPairs.remove(identifier);
      }
      // Else: Later Perhaps: Mark changed parents
    }
    oldChildParentsPairs.close();
  }

  private void updateState(final Map<String, Set<String>> childParentsPairs) {
    for (final Map.Entry<String, Set<String>> childParentPair : childParentsPairs.entrySet()) {
      if (childParentPair.getValue() == null) {
        this.state.delete(childParentPair.getKey());
      } else {
        this.state.put(childParentPair.getKey(), childParentPair.getValue());
      }
    }
  }

  private void forwardChildParentsPairs(final Map<String, Set<String>> childParentsPairs) {
    for (final Map.Entry<String, Set<String>> childParentPair : childParentsPairs.entrySet()) {
      this.context.forward(childParentPair.getKey(),
          Optional.ofNullable(childParentPair.getValue()));
    }
  }


}
