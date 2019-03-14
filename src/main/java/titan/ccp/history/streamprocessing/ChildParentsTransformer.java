package titan.ccp.history.streamprocessing;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import titan.ccp.configuration.events.Event;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class ChildParentsTransformer implements
    Transformer<Event, SensorRegistry, KeyValue<String, Optional<Set<String>>>> {

  private final String stateStoreName;

  private ProcessorContext context;
  private KeyValueStore<String, Set<String>> state;

  public ChildParentsTransformer(final String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    this.state = (KeyValueStore<String, Set<String>>) context.getStateStore(this.stateStoreName);
  }

  @Override
  public KeyValue<String, Optional<Set<String>>> transform(final Event event,
      final SensorRegistry registry) {

    // Values may be null for deleting a sensor
    final Map<String, Set<String>> childParentsPairs = registry
        .getMachineSensors()
        .stream()
        .collect(Collectors.toMap(
            child -> child.getIdentifier(),
            child -> child.getParents()
                .stream()
                .map(Sensor::getIdentifier)
                .collect(Collectors.toSet())));


    // Old parents
    final KeyValueIterator<String, Set<String>> oldChildParentsPairs = this.state.all();
    while (oldChildParentsPairs.hasNext()) {
      final KeyValue<String, Set<String>> oldChildParentPair = oldChildParentsPairs.next();
      final String identifier = oldChildParentPair.key;
      final Set<String> oldParents = oldChildParentPair.value;
      final Set<String> newParents = childParentsPairs.get(identifier);
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

    // update state
    for (final Map.Entry<String, Set<String>> childParentPair : childParentsPairs.entrySet()) {
      if (childParentPair.getValue() == null) {
        this.state.delete(childParentPair.getKey());
      } else {
        this.state.put(childParentPair.getKey(), childParentPair.getValue());
      }
    }

    // forward
    for (final Map.Entry<String, Set<String>> childParentPair : childParentsPairs.entrySet()) {
      this.context.forward(childParentPair.getKey(),
          Optional.ofNullable(childParentPair.getValue()));
    }

    // Flat map results forwarded before
    return null;
  }

  @Override
  public void close() {
    // Do nothing
  }

}
