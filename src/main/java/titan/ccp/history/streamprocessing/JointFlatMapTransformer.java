package titan.ccp.history.streamprocessing;

import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import titan.ccp.models.records.ActivePowerRecord;

public class JointFlatMapTransformer implements
    Transformer<String, Pair<String, ActivePowerRecord>, KeyValue<String, ActivePowerRecord>> {

  private final String stateStoreName;

  private ProcessorContext context;
  private KeyValueStore<String, String> state;

  public JointFlatMapTransformer(final String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    this.state = (KeyValueStore<String, String>) context.getStateStore(this.stateStoreName);
  }

  @Override
  public KeyValue<String, ActivePowerRecord> transform(final String key,
      final Pair<String, ActivePowerRecord> jointValue) {

    final String parentsString = jointValue.getLeft();
    final ActivePowerRecord record = jointValue.getRight();

    final String oldParentsString = this.state.get(key);

    // TODO Instead deserialize
    final Set<String> newParents =
        parentsString == null ? Set.of() : Set.of(parentsString.split(";"));
    final Set<String> oldParents =
        oldParentsString == null ? Set.of() : Set.of(oldParentsString.split(";"));

    for (final String parent : newParents) {
      this.forward(record, parent);
    }

    if (!newParents.equals(oldParents)) {
      for (final String oldParent : oldParents) {
        if (!newParents.contains(oldParent)) {
          this.forward(record, oldParent);
        }
      }
      this.state.put(key, parentsString); // TODO instead serialize
    }

    // Flat map results forwarded before
    return null;
  }

  @Override
  public void close() {
    // Do nothing
  }

  private void forward(final ActivePowerRecord record, final String parent) {
    final String key = record.getIdentifier() + '#' + parent;
    this.context.forward(key, record);
  }

}
