package titan.ccp.history.streamprocessing;

import com.google.common.base.MoreObjects;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import titan.ccp.models.records.ActivePowerRecord;

public class JointFlatMapTransformer implements
    Transformer<String, Pair<Set<String>, ActivePowerRecord>, KeyValue<String, ActivePowerRecord>> {

  private final String stateStoreName;

  private ProcessorContext context;
  private KeyValueStore<String, Set<String>> state;

  public JointFlatMapTransformer(final String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    this.state = (KeyValueStore<String, Set<String>>) context.getStateStore(this.stateStoreName);
  }

  @Override
  public KeyValue<String, ActivePowerRecord> transform(final String key,
      final Pair<Set<String>, ActivePowerRecord> jointValue) {

    final ActivePowerRecord record = jointValue == null ? null : jointValue.getRight();
    final Set<String> newParents = jointValue == null ? Set.of() : jointValue.getLeft();
    final Set<String> oldParents = MoreObjects.firstNonNull(this.state.get(key), Set.of());

    for (final String parent : newParents) {
      // Forward flat mapped record
      this.forward(key, parent, record);
    }

    if (!newParents.equals(oldParents)) {
      for (final String oldParent : oldParents) {
        if (!newParents.contains(oldParent)) {
          // Forward Delete
          this.forward(key, oldParent, null);
        }
      }
      this.state.put(key, newParents);
    }

    // Flat map results forwarded before
    return null;
  }

  @Override
  public void close() {
    // Do nothing
  }



  private void forward(final String identifier, final String parent,
      final ActivePowerRecord record) {
    final String key = identifier + '#' + parent;
    this.context.forward(key, record);
  }

}
