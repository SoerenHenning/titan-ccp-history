package titan.ccp.history.streamprocessing;

import com.google.common.base.MoreObjects;
import java.util.Set;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import titan.ccp.models.records.ActivePowerRecord;

/**
 * Transforms the join result of an {@link ActivePowerRecord} and the corresponding sensor parents
 * to multiple of this {@link ActivePowerRecord} keyed by all sensor parents.
 */
public class JointFlatTransformer implements
    Transformer<String, JointRecordParents, KeyValue<SensorParentKey, ActivePowerRecord>> {

  private final String stateStoreName;

  private ProcessorContext context;
  private KeyValueStore<String, Set<String>> state;

  public JointFlatTransformer(final String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    this.state = (KeyValueStore<String, Set<String>>) context.getStateStore(this.stateStoreName);
  }

  @Override
  public KeyValue<SensorParentKey, ActivePowerRecord> transform(final String identifier,
      final JointRecordParents jointValue) {

    final ActivePowerRecord record = jointValue == null ? null : jointValue.getRecord();
    final Set<String> newParents = jointValue == null ? Set.of() : jointValue.getParents();
    final Set<String> oldParents = MoreObjects.firstNonNull(this.state.get(identifier), Set.of());

    for (final String parent : newParents) {
      // Forward flat mapped record
      this.forward(identifier, parent, record);
    }

    if (!newParents.equals(oldParents)) {
      for (final String oldParent : oldParents) {
        if (!newParents.contains(oldParent)) {
          // Forward Delete
          this.forward(identifier, oldParent, null);
        }
      }
      this.state.put(identifier, newParents);
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
    final SensorParentKey key = new SensorParentKey(identifier, parent);
    this.context.forward(key, record);
  }

}
