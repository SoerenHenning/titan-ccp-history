package titan.ccp.history.streamprocessing;

import com.google.common.math.Stats;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import titan.ccp.model.records.WindowedActivePowerRecord;

/**
 * Factory to create an {@link WindowedActivePowerRecord}.
 */
public final class WindowedActivePowerRecordFactory {

  private WindowedActivePowerRecordFactory() {

  }

  /**
   * Method to create a {@link WindowedActivePowerRecord}.
   *
   * @param windowedKey The window with the key.
   * @param stats Stats used for the record class.
   * @return
   */
  public static WindowedActivePowerRecord create(final Windowed<String> windowedKey,
      final Stats stats) {

    final Window window = windowedKey.window();

    return new WindowedActivePowerRecord(
        windowedKey.key(),
        window.start(),
        window.end(),
        stats.count(),
        stats.mean(),
        stats.populationVariance(),
        stats.min(),
        stats.max());
  }

}
