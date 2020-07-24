package titan.ccp.history.streamprocessing;

import com.google.common.collect.Streams;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.kstream.TimeWindows;
import titan.ccp.history.ConfigurationKeys;

/**
 * Utility class to create {@code TimeWindowConfiguration}s.
 */
public final class TimeWindowsConfigurationsFactory {

  private TimeWindowsConfigurationsFactory() {}

  /**
   * Create a {@code List} of {@code TimeWindowsConfiguaration}s with the given configurations.
   *
   * @param config The configuration object of the program.
   * @return List of TimeWindowsConfiguarations
   */
  public static List<TimeWindowsConfiguration> createTimeWindowConfigurations(
      final Configuration config) {
    // Create list to return
    final List<TimeWindowsConfiguration> timeWindowsConfigurations =
        new LinkedList<>();

    // Get key prefixes for time windows
    final List<String> timeWindowPrefixes = getTimeWindowPrefixes(config);

    // Iterate over prefixes to create a TimeWindowsConfiguration
    for (final String timeWindowPrefix : timeWindowPrefixes) {
      final Duration duration = Duration.parse(config.getString(timeWindowPrefix + "duration"));

      final TimeWindowsConfiguration timeWindowsConfiguration = new TimeWindowsConfiguration(
          config.getString(timeWindowPrefix + "kafka"),
          config.getString(timeWindowPrefix + "cassandra"),
          config.getString(timeWindowPrefix + "api"),
          TimeWindows.of(duration));

      timeWindowsConfigurations.add(timeWindowsConfiguration);
    }

    return timeWindowsConfigurations;
  }

  /**
   * Extract the key prefixes from the configuration.
   *
   * @param config The configuration object of the program.
   * @return List of key prefixes of time windows
   */
  private static List<String> getTimeWindowPrefixes(final Configuration config) {
    // The prefix for time window keys
    final String keyPrefix = ConfigurationKeys.TIME_WINDOWS_KEY_PREFIX;

    // Pattern to get the time window key prefixes
    final Pattern pattern = Pattern.compile("(" + keyPrefix + "\\.\\w+\\.)");

    return Streams.stream(config.getKeys(keyPrefix))
        .map((final String str) -> {
          // Find match in keys
          final Matcher matcher = pattern.matcher(str);
          if (matcher.find()) {
            return matcher.group();
          } else {
            return null;
          }
        })
        .filter(Objects::nonNull) // filter out non null objects
        .distinct() // only want to get resulting keys once
        .collect(Collectors.toList()); // transform to a list
  }
}
