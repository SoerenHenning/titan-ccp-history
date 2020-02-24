package titan.ccp.history.api;

import java.util.List;

/**
 * An interface for encapsulating the data storage from queries to it.
 *
 * @param <T> type of records in this repository
 */
public interface ActivePowerRepository<T> {

  /**
   * Get all selected records.
   */
  List<T> getRange(String identifier, long from, long to);

  /**
   * Get all selected records.
   */
  List<T> get(String identifier, long from);

  /**
   * Get the latests records.
   */
  List<T> getLatest(String identifier, int count);

  /**
   * Get the latests records.
   */
  List<T> getLatestBeforeTo(String identifier, int count, long to);

  /**
   * Compute a trend for the selected records, i.e., a value showing how the values increased or
   * decreased over time.
   */

  double getTrend(String identifier, long from, int pointsToSmooth,
      long to);

  /**
   * Get a frequency distribution of records. Records are grouped by their values and this methods
   * returns a list of {@link DistributionBucket}s.
   */
  List<DistributionBucket> getDistribution(String identifier, long from,
      long to, int bucketsCount);

  /**
   * Get the total amount of all records.
   */
  long getTotalCount();

  /**
   * Get the number of records for the given sensor identifier and after a timestamp.
   */
  long getCount(String identifier, long from, long to);

  /**
   * Get all available sensor identifiers.
   */
  List<String> getIdentifiers();

}
