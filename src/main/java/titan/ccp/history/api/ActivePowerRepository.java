package titan.ccp.history.api;

import java.util.List;

/**
 * An interface for encapsulating the data storage from queries to it.
 *
 * @param <T> type of records in this repository
 */
public interface ActivePowerRepository<T> {

  /**
   * Get all records for the provided identifier in the provided time interval.
   */
  List<T> get(String identifier, TimeRestriction timeRestriction);

  /**
   * Get the latests records.
   */
  List<T> getLatest(String identifier, TimeRestriction timeRestriction, int count);

  /**
   * Get the earliest records.
   */
  List<T> getEarliest(String identifier, TimeRestriction timeRestriction, int count);

  /**
   * Compute a trend for the selected records, i.e., a value showing how the values increased or
   * decreased over time.
   */
  double getTrend(String identifier, final TimeRestriction timeRestriction, int pointsToSmooth);

  /**
   * Get a frequency distribution of records. Records are grouped by their values and this methods
   * returns a list of {@link DistributionBucket}s.
   */
  List<DistributionBucket> getDistribution(String identifier, TimeRestriction timeRestriction,
      int bucketsCount);

  /**
   * Get the total amount of all records.
   */
  long getTotalCount();

  /**
   * Get the number of records for the given sensor identifier and after a timestamp.
   */
  long getCount(String identifier, TimeRestriction timeRestriction);

  /**
   * Get all available sensor identifiers.
   */
  List<String> getIdentifiers();

}
