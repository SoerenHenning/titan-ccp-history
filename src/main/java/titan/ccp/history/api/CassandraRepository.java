package titan.ccp.history.api;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.cassandra.CassandraReader;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;

/**
 * An {@link ActivePowerRepository} for the Cassandra data storage.
 *
 * @param <T> type of records in this repository
 */
public class CassandraRepository<T> implements ActivePowerRepository<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRepository.class);

  private static final String TIMESTAMP_KEY = "timestamp";
  private static final String IDENTIFIER_KEY = "identifier";

  private final Session cassandraSession;
  private final String tableName;
  private final Function<Row, T> recordFactory;
  private final ToDoubleFunction<T> valueAccessor;

  /**
   * Create a new {@link CassandraRepository}.
   */
  public CassandraRepository(final Session cassandraSession, final String tableName,
      final Function<Row, T> recordFactory, final ToDoubleFunction<T> valueAccessor) {
    this.cassandraSession = cassandraSession;
    this.tableName = tableName;
    this.recordFactory = recordFactory;
    this.valueAccessor = valueAccessor;
  }

  @Override
  public List<T> get(final String identifier, final TimeRestriction timeRestriction) {
    final Statement statement =
        this.buildRestrictedSelectAllBaseStatement(identifier, timeRestriction);
    return this.executeStatement(statement);
  }

  @Override
  public List<T> getLatest(final String identifier, final TimeRestriction timeRestriction,
      final int count) {
    final Statement statement =
        this.buildRestrictedSelectAllBaseStatement(identifier, timeRestriction)
            .orderBy(QueryBuilder.desc(TIMESTAMP_KEY))
            .limit(count);

    return this.executeStatement(statement);
  }

  @Override
  public List<T> getEarliest(final String identifier, final TimeRestriction timeRestriction,
      final int count) {
    final Statement statement =
        this.buildRestrictedSelectAllBaseStatement(identifier, timeRestriction)
            .orderBy(QueryBuilder.asc(TIMESTAMP_KEY))
            .limit(count);

    return this.executeStatement(statement);
  }

  @Override
  public double getTrend(final String identifier, final TimeRestriction timeRestriction,
      final int pointsToSmooth) {
    // TODO Could be a default implementation of the interface
    final List<T> earliest =
        this.getEarliest(identifier, timeRestriction, pointsToSmooth);
    final List<T> latest =
        this.getLatest(identifier, timeRestriction, pointsToSmooth);

    final OptionalDouble start = earliest.stream().mapToDouble(this.valueAccessor).average();
    final OptionalDouble end = latest.stream().mapToDouble(this.valueAccessor).average();

    if (start.isPresent() && end.isPresent()) {
      return start.getAsDouble() > 0.0 ? end.getAsDouble() / start.getAsDouble() : 1;
    } else { // NOPMD
      LOGGER.warn(
          "Trend could not be computed for interval={} and pointsToSmooth={}. Getting start={} and end={}.", // NOCS_NOPMD
          timeRestriction, pointsToSmooth, start, end);
      return -1;
    }

  }

  @Override
  public List<DistributionBucket> getDistribution(final String identifier,
      final TimeRestriction timeRestriction, final int bucketsCount) {
    // TODO Could be a default implementation of the interface
    final List<T> records = this.get(identifier, timeRestriction);

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    final double min = records.stream().mapToDouble(this.valueAccessor).min().getAsDouble();
    final double max = records.stream().mapToDouble(this.valueAccessor).max().getAsDouble();

    final double sliceSize = (max - min) / bucketsCount;

    final int[] distribution = new int[bucketsCount];
    for (final T record : records) {
      final double value = this.valueAccessor.applyAsDouble(record);
      final int index = Integer.min((int) ((value - min) / sliceSize), bucketsCount - 1);
      distribution[index]++;
    }

    final List<DistributionBucket> buckets = new ArrayList<>(bucketsCount);
    for (int i = 0; i < bucketsCount; i++) {
      final double lower = i > 0 ? buckets.get(i - 1).getUpper() : min;
      final double upper = i < bucketsCount ? lower + sliceSize : max;
      buckets.add(new DistributionBucket(lower, upper, distribution[i])); // NOPMD
    }

    return buckets;
  }

  @Override
  public long getTotalCount() {
    // TODO This is not working for huge data sets
    final Statement statement = QueryBuilder.select().countAll().from(this.tableName);
    return this.cassandraSession.execute(statement).all().get(0).getLong(0);
  }

  @Override
  public long getCount(final String identifier, final TimeRestriction timeRestriction) {
    final Statement statement = QueryBuilder.select()
        .countAll()
        .from(this.tableName)
        .where(QueryBuilder.eq(IDENTIFIER_KEY, identifier))
        .and(QueryBuilder.gte(TIMESTAMP_KEY, timeRestriction.getFromOrDefault(Long.MIN_VALUE)))
        .and(QueryBuilder.lte(TIMESTAMP_KEY, timeRestriction.getToOrDefault(Long.MAX_VALUE)))
        .and(QueryBuilder.gt(TIMESTAMP_KEY, timeRestriction.getAfterOrDefault(Long.MIN_VALUE)));
    return this.cassandraSession.execute(statement).all().get(0).getLong(0);
  }

  @Override
  public List<String> getIdentifiers() {
    final Statement statement = QueryBuilder
        .select(IDENTIFIER_KEY)
        .distinct()
        .from(this.tableName);
    return this.cassandraSession
        .execute(statement)
        .all()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
  }

  private Where buildRestrictedSelectAllBaseStatement(
      final String identifier,
      final TimeRestriction timeRestriction) {
    return QueryBuilder.select().all()
        .from(this.tableName)
        .where(QueryBuilder.eq(IDENTIFIER_KEY, identifier))
        .and(this.buildLowerTimeRestrictionClause(timeRestriction))
        .and(this.buildUpperTimeRestrictionClause(timeRestriction));
  }

  private Clause buildLowerTimeRestrictionClause(final TimeRestriction timeRestriction) {
    if (timeRestriction.hasFrom()) {
      // If two lower restrictions do exists, find the "superior" one.
      if (timeRestriction.hasAfter() && timeRestriction.getAfter() >= timeRestriction.getFrom()) {
        QueryBuilder.gt(TIMESTAMP_KEY, timeRestriction.getAfter());
      } else {
        QueryBuilder.gte(TIMESTAMP_KEY, timeRestriction.getFrom());
      }
    } else if (timeRestriction.hasAfter()) {
      QueryBuilder.gt(TIMESTAMP_KEY, timeRestriction.getAfter());
    }
    return QueryBuilder.gte(TIMESTAMP_KEY, Long.MIN_VALUE);
  }

  private Clause buildUpperTimeRestrictionClause(final TimeRestriction timeRestriction) {
    return QueryBuilder.lte(TIMESTAMP_KEY, timeRestriction.getToOrDefault(Long.MAX_VALUE));
  }

  /**
   * Execute the provided Cassandra {@link Statement} and reconstruct records of type T.
   */
  private List<T> executeStatement(final Statement statement) {
    final ResultSet resultSet = this.cassandraSession.execute(statement); // NOPMD no close()

    final List<T> records = new ArrayList<>();
    for (final Row row : resultSet) {
      final T record = this.recordFactory.apply(row);
      records.add(record);
    }

    return records;
  }

  /**
   * Create an {@link ActivePowerRepository} for {@link AggregatedActivePowerRecord}s.
   */
  public static ActivePowerRepository<AggregatedActivePowerRecord> forAggregated(
      final Session cassandraSession) {

    return new CassandraRepository<>(
        cassandraSession,
        AggregatedActivePowerRecord.class.getSimpleName(),
        CassandraReader.recordFactory(AggregatedActivePowerRecord::new),
        record -> record.getSumInW());
  }


  /**
   * Create an {@link ActivePowerRepository} for {@link ActivePowerRecord}s.
   */
  public static ActivePowerRepository<ActivePowerRecord> forNormal(final Session cassandraSession) {
    return new CassandraRepository<>(
        cassandraSession,
        ActivePowerRecord.class.getSimpleName(),
        CassandraReader.recordFactory(ActivePowerRecord::new),
        record -> record.getValueInW());
  }

}
