package titan.ccp.history.streamprocessing;

import com.datastax.driver.core.Session;
import org.apache.avro.specific.SpecificRecord;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;

/**
 * Factory class for creating {@link CassandraWriter}s.
 */
public final class CassandraWriterFactory {

  private static final String IDENTIFIER_COLUMN = "identifier";
  private static final String START_TIMESTAMP_COLUMN = "startTimestamp";
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private final Session session;

  public CassandraWriterFactory(final Session session) {
    this.session = session;
  }

  /**
   * Build a {@link CassandraWriter} for arbitrary {@link SpecificRecord}s, which provide an
   * {@code identifier} and a {@code timestamp} field.
   */
  public <T extends SpecificRecord> CassandraWriter<SpecificRecord> buildUnwindowed(
      final Class<T> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy strategy = new ExplicitPrimaryKeySelectionStrategy();
    strategy.registerPartitionKeys(recordClass.getSimpleName(), IDENTIFIER_COLUMN);
    strategy.registerClusteringColumns(recordClass.getSimpleName(), TIMESTAMP_COLUMN);

    final CassandraWriter<SpecificRecord> cassandraWriter = CassandraWriter
        .builder(this.session, new AvroDataAdapter())
        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
        .primaryKeySelectionStrategy(strategy)
        .build();

    return cassandraWriter;
  }

  /**
   * Build a {@link CassandraWriter} for windowed Avro records, which provide an {@code identifier}
   * and a {@code standTimestamp} field.
   */
  public CassandraWriter<SpecificRecord> buildWindowed(final String tableName, final int ttl) {
    final ExplicitPrimaryKeySelectionStrategy strategy = new ExplicitPrimaryKeySelectionStrategy();
    strategy.registerPartitionKeys(tableName, IDENTIFIER_COLUMN);
    strategy.registerClusteringColumns(tableName, START_TIMESTAMP_COLUMN);

    final CassandraWriter<SpecificRecord> cassandraWriter = CassandraWriter
        .builder(this.session, new AvroDataAdapter())
        .tableNameMapper(c -> tableName)
        .primaryKeySelectionStrategy(strategy)
        .ttl(ttl)
        .build();

    return cassandraWriter;
  }

}
