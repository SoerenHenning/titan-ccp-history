package titan.ccp.history.api;

import static org.junit.Assert.assertEquals;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.model.records.ActivePowerRecord;

public class CassandraRepositoryTest extends AbstractCassandraTest {

  @Test
  public void testFromRestriction() {
    final ActivePowerRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final TimeRestriction restriction = new TimeRestriction();
    restriction.setFrom(15);

    final List<ActivePowerRecord> records = repository.get("machine", restriction);
    assertEquals(2, records.size());
    assertEquals(20, records.get(0).getTimestamp());
    assertEquals(30, records.get(1).getTimestamp());
  }

  private <T extends SpecificRecord> CassandraWriter<SpecificRecord> buildCassandraWriter(
      final Class<T> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    final CassandraWriter<SpecificRecord> cassandraWriter = CassandraWriter
        .builder(this.session, new AvroDataAdapter())
        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
        .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

}
