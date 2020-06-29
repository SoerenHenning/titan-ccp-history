package titan.ccp.history.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Ignore;
import org.junit.Test;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.model.records.ActivePowerRecord;

public class CassandraRepositoryTest extends AbstractCassandraTest {

  @Test
  @Ignore // Throwing exception atm as table not exists
  public void testGetOnEmptyRepository() {
    final CassandraRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);

    final List<ActivePowerRecord> records = repository.get("machine", new TimeRestriction());
    assertTrue(records.isEmpty());
  }

  @Test
  public void testGetWithoutRestriction() {
    final CassandraRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final List<ActivePowerRecord> records = repository.get("machine", new TimeRestriction());
    assertEquals(3, records.size());
    assertEquals(10, records.get(0).getTimestamp());
    assertEquals(20, records.get(1).getTimestamp());
    assertEquals(30, records.get(2).getTimestamp());
  }

  @Test
  public void testGetWithFromRestriction() {
    final CassandraRepository<ActivePowerRecord> repository =
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

  @Test
  public void testGetWithAfterRestriction() {
    final ActivePowerRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final TimeRestriction restriction = new TimeRestriction();
    restriction.setAfter(15);

    final List<ActivePowerRecord> records = repository.get("machine", restriction);
    assertEquals(2, records.size());
    assertEquals(20, records.get(0).getTimestamp());
    assertEquals(30, records.get(1).getTimestamp());
  }

  @Test
  public void testGetWithToRestriction() {
    final CassandraRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final TimeRestriction restriction = new TimeRestriction();
    restriction.setFrom(25);

    final List<ActivePowerRecord> records = repository.get("machine", restriction);
    assertEquals(2, records.size());
    assertEquals(10, records.get(0).getTimestamp());
    assertEquals(20, records.get(1).getTimestamp());
  }

  @Test
  public void testGetWithFromRestrictionOnExact() {
    final CassandraRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final TimeRestriction restriction = new TimeRestriction();
    restriction.setFrom(10);

    final List<ActivePowerRecord> records = repository.get("machine", restriction);
    assertEquals(3, records.size());
    assertEquals(10, records.get(0).getTimestamp());
    assertEquals(20, records.get(0).getTimestamp());
    assertEquals(30, records.get(1).getTimestamp());
  }

  @Test
  public void testGetWithAfterRestrictionOnExact() {
    final CassandraRepository<ActivePowerRecord> repository =
        CassandraRepository.forNormal(this.session);
    final CassandraWriter<SpecificRecord> writer =
        this.buildCassandraWriter(ActivePowerRecord.class);

    writer.write(new ActivePowerRecord("machine", 10L, 20.0));
    writer.write(new ActivePowerRecord("machine", 20L, 20.0));
    writer.write(new ActivePowerRecord("machine", 30L, 20.0));

    final TimeRestriction restriction = new TimeRestriction();
    restriction.setAfter(10);

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
