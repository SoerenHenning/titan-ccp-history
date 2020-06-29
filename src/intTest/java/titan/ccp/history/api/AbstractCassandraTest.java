package titan.ccp.history.api;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.DropKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.CassandraContainer;

public abstract class AbstractCassandraTest {

  @Rule
  public CassandraContainer<?> cassandra = new CassandraContainer<>();

  private static final String KEYSPACE = "test";

  protected Session session;

  @Before
  public void setUpSession() throws Exception {
    try (Session session = this.cassandra.getCluster().connect()) {
      final KeyspaceOptions createKeyspace =
          SchemaBuilder
              .createKeyspace(KEYSPACE)
              .ifNotExists()
              .with()
              .replication(ImmutableMap.of("class", "SimpleStrategy", "replication_factor", 1));
      session.execute(createKeyspace);
    }

    this.session = this.cassandra.getCluster().connect(KEYSPACE);
  }

  @After
  public void tearDownSession() throws Exception {
    this.session.close();
    this.session = null;

    try (Session session = this.cassandra.getCluster().connect()) {
      final DropKeyspace dropKeyspace = SchemaBuilder.dropKeyspace(KEYSPACE).ifExists();
      session.execute(dropKeyspace);
    }
  }

}
