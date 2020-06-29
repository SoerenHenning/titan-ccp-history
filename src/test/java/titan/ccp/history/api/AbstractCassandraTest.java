package titan.ccp.history.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.DropKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableMap;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractCassandraTest {

  private static final String KEYSPACE = "test";

  private static Cluster cluster;
  protected Session session;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();

    AbstractCassandraTest.cluster = Cluster.builder()
        .addContactPoint(EmbeddedCassandraServerHelper.getHost())
        .withPort(EmbeddedCassandraServerHelper.getNativeTransportPort())
        .build();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    cluster.close();
  }

  @Before
  public void setUpSession() throws Exception {
    final Session session = EmbeddedCassandraServerHelper.getSession();
    final KeyspaceOptions createKeyspace =
        SchemaBuilder.createKeyspace(KEYSPACE).ifNotExists().with()
            .replication(ImmutableMap.of("class", "SimpleStrategy", "replication_factor", 1));
    session.execute(createKeyspace);

    this.session = cluster.connect(KEYSPACE);
  }

  @After
  public void tearDownSession() throws Exception {
    this.session.close();
    this.session = null;

    final Session session = EmbeddedCassandraServerHelper.getSession();
    final DropKeyspace dropKeyspace = SchemaBuilder.dropKeyspace(KEYSPACE).ifExists();
    session.execute(dropKeyspace);
  }

}
