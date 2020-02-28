package titan.ccp.history.recordcounter;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.history.api.ActivePowerRepository;
import titan.ccp.history.api.CassandraRepository;
import titan.ccp.model.records.ActivePowerRecord;

/**
 * Prints the total amount of input measurements along with the total amount of stored measurements
 * every second to the command line.
 */
public final class RecordCounter {

  private RecordCounter() {}

  /**
   * Main method to start the record counter.
   */
  public static void main(final String[] args) {

    final String cassandraHost =
        Objects.requireNonNullElse(System.getenv("CASSANDRA_HOST"), "localhost"); // NOCS
    final int cassandraPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("CASSANDRA_PORT"), "9042"));
    final String cassandraKeyspace =
        Objects.requireNonNullElse(System.getenv("CASSANDRA_KEYSPACE"), "titanccp");
    final ClusterSession clusterSession = new SessionBuilder().contactPoint(cassandraHost)
        .port(cassandraPort).keyspace(cassandraKeyspace).build();
    final ActivePowerRepository<ActivePowerRecord> normalRepository =
        CassandraRepository.forNormal(clusterSession.getSession());
    final String redisHost = Objects.requireNonNullElse(System.getenv("REDIS_HOST"), "localhost");
    final int redisPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("REDIS_PORT"), "6379"));
    final Jedis jedis = new Jedis(redisHost, redisPort);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {
      final long inputValue = Long.parseLong(jedis.get("input_counter"));
      final long outputValue = normalRepository.getTotalCount();
      System.out.println(inputValue + "," + outputValue + "," + (inputValue - outputValue));// NOPMD
    }, 1, 1, TimeUnit.SECONDS);

  }

}
