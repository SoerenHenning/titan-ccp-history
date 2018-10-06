package titan.ccp.aggregation.recordcounter;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import titan.ccp.aggregation.api.ActivePowerRepository;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.models.records.ActivePowerRecord;

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
        ActivePowerRepository.forNormal(clusterSession.getSession());
    final String redisHost = Objects.requireNonNullElse(System.getenv("REDIS_HOST"), "localhost");
    final int redisPort =
        Integer.parseInt(Objects.requireNonNullElse(System.getenv("REDIS_PORT"), "6379"));
    final Jedis jedis = new Jedis(redisHost, redisPort);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {
      final long inputValue = Long.parseLong(jedis.get("input_counter"));
      final long redisOutputValue = Long.parseLong(jedis.get("output_counter"));
      final long cassandraOutputValue = normalRepository.getTotalCount();
      System.out.println("" + inputValue + ',' + redisOutputValue + ',' + cassandraOutputValue + ','
          + (inputValue - redisOutputValue) + ',' + (inputValue - cassandraOutputValue));
      // System.out.println("" + inputValue + ',' + redisOutputValue + ',' + (inputValue -
      // redisOutputValue));
    }, 1, 1, TimeUnit.SECONDS);

  }

}
