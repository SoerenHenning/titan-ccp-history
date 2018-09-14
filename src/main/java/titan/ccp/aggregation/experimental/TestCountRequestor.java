package titan.ccp.aggregation.experimental;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import titan.ccp.aggregation.api.ActivePowerRepository;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.models.records.ActivePowerRecord;

public class TestCountRequestor {

	public static void main(final String[] args) {

		final String cassandraHost = Objects.requireNonNullElse(System.getenv("CASSANDRA_HOST"), "localhost");
		final int cassandraPort = Integer.parseInt(Objects.requireNonNullElse(System.getenv("CASSANDRA_PORT"), "9042"));
		final String cassandraKeyspace = Objects.requireNonNullElse(System.getenv("CASSANDRA_KEYSPACE"), "titanccp");
		final ClusterSession clusterSession = new SessionBuilder().contactPoint(cassandraHost).port(cassandraPort)
				.keyspace(cassandraKeyspace).build();
		final ActivePowerRepository<ActivePowerRecord> normalRepository = ActivePowerRepository
				.forNormal(clusterSession.getSession());
		final String redisHost = Objects.requireNonNullElse(System.getenv("REDIS_HOST"), "localhost");
		final int redisPort = Integer.parseInt(Objects.requireNonNullElse(System.getenv("REDIS_PORT"), "6379"));
		final Jedis jedis = new Jedis(redisHost, redisPort);

		final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			final long inputValue = Long.parseLong(jedis.get("input_counter"));
			final long outputValue = normalRepository.getTotalCount();
			System.out.println(inputValue + "," + outputValue + "," + (inputValue - outputValue));
		}, 1, 1, TimeUnit.SECONDS);

	}

}
