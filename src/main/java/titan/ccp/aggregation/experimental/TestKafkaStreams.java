package titan.ccp.aggregation.experimental;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

import kieker.common.record.IMonitoringRecord;
import titan.ccp.common.kieker.cassandra.CassandraWriter;
import titan.ccp.common.kieker.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.kieker.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.SessionBuilder;
import titan.ccp.common.kieker.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;

public class TestKafkaStreams {

	private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaStreams.class);

	public static void main(final String[] args) {
		final String cassandraHost = Objects.requireNonNullElse(System.getenv("CASSANDRA_HOST"), "localhost");
		final int cassandraPort = Integer.parseInt(Objects.requireNonNullElse(System.getenv("CASSANDRA_PORT"), "9042"));
		final String cassandraKeyspace = Objects.requireNonNullElse(System.getenv("CASSANDRA_KEYSPACE"), "titanccp");
		final String kafkaBootstrapServers = Objects.requireNonNullElse(System.getenv("KAFKA_BOOTSTRAP_SEVERS"),
				"localhost:9092");
		final String kafkaApplicationId = Objects.requireNonNullElse(System.getenv("KAFKA_APPLICATION_ID"),
				"titanccp-aggregation-test-0.0.1");
		final int kafkaCommitInterval = Integer
				.parseInt(Objects.requireNonNullElse(System.getenv("KAFKA_COMMIT_INTERVAL"), "1000"));
		final String kafkaInputTopic = Objects.requireNonNullElse(System.getenv("KAFKA_INPUT_TOPIC"), "input");

		final Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaApplicationId);
		settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kafkaCommitInterval);
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

		final ClusterSession clusterSession = new SessionBuilder().contactPoint(cassandraHost).port(cassandraPort)
				.keyspace(cassandraKeyspace).build();
		final CassandraWriter cassandraWriter = buildCassandraWriter(clusterSession.getSession(),
				ActivePowerRecord.class);

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, ActivePowerRecord> input = builder.stream(kafkaInputTopic,
				Consumed.with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

		// input.foreach((k, v) -> System.out.println(k + ": " + v));

		input.foreach((k, v) -> {
			// System.out.println("Write record: " + v);
			cassandraWriter.write(v);
		});

		/*
		 * final KStream<String, Long> sensorCounts = input .groupBy((k, v) -> k,
		 * Serialized.with(Serdes.String(), IMonitoringRecordSerde.serde(new
		 * ActivePowerRecordFactory()))) .aggregate(() -> 0L, (ak, n, av) -> av + 1,
		 * Materialized.<String, Long, KeyValueStore<Bytes,
		 * byte[]>>as("aggregated-stream-store-test-1")
		 * .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long())) .toStream();
		 *
		 * sensorCounts.foreach((k, v) -> System.out.println(k + ": " + v));
		 *
		 * sensorCounts.groupBy((k, v) -> "all", Serialized.with(Serdes.String(),
		 * Serdes.Long())) .aggregate(() -> 0L, (ak, n, av) -> av + n,
		 * Materialized.<String, Long, KeyValueStore<Bytes,
		 * byte[]>>as("aggregated-stream-store-test-2")
		 * .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
		 * .toStream().foreach((k, v) -> System.out.println(k + ": " + v));
		 */
		// .aggregate(v -> 0, (aggKey, newValue, aggValue) -> aggValue + 1, //
		// Materialized.as("aggregated-stream-store"));

		final Topology topology = builder.build();
		final TopologyDescription topologyDescription = topology.describe();
		System.out.println(topologyDescription);
		final KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(settings));
		streams.start();
	}

	private static CassandraWriter buildCassandraWriter(final Session session,
			final Class<? extends IMonitoringRecord> recordClass) {
		final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy = new ExplicitPrimaryKeySelectionStrategy();
		primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
		primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

		final CassandraWriter cassandraWriter = CassandraWriter.builder(session).excludeRecordType()
				.excludeLoggingTimestamp().tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
				.primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

		return cassandraWriter;
	}

}
