package titan.ccp.aggregation.experimental;

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

	private final String inputTopic = "test";
	private final String aggregationStoreName = "stream-store"; // TODO

	public static void main(final String[] args) {
		final Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "titanccp-aggregation-test-0.0.7"); // TODO as parameter
		settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // TODO as parameter
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		final ClusterSession clusterSession = new SessionBuilder().contactPoint("localhost").port(9042)
				.keyspace("titanccp").build();
		final CassandraWriter cassandraWriter = buildCassandraWriter(clusterSession.getSession(),
				ActivePowerRecord.class);

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, ActivePowerRecord> input = builder.stream("input",
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
