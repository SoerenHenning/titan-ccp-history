package titan.ccp.aggregation.experimental.teetimebased;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kieker.analysis.plugin.reader.newio.deserializer.BinaryDeserializer;
import kieker.analysis.plugin.reader.newio.deserializer.IMonitoringRecordDeserializer;
import kieker.common.record.IMonitoringRecord;

public class KafkaReader {

	private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

	private static final String GROUP_ID = "test-2";

	private static final String TOPIC_NAME = "test-soeren--3";

	// private final String deserializerClassName;

	// private final String readerClassName;

	// private final IRawDataReader reader;

	private final KafkaConsumer<String, byte[]> kafkaConsumer;

	private final IMonitoringRecordDeserializer deserializer = new BinaryDeserializer(null, null); // TODO

	private final String topicName;

	private final Consumer<IMonitoringRecord> recordHandler;

	private volatile boolean terminationRequested = false;

	private final CompletableFuture<Void> terminationRequestResult = new CompletableFuture<>();

	public KafkaReader(final Consumer<IMonitoringRecord> recordHandler) {
		final Properties properties = new Properties();

		properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		properties.put("group.id", GROUP_ID);
		// properties.put("enable.auto.commit", this.enableAutoCommit);
		// properties.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
		// properties.put("session.timeout.ms", this.sessionTimeoutMs);

		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		this.kafkaConsumer = new KafkaConsumer<>(properties);
		this.recordHandler = recordHandler;
		this.topicName = TOPIC_NAME;

	}

	public void run() {
		this.kafkaConsumer.subscribe(Arrays.asList(this.topicName));

		while (!this.terminationRequested) {
			final ConsumerRecords<String, byte[]> records = this.kafkaConsumer.poll(1000); // TODO

			this.processRecords(records);
		}

		this.kafkaConsumer.close();

		this.terminationRequestResult.complete(null);
	}

	public CompletableFuture<Void> requestTermination() {
		this.terminationRequested = true;
		return this.terminationRequestResult;
	}

	// PMD thinks this is an unused private method (see:
	// https://github.com/pmd/pmd/issues/521)
	private void processRecords(final ConsumerRecords<String, byte[]> records) { // NOPMD (false positive, see above)
		for (final ConsumerRecord<String, byte[]> record : records) {
			final byte[] data = record.value();
			this.decodeAndDeliverRecords(data);
		}
	}

	private void decodeAndDeliverRecords(final byte[] rawData) {
		this.decodeAndDeliverRecords(ByteBuffer.wrap(rawData), rawData.length);
	}

	private void decodeAndDeliverRecords(final ByteBuffer rawData, final int dataSize) {
		final List<IMonitoringRecord> monitoringRecords = this.deserializer.deserializeRecords(rawData, dataSize);

		for (final IMonitoringRecord monitoringRecord : monitoringRecords) {
			this.deliver(monitoringRecord);
		}
	}

	private void deliver(final IMonitoringRecord monitoringRecord) {
		this.recordHandler.accept(monitoringRecord);

	}

}
