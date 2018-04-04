package titan.ccp.aggregation.experimental.kafkastreams;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import titan.ccp.model.PowerConsumptionRecord;

public class KafkaRecordProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordProducer.class);

	private final Producer<String, PowerConsumptionRecord> producer;
	private static final String BOOTSRATP_SERVERS = "localhost:9092";
	private static final String TOPIC = "test-topic-18040319";

	public KafkaRecordProducer() {
		final Properties properties = new Properties();

		properties.put("bootstrap.servers", BOOTSRATP_SERVERS);
		// properties.put("acks", this.acknowledges);
		// properties.put("batch.size", this.batchSize);
		// properties.put("linger.ms", this.lingerMs);
		// properties.put("buffer.memory", this.bufferMemory);

		// properties.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		// properties.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");

		this.producer = new KafkaProducer<>(properties, new StringSerializer(),
				new KafkaStreamsFactory.PowerConsumptionRecordSerializer());
	}

	public void write(final PowerConsumptionRecord powerConsumptionRecord) {
		final ProducerRecord<String, PowerConsumptionRecord> record = new ProducerRecord<>(TOPIC,
				powerConsumptionRecord.getIdentifier(), powerConsumptionRecord);
		this.producer.send(record);
	}

	public static void main(final String[] args) throws InterruptedException {
		LOGGER.info("Start producing records");

		final Random random = new Random();
		final KafkaRecordProducer kafkaWriter = new KafkaRecordProducer();

		final List<String> identifiers = List.of("sensor-1", "sensor-2", "sensor-3");

		while (true) {
			final String identifier = identifiers.get(random.nextInt(identifiers.size()));
			final long timestamp = System.currentTimeMillis();
			final int value = random.nextInt(100);
			final PowerConsumptionRecord record = new PowerConsumptionRecord(identifier, timestamp, value);
			kafkaWriter.write(record);
			Thread.sleep(1000);
		}

	}

}
