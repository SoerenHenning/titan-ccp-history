package titan.ccp.aggregation;

import java.util.concurrent.CompletableFuture;

import kieker.common.record.IMonitoringRecord;
import teetime.framework.AbstractProducerStage;

public class KafkaReaderStage extends AbstractProducerStage<IMonitoringRecord> {

	private final KafkaReader kafkaReader;

	public KafkaReaderStage() {
		this.kafkaReader = new KafkaReader(this::sendRecord);
	}

	@Override
	protected void execute() throws Exception {
		this.kafkaReader.run();
		super.workCompleted();
	}

	public CompletableFuture<Void> requestTermination() {
		return this.kafkaReader.requestTermination();
	}

	private void sendRecord(final IMonitoringRecord record) {
		this.getOutputPort().send(record);
	}

}
