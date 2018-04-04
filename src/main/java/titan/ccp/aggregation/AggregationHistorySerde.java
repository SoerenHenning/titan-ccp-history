package titan.ccp.aggregation;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class AggregationHistorySerde {

	public static Serde<AggregationHistory> create() {
		return Serdes.serdeFrom(new AggregationHistorySerializer(), new AggregationHistoryDeserializer());
	}

}
