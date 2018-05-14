package titan.ccp.aggregation.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import titan.ccp.models.records.AggregatedPowerConsumptionRecord;

public class AggregatedPowerConsumptionRepository {

	private final Session cassandraSession;

	public AggregatedPowerConsumptionRepository(final Session cassandraSession) {
		this.cassandraSession = cassandraSession;
	}

	public List<AggregatedPowerConsumptionRecord> get(final String identifier, final long after) {
		final Statement statement = QueryBuilder.select().all()
				.from(AggregatedPowerConsumptionRecord.class.getSimpleName())
				.where(QueryBuilder.eq("identifier", identifier)).and(QueryBuilder.gt("timestamp", after));
		final ResultSet resultSet = this.cassandraSession.execute(statement);

		final List<AggregatedPowerConsumptionRecord> records = new ArrayList<>();
		for (final Row row : resultSet) {
			// BETTER Use factory and deserializer
			records.add(new AggregatedPowerConsumptionRecord(row.getString("identifier"), row.getLong("timestamp"),
					row.getInt("min"), row.getInt("max"), row.getLong("count"), row.getLong("sum"),
					row.getDouble("average")));
		}

		return records;
	}

	public List<DistributionBucket> getDistribution(final String identifier, final long after, final int bucketsCount) {
		final List<AggregatedPowerConsumptionRecord> records = this.get(identifier, after);

		if (records.isEmpty()) {
			return Collections.emptyList();
		}

		final long min = records.stream().mapToLong(r -> r.getSum()).min().getAsLong();
		final long max = records.stream().mapToLong(r -> r.getSum()).max().getAsLong();

		final double sliceSize = (max - min) / (double) bucketsCount;

		final int[] distribution = new int[bucketsCount];
		for (final AggregatedPowerConsumptionRecord record : records) {
			final long value = record.getSum();
			final int index = Integer.min((int) ((value - min) / sliceSize), bucketsCount - 1);
			distribution[index]++;
		}

		final List<DistributionBucket> buckets = new ArrayList<>(bucketsCount);
		for (int i = 0; i < bucketsCount; i++) {
			final double lower = i > 0 ? buckets.get(i - 1).getUpper() : min;
			final double upper = i < bucketsCount ? lower + sliceSize : max;
			buckets.add(new DistributionBucket(lower, upper, distribution[i]));
		}

		return buckets;
	}

}
