package titan.ccp.aggregation.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import kieker.common.record.factory.IRecordFactory;
import titan.ccp.common.kieker.cassandra.CassandraDeserializer;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.AggregatedPowerConsumptionRecordFactory;
import titan.ccp.models.records.PowerConsumptionRecord;

public class PowerConsumptionRepository<T> {

	private final Session cassandraSession;
	private final String tableName;
	private final Function<Row, T> recordFactory;
	private final ToLongFunction<T> valueAccessor;

	public PowerConsumptionRepository(final Session cassandraSession, final String tableName,
			final Function<Row, T> recordFactory, final ToLongFunction<T> valueAccessor) {
		this.cassandraSession = cassandraSession;
		this.tableName = tableName;
		this.recordFactory = recordFactory;
		this.valueAccessor = valueAccessor;
	}

	public PowerConsumptionRepository(final Session cassandraSession, final String tableName,
			final IRecordFactory<T> recordFactory, final ToLongFunction<T> valueAccessor) {
		this(cassandraSession, tableName, row -> recordFactory.create(new CassandraDeserializer(row)), valueAccessor);
	}

	public List<T> get(final String identifier, final long after) {
		final Statement statement = QueryBuilder.select().all().from(this.tableName)
				.where(QueryBuilder.eq("identifier", identifier)).and(QueryBuilder.gt("timestamp", after));

		return this.get(statement);
	}

	private List<T> get(final Statement statement) {
		final ResultSet resultSet = this.cassandraSession.execute(statement);

		final List<T> records = new ArrayList<>();
		for (final Row row : resultSet) {
			final T record = this.recordFactory.apply(row);
			records.add(record);
		}

		return records;
	}

	public List<T> getLatest(final String identifier, final int count) {
		final Statement statement = QueryBuilder.select().all().from(this.tableName)
				.where(QueryBuilder.eq("identifier", identifier)).orderBy(QueryBuilder.desc("timestamp")).limit(count);

		return this.get(statement);
	}

	public List<DistributionBucket> getDistribution(final String identifier, final long after, final int bucketsCount) {
		final List<T> records = this.get(identifier, after);

		if (records.isEmpty()) {
			return Collections.emptyList();
		}

		final long min = records.stream().mapToLong(this.valueAccessor).min().getAsLong();
		final long max = records.stream().mapToLong(this.valueAccessor).max().getAsLong();

		final double sliceSize = (max - min) / (double) bucketsCount;

		final int[] distribution = new int[bucketsCount];
		for (final T record : records) {
			final long value = this.valueAccessor.applyAsLong(record);
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

	public static PowerConsumptionRepository<AggregatedPowerConsumptionRecord> forAggregated(
			final Session cassandraSession) {
		final AggregatedPowerConsumptionRecordFactory factory = new AggregatedPowerConsumptionRecordFactory();
		return new PowerConsumptionRepository<>(cassandraSession,
				AggregatedPowerConsumptionRecord.class.getSimpleName(),
				// BETTER Use factory and deserializer
				// new AggregatedPowerConsumptionRecordFactory(),
				row -> new AggregatedPowerConsumptionRecord(row.getString("identifier"), row.getLong("timestamp"),
						row.getInt("min"), row.getInt("max"), row.getLong("count"), row.getLong("sum"),
						row.getDouble("average")),
				record -> record.getSum());
	}

	public static PowerConsumptionRepository<PowerConsumptionRecord> forNormal(final Session cassandraSession) {
		return new PowerConsumptionRepository<>(cassandraSession, PowerConsumptionRecord.class.getSimpleName(),
				// BETTER Use factory and deserializer
				// new PowerConsumptionRecordFactory
				row -> new PowerConsumptionRecord(row.getString("identifier"), row.getLong("timestamp"),
						row.getInt("powerConsumptionInWh")),
				record -> record.getPowerConsumptionInWh());
	}

}
