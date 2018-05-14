package titan.ccp.aggregation.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import spark.Service;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;

//TODO make a builder that returns this server
public class RestApiServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

	private final Gson gson = new GsonBuilder().create();

	private final Session cassandraSession;

	private final Service webService;

	private final boolean enableCors;

	public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors) {
		this.cassandraSession = cassandraSession;
		LOGGER.info("Instantiate API server.");
		this.webService = Service.ignite().port(port);
		this.enableCors = enableCors;
	}

	public void start() {
		LOGGER.info("Instantiate API routes.");

		if (this.enableCors) {
			this.webService.options("/*", (request, response) -> {

				final String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
				if (accessControlRequestHeaders != null) {
					response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
				}

				final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
				if (accessControlRequestMethod != null) {
					response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
				}

				return "OK";
			});

			this.webService.before((request, response) -> {
				response.header("Access-Control-Allow-Origin", "*");
			});
		}

		this.webService.get("/aggregated-power-consumption/:identifier", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			return this.getAggregatedPowerConsumption(identifier, after).toString();
		});

		this.webService.get("/aggregated-power-consumption/:identifier/distribution", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4);
			return this.getAggregatedPowerConsumptionDistribution(identifier, after, buckets);
		}, this.gson::toJson);

		this.webService.after((request, response) -> {
			response.type("application/json");
		});

	}

	private JsonElement getAggregatedPowerConsumption(final String identifier, final long after) {
		final Statement statement = QueryBuilder.select().all()
				.from(AggregatedPowerConsumptionRecord.class.getSimpleName())
				.where(QueryBuilder.eq("identifier", identifier)).and(QueryBuilder.gt("timestamp", after));
		final ResultSet resultSet = this.cassandraSession.execute(statement);

		final JsonArray jsonArray = new JsonArray();
		for (final Row row : resultSet) {
			final JsonObject jsonObject = new JsonObject();
			// row.get("identifier", Type);
			jsonObject.addProperty("identifier", row.getString("identifier"));
			jsonObject.addProperty("timestamp", row.getLong("timestamp"));
			jsonObject.addProperty("min", row.getInt("min"));
			jsonObject.addProperty("max", row.getInt("max"));
			jsonObject.addProperty("count", row.getLong("count"));
			jsonObject.addProperty("sum", row.getLong("sum"));
			jsonObject.addProperty("average", row.getDouble("average"));

			jsonArray.add(jsonObject);
		}

		return jsonArray; // TODO
	}

	private List<DistributionBucket> getAggregatedPowerConsumptionDistribution(final String identifier,
			final long after, final int bucketsCount) {
		final Statement statement = QueryBuilder.select().all()
				.from(AggregatedPowerConsumptionRecord.class.getSimpleName())
				.where(QueryBuilder.eq("identifier", identifier)).and(QueryBuilder.gt("timestamp", after));
		final ResultSet resultSet = this.cassandraSession.execute(statement);

		final List<AggregatedPowerConsumptionRecord> records = new ArrayList<>();
		for (final Row row : resultSet) {
			records.add(new AggregatedPowerConsumptionRecord(row.getString("identifier"), row.getLong("timestamp"),
					row.getInt("min"), row.getInt("max"), row.getLong("count"), row.getLong("sum"),
					row.getDouble("average")));
		}

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
		for (int i = 0; i <= bucketsCount; i++) {
			final double lower = i > 0 ? buckets.get(i - 1).getUpper() : min;
			final double upper = i < bucketsCount ? lower + sliceSize : max;
			buckets.add(new DistributionBucket(lower, upper, distribution[i]));
		}

		return buckets;
	}

}
