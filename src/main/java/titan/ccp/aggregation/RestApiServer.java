package titan.ccp.aggregation;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import spark.Service;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;

//TODO make a builder that returns this server
public class RestApiServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

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

}
