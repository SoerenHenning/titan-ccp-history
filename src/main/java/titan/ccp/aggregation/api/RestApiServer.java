package titan.ccp.aggregation.api;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import spark.Service;
import titan.ccp.models.records.AggregatedPowerConsumptionRecord;
import titan.ccp.models.records.PowerConsumptionRecord;

//TODO make a builder that returns this server
public class RestApiServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

	private final Gson gson = new GsonBuilder().create();

	private final ActivePowerRepository<AggregatedPowerConsumptionRecord> aggregatedRepository;
	private final ActivePowerRepository<PowerConsumptionRecord> normalRepository;

	private final Service webService;

	private final boolean enableCors;

	public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors) {
		this.aggregatedRepository = ActivePowerRepository.forAggregated(cassandraSession);
		this.normalRepository = ActivePowerRepository.forNormal(cassandraSession);
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

		this.webService.get("/power-consumption/:identifier", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			return this.normalRepository.get(identifier, after);
		}, this.gson::toJson);

		this.webService.get("/power-consumption/:identifier/latest", (request, response) -> {
			final String identifier = request.params("identifier");
			final int count = NumberUtils.toInt(request.queryParams("count"), 1);
			return this.normalRepository.getLatest(identifier, count);
		}, this.gson::toJson);

		this.webService.get("/power-consumption/:identifier/distribution", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4);
			return this.normalRepository.getDistribution(identifier, after, buckets);
		}, this.gson::toJson);

		this.webService.get("/aggregated-power-consumption/:identifier", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			return this.aggregatedRepository.get(identifier, after);
		}, this.gson::toJson);

		this.webService.get("/aggregated-power-consumption/:identifier/latest", (request, response) -> {
			final String identifier = request.params("identifier");
			final int count = NumberUtils.toInt(request.queryParams("count"), 1);
			return this.aggregatedRepository.getLatest(identifier, count);
		}, this.gson::toJson);

		this.webService.get("/aggregated-power-consumption/:identifier/distribution", (request, response) -> {
			final String identifier = request.params("identifier");
			final long after = NumberUtils.toLong(request.queryParams("after"), 0);
			final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4);
			return this.aggregatedRepository.getDistribution(identifier, after, buckets);
		}, this.gson::toJson);

		this.webService.after((request, response) -> {
			response.type("application/json");
		});

	}

}
