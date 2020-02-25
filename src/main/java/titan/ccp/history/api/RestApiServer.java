package titan.ccp.history.api;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.function.LongConsumer;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Service;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;


/**
 * Contains a web server for accessing the history via a REST interface.
 */
public class RestApiServer {
  // TODO make a builder that returns this server

  private static final String FROM_QUERY_PARAM = "from";
  private static final String TO_QUERY_PARAM = "to";
  private static final String AFTER_QUERY_PARAM = "after";

  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

  private final Gson gson = new GsonBuilder().create();

  private final ActivePowerRepository<AggregatedActivePowerRecord> aggregatedRepository;
  private final ActivePowerRepository<ActivePowerRecord> normalRepository;

  private final Service webService;

  private final boolean enableCors;
  private final boolean enableGzip;

  /**
   * Creates a new API server using the passed parameters.
   */
  public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors,
      final boolean enableGzip) {
    this.aggregatedRepository = CassandraRepository.forAggregated(cassandraSession);
    this.normalRepository = CassandraRepository.forNormal(cassandraSession);
    LOGGER.info("Instantiate API server.");
    this.webService = Service.ignite().port(port);
    this.enableCors = enableCors;
    this.enableGzip = enableGzip;
  }

  /**
   * Start the web server by setting up the API routes.
   */
  public void start() { // NOPMD NOCS declaration of routes
    LOGGER.info("Instantiate API routes.");

    if (this.enableCors) {
      this.webService.options("/*", (request, response) -> {

        final String accessControlRequestHeaders =
            request.headers("Access-Control-Request-Headers");
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

    // TODO rename urls

    this.webService.get("/power-consumption", (request, response) -> {
      return this.normalRepository.getIdentifiers();
    }, this.gson::toJson);

    this.webService.get("/power-consumption/:identifier", (request, response) -> {
      final String identifier = request.params("identifier"); // NOCS NOPMD
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      return this.normalRepository.get(identifier, timeRestriction);
    }, this.gson::toJson);

    this.webService.get("/power-consumption/:identifier/latest", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int count = NumberUtils.toInt(request.queryParams("count"), 1); // NOCS
      return this.normalRepository.getLatest(identifier, timeRestriction, count);
    }, this.gson::toJson);

    this.webService.get("/power-consumption/:identifier/distribution", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4); // NOCS
      return this.normalRepository.getDistribution(identifier, timeRestriction, buckets);
    }, this.gson::toJson);

    this.webService.get("/power-consumption/:identifier/trend", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int pointsToSmooth =
          NumberUtils.toInt(request.queryParams("pointsToSmooth"), 10); // NOCS NOPMD
      return this.normalRepository.getTrend(identifier, timeRestriction, pointsToSmooth);
    }, this.gson::toJson);


    this.webService.get("/power-consumption/:identifier/count", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      return this.normalRepository.getCount(identifier, timeRestriction);
    }, this.gson::toJson);

    // TODO Temporary for evaluation, this is not working for huge data sets
    this.webService.get("/power-consumption-count", (request, response) -> {
      return this.normalRepository.getTotalCount();
    }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption", (request, response) -> {
      return this.aggregatedRepository.getIdentifiers();
    }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption/:identifier", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      return this.aggregatedRepository.get(identifier, timeRestriction);
    }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption/:identifier/latest",
        (request, response) -> {
          final String identifier = request.params("identifier");
          final TimeRestriction timeRestriction = constructTimeRestriction(request);
          final int count = NumberUtils.toInt(request.queryParams("count"), 1); // NOCS NOPMD
          return this.aggregatedRepository.getLatest(identifier, timeRestriction, count);
        }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption/:identifier/distribution",
        (request, response) -> {
          final String identifier = request.params("identifier");
          final TimeRestriction timeRestriction = constructTimeRestriction(request);
          final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4); // NOCS NOPMD
          return this.aggregatedRepository.getDistribution(identifier, timeRestriction, buckets);
        }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption/:identifier/trend",
        (request, response) -> {
          final String identifier = request.params("identifier");
          final TimeRestriction timeRestriction = constructTimeRestriction(request);
          final int pointsToSmooth =
              NumberUtils.toInt(request.queryParams("pointsToSmooth"), 10); // NOCS NOPMD
          return this.aggregatedRepository.getTrend(identifier, timeRestriction, pointsToSmooth);
        }, this.gson::toJson);

    this.webService.get("/aggregated-power-consumption/:identifier/count",
        (request, response) -> {
          final String identifier = request.params("identifier");
          final TimeRestriction timeRestriction = constructTimeRestriction(request);
          return this.aggregatedRepository.getCount(identifier, timeRestriction);
        }, this.gson::toJson);

    this.webService.after((request, response) -> {
      response.type("application/json");
      if (this.enableGzip) {
        response.header("Content-Encoding", "gzip");
      }
    });
  }

  /**
   * Stop the webserver.
   */
  public void stop() {
    this.webService.stop();
  }

  private static TimeRestriction constructTimeRestriction(final Request request) {
    final TimeRestriction timeRestriction = new TimeRestriction();
    maybeAddRestriction(request, FROM_QUERY_PARAM, timeRestriction::setFrom);
    maybeAddRestriction(request, TO_QUERY_PARAM, timeRestriction::setTo);
    maybeAddRestriction(request, AFTER_QUERY_PARAM, timeRestriction::setAfter);
    return timeRestriction;
  }

  private static void maybeAddRestriction(
      final Request request,
      final String paramName,
      final LongConsumer setter) {
    final String param = request.queryParams(paramName);
    if (param != null) {
      try {
        final long parsedParam = Long.parseLong(param);
        setter.accept(parsedParam);
      } catch (final NumberFormatException e) {
        throw new InvalidQueryException(
            "Query parameter '" + paramName + "' does not match required format.",
            e);
      }
    }

  }

}
