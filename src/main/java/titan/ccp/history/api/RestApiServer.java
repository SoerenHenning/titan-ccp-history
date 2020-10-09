package titan.ccp.history.api;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.LinkedList;
import java.util.List;
import java.util.function.LongConsumer;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Service;
import titan.ccp.history.streamprocessing.TimeWindowsConfiguration;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.records.WindowedActivePowerRecord;


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

  private final Session cassandraSession;
  private final ActivePowerRepository<ActivePowerRecord> normalRepository;
  private final ActivePowerRepository<AggregatedActivePowerRecord> aggregatedRepository;

  private final Service webService;

  private final boolean enableCors;
  private final boolean enableGzip;
  private final List<String> windowResolutions = new LinkedList<>();

  /**
   * Creates a new API server using the passed parameters.
   */
  public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors,
      final boolean enableGzip) {
    this.cassandraSession = cassandraSession;
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

    // Common configurations
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

    this.webService.after((request, response) -> {
      response.type("application/json");
      if (this.enableGzip) {
        response.header("Content-Encoding", "gzip");
      }
    });

    // Active power routes for raw and aggregated
    this.addActivePowerEndpoints("active-power/raw", this.normalRepository);
    this.addActivePowerEndpoints("active-power/aggregated", this.aggregatedRepository);

    // Route to get the different windowed power routes
    this.webService.get("/active-power/windowed", (request, response) -> {
      return this.windowResolutions;
    }, this.gson::toJson);
  }

  /**
   * Stop the webserver.
   */
  public void stop() {
    this.webService.stop();
  }

  /**
   * Creates for every time windows configuration an endpoint.
   *
   * @param timeWindowsConfigurations for the endpoints.
   */
  public void addWindowedEndpoints(final List<TimeWindowsConfiguration> timeWindowsConfigurations) {
    for (final TimeWindowsConfiguration twc : timeWindowsConfigurations) {
      // Check if an Endpoint name is given, otherwise discard this time window
      if (twc.getApiEndpoint() == null) {
        LOGGER.info("No endpoint created for windowed aggregation: {}", twc.getKafkaTopic());
        return;
      }

      // Create a Cassandra repository for this particular window
      final CassandraRepository<WindowedActivePowerRecord> windowedRepository = CassandraRepository
          .forWindowed(twc, this.cassandraSession);

      this.addActivePowerEndpoints("active-power/windowed/" + twc.getApiEndpoint(),
          windowedRepository);
      this.windowResolutions.add(twc.getApiEndpoint());
    }
  }

  /**
   * Creates the common active power records for a given prefix and {@code ActivePowerRepository}.
   *
   * @param routePrefix to access the resource (e.g. "aggregated" creates route "/aggregated").
   * @param activePowerRepository to access the data.
   */
  private void addActivePowerEndpoints(final String prefix,
      final ActivePowerRepository<?> activePowerRepository) {

    // Create the prefix for the routes
    final String routePrefix = "/" + prefix;


    this.webService.get(routePrefix, (request, response) -> {
      return activePowerRepository.getIdentifiers();
    }, this.gson::toJson);

    this.webService.get(routePrefix + "/:identifier", (request, response) -> {
      final String identifier = request.params("identifier"); // NOCS NOPMD
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      return activePowerRepository.get(identifier, timeRestriction);
    }, this.gson::toJson);

    this.webService.get(routePrefix + "/:identifier/latest", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int count = NumberUtils.toInt(request.queryParams("count"), 1); // NOCS
      return activePowerRepository.getLatest(identifier, timeRestriction, count);
    }, this.gson::toJson);

    this.webService.get(routePrefix + "/:identifier/distribution", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4); // NOCS
      return activePowerRepository.getDistribution(identifier, timeRestriction, buckets);
    }, this.gson::toJson);

    this.webService.get(routePrefix + "/:identifier/trend", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      final int pointsToSmooth =
          NumberUtils.toInt(request.queryParams("pointsToSmooth"), 10); // NOCS NOPMD
      return activePowerRepository.getTrend(identifier, timeRestriction, pointsToSmooth);
    }, this.gson::toJson);


    this.webService.get(routePrefix + "/:identifier/count", (request, response) -> {
      final String identifier = request.params("identifier");
      final TimeRestriction timeRestriction = constructTimeRestriction(request);
      return activePowerRepository.getCount(identifier, timeRestriction);
    }, this.gson::toJson);
  }

  /**
   * Create a {@code TimeRestriction} object from a request.
   *
   * @param request containing the parameters.
   * @return a {@code TimeRestriction} object.
   */
  private static TimeRestriction constructTimeRestriction(final Request request) {
    final TimeRestriction timeRestriction = new TimeRestriction();
    maybeAddRestriction(request, FROM_QUERY_PARAM, timeRestriction::setFrom);
    maybeAddRestriction(request, TO_QUERY_PARAM, timeRestriction::setTo);
    maybeAddRestriction(request, AFTER_QUERY_PARAM, timeRestriction::setAfter);
    return timeRestriction;
  }

  /**
   * Helper method to set the {@code TimeRestriction} attribute using the request.
   *
   * @param request containing the parameters.
   * @param paramName to check if exists.
   * @param setter to set the attribute in the {@code TimeRestriction} object.
   */
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
