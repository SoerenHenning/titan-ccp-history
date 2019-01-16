package titan.ccp.model.sensorregistry.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Loads a {@link SensorRegistry} via HTTP from the given URI.
 */
public class SensorRegistryRequester {

  private static final Logger LOGGER = LoggerFactory.getLogger(SensorRegistryRequester.class);

  private static final String DEFAULT_PATH = "/sensor-registry";
  private static final String DEFAULT_SCHEME = "http";

  private final HttpClient client = HttpClient.newHttpClient();
  private final URI uri;

  public SensorRegistryRequester(final String host, final int port) {
    this(buildUri(host, port));
  }

  public SensorRegistryRequester(final String uri) {
    this(URI.create(uri));
  }

  public SensorRegistryRequester(final URI uri) {
    this.uri = uri;
  }

  /**
   * Requests a {@link SensorRegistry} asynchronously.
   */
  public CompletableFuture<SensorRegistry> request() {
    final HttpRequest request = HttpRequest.newBuilder().uri(this.uri).GET().build();

    LOGGER.info("Request sensor registry on GET: {}", this.uri);

    // TODO handle errors
    return this.client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(r -> {
      return SensorRegistry.fromJson(r.body());
    });
  }

  private static URI buildUri(final String host, final int port) {
    try {
      return new URI(DEFAULT_SCHEME, null, host, port, DEFAULT_PATH, null, null);
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

}
