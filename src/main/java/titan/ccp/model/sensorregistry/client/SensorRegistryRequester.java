package titan.ccp.model.sensorregistry.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class SensorRegistryRequester {

	private static final String DEFAULT_PATH = "/sensor-registry/";
	private static final String DEFAULT_SCHEME = "http";

	private final HttpClient client = HttpClient.newHttpClient();
	private final URI uri;

	public SensorRegistryRequester(final String host, final int port) {
		this(buildURI(host, port));
	}

	public SensorRegistryRequester(final String uri) {
		this(URI.create(uri));
	}

	public SensorRegistryRequester(final URI uri) {
		this.uri = uri;
	}

	public CompletableFuture<SensorRegistry> request() {
		final HttpRequest request = HttpRequest.newBuilder().uri(this.uri).GET().build();

		// TODO handle errors
		return this.client.sendAsync(request, HttpResponse.BodyHandler.asString()).thenApply(r -> {
			return SensorRegistry.fromJson(r.body());
		});
	}

	private static final URI buildURI(final String host, final int port) {
		try {
			return new URI(DEFAULT_SCHEME, null, host, port, DEFAULT_PATH, null, null);
		} catch (final URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
