package titan.ccp.aggregation;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.serialization.SensorRegistryDeserializer;

public class SensorRegistryRequester {

	private final HttpClient client = HttpClient.newHttpClient();
	private final URI uri;

	public SensorRegistryRequester(final String uri) {
		try {
			this.uri = new URI(uri);
		} catch (final URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public SensorRegistryRequester(final URI uri) {
		this.uri = uri;
	}

	public CompletableFuture<SensorRegistry> request() {
		final HttpRequest request = HttpRequest.newBuilder().uri(this.uri).GET().build();

		// TODO handle errors
		return this.client.sendAsync(request, HttpResponse.BodyHandler.asString())
				.thenApply(r -> this.sensorRegistryFromJson(r.body()));
	}

	// TODO move to factory method of SensorRegistry
	private SensorRegistry sensorRegistryFromJson(final String json) {
		final Gson gson = new GsonBuilder().registerTypeAdapter(SensorRegistry.class, new SensorRegistryDeserializer())
				.create();

		return gson.fromJson(json, SensorRegistry.class);
	}

}
