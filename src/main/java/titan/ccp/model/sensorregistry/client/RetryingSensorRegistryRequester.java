package titan.ccp.model.sensorregistry.client;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class RetryingSensorRegistryRequester {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RetryingSensorRegistryRequester.class);

  private final SensorRegistryRequester requester;

  public RetryingSensorRegistryRequester(final SensorRegistryRequester requester) {
    this.requester = requester;
  }

  public CompletableFuture<SensorRegistry> request() {
    final RetryPolicy retryPolicy =
        new RetryPolicy().withBackoff(500, 10_000, TimeUnit.MILLISECONDS).withMaxRetries(10)
            .retryOn(IOException.class);

    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    final CompletableFuture<SensorRegistry> sensorRegistry = Failsafe.with(retryPolicy)
        .with(executor).onSuccess(x -> LOGGER.info("Received sensor registry."))
        .onFailure(e -> LOGGER.error("Could not receive sensor registry." + e))
        .onFailedAttempt(e -> LOGGER.info("Sensor registry not accessible. Wait for retry...", e))
        .onRetry(x -> LOGGER.info("Try to access sensor registry."))
        .onRetriesExceeded(e -> LOGGER
            .error("Could not receive sensor registry. Max. number of retries exceeded." + e))
        .future(() -> this.requester.request());
    sensorRegistry.thenRun(() -> executor.shutdown());
    return sensorRegistry;
  }

}
