package titan.ccp.model.sensorregistry.client;

import java.util.concurrent.CompletableFuture;
import titan.ccp.model.sensorregistry.SensorRegistry;


/**
 * Requests a {@link SensorRegistry}.
 */
public interface SensorRegistryRequester {

  /**
   * Requests a {@link SensorRegistry} asynchronously.
   */
  CompletableFuture<SensorRegistry> request();

}
