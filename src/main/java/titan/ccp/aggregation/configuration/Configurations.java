package titan.ccp.aggregation.configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Configurations {

	private static final Logger LOGGER = LoggerFactory.getLogger(Configurations.class);

	private final static String DEFAULT_PROPERTY_LOCATION = "META-INF/application.properties";
	private final static String USER_PROPERTY_LOCATION = "config/application.properties";

	public static final Configuration create() {
		final CompositeConfiguration configuration = new CompositeConfiguration();
		addEnvironmentVariables(configuration);
		addUserConfigurationFile(configuration);
		addDefaultConfigurationFile(configuration);
		return configuration;
	}

	private static void addEnvironmentVariables(final CompositeConfiguration configuration) {
		configuration.addConfiguration(new NameResolvingEnvironmentConfiguration());
	}

	private static void addUserConfigurationFile(final CompositeConfiguration configuration) {
		final Path path = Paths.get(USER_PROPERTY_LOCATION);
		if (Files.exists(path)) {
			LOGGER.info("Found user configuration at {}", USER_PROPERTY_LOCATION);
			try {
				configuration.addConfiguration(configurations().properties(path.toFile()));
			} catch (final ConfigurationException e) {
				throw new IllegalArgumentException(
						"Cannot load configuration from file \"" + USER_PROPERTY_LOCATION + "\"", e);
			}
		} else {
			LOGGER.info("No user configuration found at {}", USER_PROPERTY_LOCATION);
		}
	}

	private static void addDefaultConfigurationFile(final CompositeConfiguration configuration) {
		try {
			configuration.addConfiguration(configurations().properties(DEFAULT_PROPERTY_LOCATION));
		} catch (final ConfigurationException e) {
			throw new IllegalArgumentException(
					"Cannot load configuration from ressource " + "\"" + DEFAULT_PROPERTY_LOCATION + "\"", e);
		}
	}

	/**
	 * Shortcut for long class name
	 */
	private static org.apache.commons.configuration2.builder.fluent.Configurations configurations() {
		return new org.apache.commons.configuration2.builder.fluent.Configurations();
	}

}
