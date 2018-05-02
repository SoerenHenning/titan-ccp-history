package titan.ccp.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.commons.configuration2.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class MyEnvironmentConfigurationTest {

	private static final String PROPERTY_FILES_KEY = "my.env.var";
	private static final String ENV_VAR_KEY = "MY_ENV_VAR";
	private static final String STRING_VALUE = "value";

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void testHelperLibrary() {
		this.environmentVariables.clear("name");
		this.environmentVariables.set("name", STRING_VALUE);
		assertEquals("value", System.getenv("name"));
	}

	@Test
	public void testGetUsingEnvVarFormat() {
		this.environmentVariables.clear(ENV_VAR_KEY);
		this.environmentVariables.set(ENV_VAR_KEY, STRING_VALUE);
		final Configuration config = new MyEnvironmentConfiguration();
		final String result = config.getString(ENV_VAR_KEY);
		assertEquals(STRING_VALUE, result);
	}

	@Test
	public void testGetUsingPropertiesFormat() {
		this.environmentVariables.clear(ENV_VAR_KEY);
		this.environmentVariables.set(ENV_VAR_KEY, STRING_VALUE);
		final Configuration config = new MyEnvironmentConfiguration();
		final String result = config.getString(PROPERTY_FILES_KEY);
		assertEquals(STRING_VALUE, result);
	}

	@Test
	public void testGetNonExistingUsingEnvVarFormat() {
		this.environmentVariables.clear(ENV_VAR_KEY);
		final Configuration config = new MyEnvironmentConfiguration();
		final String result = config.getString(ENV_VAR_KEY);
		assertNull(result);
	}

	@Test
	public void testGetNonExistingUsingPropertiesForma() {
		this.environmentVariables.clear(ENV_VAR_KEY);
		final Configuration config = new MyEnvironmentConfiguration();
		final String result = config.getString(PROPERTY_FILES_KEY);
		assertNull(result);
	}

	@Test
	public void testFormatKeyAsEnvVariable() {
		assertEquals(ENV_VAR_KEY, MyEnvironmentConfiguration.formatKeyAsEnvVariable(PROPERTY_FILES_KEY));
	}

}
