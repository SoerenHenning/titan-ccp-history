package titan.ccp.aggregation;

import java.util.Locale;

import org.apache.commons.configuration2.EnvironmentConfiguration;

public class NameResolvingEnvironmentConfiguration extends EnvironmentConfiguration {

	public NameResolvingEnvironmentConfiguration() {
		super();
	}

	@Override
	protected Object getPropertyInternal(final String key) {
		final Object value = super.getPropertyInternal(key);
		if (value == null) {
			return super.getPropertyInternal(formatKeyAsEnvVariable(key));
		}
		return value;
	}

	@Override
	protected boolean containsKeyInternal(final String key) {
		final boolean value = super.containsKeyInternal(key);
		if (value == false) {
			return super.containsKeyInternal(formatKeyAsEnvVariable(key));
		}
		return value;
	}

	public static String formatKeyAsEnvVariable(final String key) {
		return key.toUpperCase(Locale.ROOT).replace('.', '_');
	}

}
