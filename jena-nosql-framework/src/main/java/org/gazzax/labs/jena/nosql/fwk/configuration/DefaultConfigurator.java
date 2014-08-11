package org.gazzax.labs.jena.nosql.fwk.configuration;

import static org.gazzax.labs.jena.nosql.fwk.Constants.EMPTY_STRING;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Default configurator.
 * Implements a <a href="http://en.wikipedia.org/wiki/Finite-state_machine">finite state machine</a> trying 
 * to find the configuration according with the following chain:
 * 
 * <ol>
 * 	<li>If a <b>jena.nosql.config.file</b> system property is found, then the file will be loaded.</li>
 * 	<li>If a <b>jena.nosql.config.dir</b> system property is found, then a file in that directory called <i>jena-nosql.yaml</i> will be loaded.</li>
 * 	<li>The system will try to load a classpath resource called /jena-nosql.yaml</li>
 * 	<li>The system will use the embedded default classpath resource jena-nosql-default.yaml</li> 
 * </ol>
 * 
 * Whenever the precondition of each step fails or a failure is met, then the system will try the next step.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @see <a href="http://en.wikipedia.org/wiki/Finite-state_machine">http://en.wikipedia.org/wiki/Finite-state_machine</a>
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class DefaultConfigurator implements Configuration<Map<String, Object>> {
	
	/**
	 * A configuration state.
	 * It represents a concrete state of the internal Finite State Machine.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	interface ConfigurationState {
		/**
		 * Specifies the behavior of the configurator when it will be in this specific state.
		 * 
		 * @param configurable the configurable target.
		 */
		void configure(final Configurable configurable);
	}

	protected final Log logger = new Log(LoggerFactory.getLogger(DefaultConfigurator.class));

	public static final String CONFIGURATION_FILE_SYSTEM_PROPERTY = "jena.nosql.config.file";
	public static final String ETC_DIR_SYSTEM_PROPERTY = "jena.nosql.config.dir";
	public static final String CONFIGURATION_FILENAME = "jena-nosql.yaml";
	public static final String DEFAULT_CONFIGURATION_FILE_NAME = "jena-nosql-default.yaml";
	
	/**
	 * 1st try: looks for a configuration file path declared as a system property.
	 */
	final ConfigurationState tryWithConfigurationFile = new ConfigurationState() {

		@SuppressWarnings("unchecked")
		@Override
		public void configure(final Configurable configurable) {
			try {
				final File file = new File(System.getProperty(CONFIGURATION_FILE_SYSTEM_PROPERTY, EMPTY_STRING));
				if (file.canRead()) {
					parameters = (Map<String, Object>) new Yaml().load(new FileReader(file));

//					logger.info(MessageCatalog._00101_USING_CONFIG_FILE, file);
					DefaultConfigurator.this.configure(configurable);
				} else {
//					logger.error(MessageCatalog._00100_CONFIG_FILE_NOT_READABLE, configFilePath);
					transitionTo(switchToConfigurationDirectory, configurable);
				}
			} catch (final FileNotFoundException exception) {
//				logger.error(MessageCatalog._00099_CONFIG_FILE_NOT_FOUND, configFilePath);
				transitionTo(switchToConfigurationDirectory, configurable);
			} catch (final Exception exception) {
//				logger.error(MessageCatalog., configFilePath);
				transitionTo(switchToConfigurationDirectory, configurable);
			}
		}
	};

	/**
	 * 2nd try: looks for a configuration directory path declared as a system property.
	 */
	final ConfigurationState switchToConfigurationDirectory = new ConfigurationState() {

		@SuppressWarnings("unchecked")
		@Override
		public void configure(final Configurable configurable) {
			final File configFile = new File(System.getProperty(ETC_DIR_SYSTEM_PROPERTY, EMPTY_STRING), CONFIGURATION_FILENAME);
			try {
				if (configFile.canRead()) {
					final Yaml loader = new Yaml();
					parameters = (Map<String, Object>) loader.load(new FileReader(configFile));

//					logger.info(MessageCatalog._00101_USING_CONFIG_FILE, configFile);
					DefaultConfigurator.this.configure(configurable);
				} else {
//					logger.error(MessageCatalog._00100_CONFIG_FILE_NOT_READABLE, configFile);
					transitionTo(switchToClasspathResource, configurable);
				}
			} catch (final FileNotFoundException exception) {
//				logger.error(MessageCatalog._00099_CONFIG_FILE_NOT_FOUND, configFile);
				transitionTo(switchToClasspathResource, configurable);
			} catch (final Exception exception) {
//				logger.error(MessageCatalog., configFilePath);
				transitionTo(switchToClasspathResource, configurable);
			}
		}
	};
	
	/**
	 * 3rd try: Looks for a classpath resource.
	 */
	final ConfigurationState switchToClasspathResource = new ConfigurationState() {
		@SuppressWarnings("unchecked")
		@Override
		public void configure(final Configurable configurable) {
			try {
				
				final InputStream stream = getClass().getResourceAsStream("/" + CONFIGURATION_FILENAME);
				if (stream != null) {
					parameters = (Map<String, Object>) new Yaml().load(stream);
	
//					logger.info(MessageCatalog._00102_USING_CLASSPATH_RESOURCE, CONFIGURATION_FILENAME);
					DefaultConfigurator.this.configure(configurable);
				} else {
					transitionTo(switchToEmbeddedConfiguration, configurable);
				}
			} catch (final Exception exception) {
//				logger.error(MessageCatalog.);
				transitionTo(switchToEmbeddedConfiguration, configurable);
			}
		}
	};
	
	/**
	 * 4th try: use the embedded configuration.
	 */
	final ConfigurationState switchToEmbeddedConfiguration = new ConfigurationState() {

		@SuppressWarnings("unchecked")
		@Override
		public void configure(final Configurable configurable) {
			parameters = (Map<String, Object>) new Yaml().load(getClass().getResourceAsStream("/" + DEFAULT_CONFIGURATION_FILE_NAME));

//			logger.info(MessageCatalog._00103_USING_EMBEDDED_CONFIGURATION, DEFAULT_CONFIGURATION_FILE_NAME);

			configurable.accept(DefaultConfigurator.this);
			currentState = configurationHasBeenLoaded;
		}
	};

	final ConfigurationState configurationHasBeenLoaded = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			DefaultConfigurator.this.configure(configurable);
		}
	};

	protected Map<String, Object> parameters;
	protected ConfigurationState currentState = tryWithConfigurationFile;

	@Override
	public void configure(final Configurable configurable) {
		currentState.configure(configurable);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getParameter(final String name, final T defaultValue) {
		Objects.requireNonNull(name, "'name' must not be null.");
		
		if (parameters == null) {
			throw new IllegalStateException("Method called before configuration initialisation.");
		}
		
		T value = (T) parameters.get(name);
		return value != null ? value : defaultValue;
	};	

	/**
	 * Switches the current state of this configurator.
	 * 
	 * @param newState the new state.
	 * @param configurable the configurable target.
	 */
	void transitionTo(final ConfigurationState newState, final Configurable configurable) {
		currentState = newState;
		currentState.configure(configurable);
	}
}