package org.gazzax.labs.jena.nosql.fwk.configuration;

/**
 * Main configuration and configurator interface.
 * "Configuration" and "Configurator" because it acts both as the configuration (i.e. configuration parameters holder) itself and 
 * who is in charge to spread that configuration. 
 * The interface uses generics in order to have an extension point for future implementations.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @param <E> the managed type that will represent the parameters holder within this configuration.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Configuration<E> {
	/**
	 * Returns the value associated with the given configuration parameter.
	 * 
	 * @param <T> the configuration parameter value kind.
	 * @param name the configuration parameter name.
	 * @param defaultValue the default value that will be returned in case the requested configuration parameter is not found.
	 * @return the value associated with a given configuration parameter, or a default value in case the parameter is not found.
	 */
	<T> T getParameter(String name, T defaultValue);

	/**
	 * "Configurator" interface used for injecting and configuring things.
	 * IMPORTANT: this method is supposed to be idempotent because the framework
	 * could call it more than once on a given instance.
	 * 
	 * @param configurable the configuration target.
	 */
	void configure(Configurable configurable);
}