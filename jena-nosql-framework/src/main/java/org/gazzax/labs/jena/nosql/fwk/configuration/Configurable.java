package org.gazzax.labs.jena.nosql.fwk.configuration;

import java.util.Map;

/**
 * Defines something that could be configured.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Configurable {
	/**
	 * Receives a configuration and configure itself.
	 *  
	 * @param configuration the configuration.
	 */
	void accept(final Configuration<Map<String, Object>> configuration);
}