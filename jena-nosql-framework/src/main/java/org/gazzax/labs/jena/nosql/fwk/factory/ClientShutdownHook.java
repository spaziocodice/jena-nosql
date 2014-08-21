package org.gazzax.labs.jena.nosql.fwk.factory;

/**
 * An object that encapsulates client (connection) shutdown procedures.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface ClientShutdownHook {
	/**
	 * Shutdown procedure.
	 */
	void close();
}