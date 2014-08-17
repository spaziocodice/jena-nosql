package org.gazzax.labs.jena.nosql.fwk;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

/**
 * An interface that adds "initialisation" behaviour to a domain object.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Initialisable {
	/** 
	 * Initializes this dictionary.
	 * This is a callback method that the owner of this instance uses for inform 
	 * the domain object about its (startup) procedure.
	 * 
	 * @param factory the storage layer factory.
	 * @throws InitialisationException in case the initialisaton fails.
	 */
	void initialise(StorageLayerFactory factory) throws InitialisationException;
}
