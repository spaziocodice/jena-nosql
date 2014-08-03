package org.gazzax.labs.jena.nosql.fwk.dictionary;

import java.nio.ByteBuffer;

/**
 * Interface for defining cache strategies behaviour.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @param <V> the value kind managed by this strategy.
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface CacheStrategy<V> {
	/**
	 * Caches a given identifier and the corresponding value.
	 * 
	 * @param id a value identifier.
	 * @param value the value.
	 */
	void cacheId(ByteBuffer id, V value);

	/**
	 * Caches a given value and its corresponding identifier.
	 * 
	 * @param value the value.
	 * @param id the value identifier.
	 */
	void cacheValue(V value, byte[] id);
}