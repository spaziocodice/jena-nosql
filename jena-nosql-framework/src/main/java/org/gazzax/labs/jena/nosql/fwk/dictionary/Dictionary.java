package org.gazzax.labs.jena.nosql.fwk.dictionary;

import org.gazzax.labs.jena.nosql.fwk.Initialisable;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;

/**
 * Dictionary interface.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 * 
 * @param <V> the kind of resource managed by this dictionary.
 */
public interface Dictionary<V> extends Initialisable {
	byte[] NOT_SET = { 1 }; 
	byte RESOURCE_BYTE_FLAG = 8; 
	byte BNODE_BYTE_FLAG = 16; 
	byte LITERAL_BYTE_FLAG = 32;
	
	/**
	 * Closes this dictionary and releases resources.
	 */
	void close();

	/**
	 * Returns the identifier of the given resource.
	 * 
	 * @param node the resource.
	 * @param p a flag indicating if the resource is a predicate.
	 * @return the identifier of the given resource.
	 * @throws StorageLayerException in case of data access failure.
	 */
	byte[] getID(V node, boolean p) throws StorageLayerException;
	
	/**
	 * Returns the value associated with the given identifier.
	 * 
	 * @param id the identifier.
	 * @param p a flag indicating if the id corresponds to a predicate.
	 * @return the value associated with the given identifier.
	 * @throws StorageLayerException in case of data access failure.
	 */
	V getValue(byte[] id, boolean p) throws StorageLayerException;

	/**
	 * Removes a given value from this dictionary.
	 *  
	 * @param value the value to be removed.
	 * @param p a flag indicating if the value is a predicate.
	 * @throws StorageLayerException in case of data access failure.
	 */
	void removeValue(V value, boolean p) throws StorageLayerException;
	
	/**
	 * Returns the name of the dictionary.
	 * The name should acts as an identifier, too.
	 * 
	 * @return the name of the dictionary.
	 */
	String getName();
}