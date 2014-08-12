package org.gazzax.labs.jena.nosql.fwk;

import org.gazzax.labs.jena.nosql.fwk.dictionary.node.TopLevelDictionaryBase;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

/**
 * A bidirectional index that can be accessed both by key and value.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class BIndex implements Initialisable{
	PersistentKeyValueMap<byte[], String> byId;
	PersistentKeyValueMap<String, byte[]> byValue;

	final String name;
	
	/**
	 * Builds a new index with the given name.
	 * 
	 * @param name the name of this index.
	 */
	public BIndex(final String name) {
		this.name = name;
	}
	
	@Override
	public void initialise(final StorageLayerFactory factory) throws InitialisationException {
		byValue = new PersistentKeyValueMap<String, byte[]>(
				String.class, 
				byte[].class,
				name,
				false,
				TopLevelDictionaryBase.NOT_SET);
		byValue.initialise(factory);

		byId = new PersistentKeyValueMap<byte[], String>(
				byte[].class,
				String.class,
				name + "_REVERSE",
				false,
				Constants.EMPTY_STRING);
		byId.initialise(factory);
	}
	
	/**
	 * Returns the id associated with the given value.
	 * 
	 * @param value the n3 value.
	 * @return the id associated with the given value.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public byte[] get(final String value) throws StorageLayerException {
		return byValue.get(value);
	}

	/**
	 * Returns the value associated with the given id.
	 * 
	 * @param id the Cumulus internal id.
	 * @return the value associated with the given id.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public String getValue(final byte[] id) throws StorageLayerException {
		return byId.get(id);
	}

	/**
	 * Puts the given pair on this index.
	 * 
	 * @param value the resource.
	 * @param id the id associated with that resource.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public void putEntry(final String value, final byte[] id) throws StorageLayerException {
		byValue.put(value, id);
		byId.put(id, value);
	}

	/**
	 * Returns true if this index contains the given id.
	 * 
	 * @param id the id of a given resource.
	 * @return true if this index contains the given id.
	 * @throws StorageLayerException in case of data access failure.

	 */
	public boolean contains(final byte[] id) throws StorageLayerException {
		return byId.containsKey(id);
	}

	/**
	 * Removes the given resource from this index.
	 * 
	 * @param n3 the n3 representation of the resource to be removed.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public void remove(final String n3) throws StorageLayerException {
		byId.remove(byValue.get(n3));
		byValue.remove(n3);
	}
}