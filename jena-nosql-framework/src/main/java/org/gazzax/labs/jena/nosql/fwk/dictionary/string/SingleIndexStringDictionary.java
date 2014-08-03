package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import java.util.Arrays;

import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

/**
 * Supertype layer for String dictionaries that are backed by a single {@link Index}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SingleIndexStringDictionary extends StringDictionaryBase {
	protected BIndex index;
	protected final String indexName;
	
	/**
	 * Builds a new dictionary with a given index name.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the index name.
	 */
	protected SingleIndexStringDictionary(final String id, final String indexName) {
		super(id);
		this.indexName = indexName;
	}

	@Override
	protected final void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {
		index = createIndex();
		index.initialise(factory);
	}
	
	@Override
	protected void closeInternal() {
		// Nothing to be done here...
	}
	
	/**
	 * Creates a new identifier for a given string.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param value the string value. 
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 */
	protected byte[] newId(final String value, final BIndex index) {
		return IDMaker.nextID();
	}

	/**
	 * Creates and initializes the underlying index.
	 * 
	 * @return the underlying index.
	 */
	protected BIndex createIndex() {
		return new BIndex(indexName);
	}
	
	/**
	 * Returns the N3 representation of the value associated with a given identifier.
	 * 
	 * @param id the value identifier.
	 * @param p the predicate flag.
	 * @return the N3 representation of the value associated with a given identifier.
	 * @throws StorageLayerException in case of data access failure. 
	 */
	protected String getStringValue(final byte[] id, final boolean p) throws StorageLayerException {

		final String value = index.getQuick(id);
		if (value == null || value.isEmpty()) {
			log.error(MessageCatalog._00086_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return value;
	}	

	/**
	 * Returns the underlying index name.
	 * 
	 * @return the underlying index name.
	 */
	public String getIndexName() {
		return indexName;
	}
	
	@Override
	public void removeValue(final String value, final boolean p) throws StorageLayerException {
		index.remove(value);
	}	
}