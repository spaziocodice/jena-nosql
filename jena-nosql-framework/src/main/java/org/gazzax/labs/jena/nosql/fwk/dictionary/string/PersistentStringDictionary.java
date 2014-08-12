package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;

/**
 * A persistent string dictionary.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class PersistentStringDictionary extends SingleIndexStringDictionary {

	static final int ID_LENGTH = 8;
	static final byte[] EMPTY_VALUE = new byte[ID_LENGTH];

	/**
	 * Builds a new known dictionary for namespaces.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the name of the underlying index.
	 */
	public PersistentStringDictionary(final String id, final String indexName) {
		super(id, indexName);
	}
	
	@Override
	protected byte[] getIdInternal(final String value, final boolean p) throws StorageLayerException {
		if (value.trim().length() == 0) {
			return EMPTY_VALUE;
		}
		
		byte[] id = null;

		synchronized (this) {
			id = index.get(value);
			if (id[0] == NOT_SET[0]) {
				id = newId(value, index);
				index.putEntry(value, id);
			}
		}
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return id;
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return index.getValue(id);
	}
}