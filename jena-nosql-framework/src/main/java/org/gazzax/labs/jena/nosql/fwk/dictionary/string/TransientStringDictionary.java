package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import static org.gazzax.labs.jena.nosql.fwk.Constants.CHARSET_UTF8;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

/**
 * Dictionary that uses strings encoding for generating variable-length identifiers.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TransientStringDictionary extends StringDictionaryBase {

	static final int DEFAULT_THRESHOLD = 1000; // 1K
	
	static final byte THRESHOLD_EXCEEDED = 1;
	static final byte THRESHOLD_NOT_EXCEEDED = 2;
	
	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public TransientStringDictionary(final String id) {
		super(id);
	}

	@Override
	protected void closeInternal() {
		// Nothing to be done here...
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) {
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return new String(id, CHARSET_UTF8);
	}

	@Override
	protected byte[] getIdInternal(final String value, final boolean p) {
		return value.getBytes(CHARSET_UTF8);
	}

	@Override
	public void removeValue(final String value, final boolean p) {
		// Nothing to be done here...
	}

	@Override
	public void initialiseInternal(final StorageLayerFactory factory) {
		// Nothing to be done here...
	}
}