package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryBase;
import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryRuntimeContext;

/**
 * Supertype layer for all string dictionaries.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class StringDictionaryBase extends DictionaryBase<String> {

	protected static final ThreadLocal<DictionaryRuntimeContext> RUNTIME_CONTEXTS = new ThreadLocal<DictionaryRuntimeContext>() {
		protected DictionaryRuntimeContext initialValue() {
			return new DictionaryRuntimeContext();
		};
	};
	
	/**
	 * Builds a new dictionary with the given identifier.
	 * 
	 * @param id the dictionary identifier.
	 */
	public StringDictionaryBase(final String id) {
		super(id);
	}	
}