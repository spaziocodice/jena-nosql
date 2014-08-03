package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import static org.junit.Assert.assertSame;

import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryRuntimeContext;
import org.junit.Test;

/**
 * Test case for {@link StringDictionaryBase}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class StringDictionaryBaseTest {
	/**
	 * The dictionary must own a dictionary runtime context.
	 */
	@Test
	public void dictionaryRuntimeContext() {
		final DictionaryRuntimeContext context = StringDictionaryBase.RUNTIME_CONTEXTS.get();

		final DictionaryRuntimeContext mustBeTheSame = StringDictionaryBase.RUNTIME_CONTEXTS.get();

		assertSame(context, mustBeTheSame);
	}
}