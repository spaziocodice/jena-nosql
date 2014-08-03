package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Date;

import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

/**
 * Test case for modes available in {@link CacheValueDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheModeTest {

	private CacheValueDictionary frontendDictionary;
	private CacheValueDictionary firstLevelCache2;

	private TopLevelDictionary firstLevelDecoratee1;
	private TopLevelDictionary firstLevelDecoratee2;

	private TopLevelDictionary leafDictionary;

	byte[] id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	
	final Node aValue = NodeFactory.createLiteral(String.valueOf(new Date()));

	/**
	 * First level cache test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Test
	public void firstLevelCache() throws Exception {
		
		leafDictionary = mock(TopLevelDictionary.class);
		firstLevelDecoratee2 = new TransientValueDictionary(randomString(), leafDictionary, 0);
		firstLevelCache2 = new CacheValueDictionary(
				randomString(), 
				firstLevelDecoratee2, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				false);

		firstLevelDecoratee1 = new TransientValueDictionary(randomString(), firstLevelCache2, TransientValueDictionary.DEFAULT_THRESHOLD);
		frontendDictionary = new CacheValueDictionary(
				randomString(), 
				firstLevelDecoratee1, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				true);

		frontendDictionary.initialise(STORAGE_LAYER_FACTORY);
		
		final Node uri = NodeFactory.createURI("http://example.org#it");
		final byte [] id = frontendDictionary.getID(uri, false);
		
		assertTrue(frontendDictionary.node2id_cache.containsKey(uri));
		
		Node value = frontendDictionary.getValue(id, false);
		assertEquals(uri, value);
		assertTrue(frontendDictionary.id2node_cache.containsKey(ByteBuffer.wrap(id)));
		
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < ((TransientValueDictionary)firstLevelDecoratee1).threshold + 1; i++) {
			builder.append('a');
		}

		final Node longLiteral = NodeFactory.createLiteral(builder.toString());
		final byte [] longLiteralId = frontendDictionary.getID(longLiteral, false);
		
		assertFalse(frontendDictionary.node2id_cache.containsKey(longLiteral));
		assertTrue(firstLevelCache2.node2id_cache.containsKey(longLiteral));
		
		value = frontendDictionary.getValue(longLiteralId, false);
		assertEquals(longLiteral, value);
		
		assertTrue(firstLevelCache2.id2node_cache.containsKey(ByteBuffer.wrap(subarray(longLiteralId, 1, longLiteralId.length - 1))));
		assertFalse(frontendDictionary.id2node_cache.containsKey(ByteBuffer.wrap(longLiteralId)));		
	}
}