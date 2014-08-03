package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

/**
 * Test case for {@link CacheValueDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CacheValueDictionaryTest {

	private CacheValueDictionary cut;
	private TopLevelDictionary decoratee;

	byte[] id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	final Node aValue = NodeFactory.createLiteral(randomString());

	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */ 
	@Before
	public void setUp() throws Exception {

		decoratee = mock(TopLevelDictionary.class);
		cut = new CacheValueDictionary(
				randomString(), 
				decoratee, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				false);

		cut.initialise(STORAGE_LAYER_FACTORY);
		verify(decoratee).initialise(STORAGE_LAYER_FACTORY);
	}

	/**
	 * In case the given cache size is lesser or equal to 0, then default value apply.
	 */
	@Test
	public void defaultCacheSize() {
		assertEquals(CacheValueDictionary.DEFAULT_CACHE_SIZE, cut.cacheSize(-0));
		assertEquals(CacheValueDictionary.DEFAULT_CACHE_SIZE, cut.cacheSize(-12));
		assertEquals(12345, cut.cacheSize(12345));
	}
	
	/**
	 * In case the decoratee is null then an exception must be thrown.
	 */
	@Test
	public void decorateeIsNull() {
		try {
			cut = new CacheValueDictionary(
					randomString(), 
					null, 
					CacheValueDictionary.DEFAULT_CACHE_SIZE, 
					CacheValueDictionary.DEFAULT_CACHE_SIZE, 
					false);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}

	/**
	 * Closing the dictionary means clearing the cache and invoking close() on decoratee.
	 */
	@Test
	public void close() {
		cut.close();
		
		assertEquals(0, cut.id2node_cache.size());
		assertEquals(0, cut.node2id_cache.size());
		verify(decoratee).close();
	}

	/**
	 * compose() must invoke compose() on decoratee.
	 */
	@Test
	public void compose() {
		cut.compose(id, id, id);
		verify(decoratee).compose(id, id, id);

		cut.compose(id, id);
		verify(decoratee).compose(id, id);
	}

	/**
	 * decompose() must invoke compose() on decoratee.
	 */
	@Test
	public void decompose() {
		cut.decompose(id);
		verify(decoratee).decompose(id);
	}

	/**
	 * Tests ID creation and caching.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getID() throws Exception {

		when(decoratee.getID(aValue, false)).thenReturn(id);

		assertTrue(cut.node2id_cache.isEmpty());
		assertArrayEquals(id, cut.getID(aValue, false));
		assertEquals(id, cut.node2id_cache.get(aValue));
		assertArrayEquals(id, cut.getID(aValue, false));
		
		verify(decoratee).getID(aValue, false);
	}

	/**
	 * Tests ID creation and caching.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValue() throws Exception {

		when(decoratee.getValue(id, false)).thenReturn(aValue);

		assertTrue(cut.id2node_cache.isEmpty());
		assertEquals(aValue, cut.getValue(id, false));
		assertEquals(aValue, cut.id2node_cache.get(ByteBuffer.wrap(id)));

		verify(decoratee).getValue(id, false);
	}

	/**
	 * If the input identifier is null the null must be returned.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithNullId() throws Exception {
		assertNull(cut.getValue(null, false));
		assertNull(cut.getValue(null, true));
	}

	/**
	 * If the input value is null the null must be returned.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithNullValue() throws Exception {
		assertNull(cut.getID(null, false));
		assertNull(cut.getID(null, true));
	}

	/**
	 * The remove() method with a null argument has no effect.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueWithNullValue() throws Exception {
		assertTrue(cut.node2id_cache.isEmpty());

		cut.removeValue(null, false);

		assertTrue(cut.node2id_cache.isEmpty());
		verifyNoMoreInteractions(decoratee);
	}

	/**
	 * The remove() method must remove the value from cache and call remove() on decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValue() throws Exception {
		when(decoratee.getID(aValue, false)).thenReturn(id);

		assertTrue(cut.node2id_cache.isEmpty());
		assertArrayEquals(id, cut.getID(aValue, false));

		assertEquals(id, cut.node2id_cache.get(aValue));
		assertArrayEquals(id, cut.getID(aValue, false));

		cut.removeValue(aValue, false);

		assertTrue(cut.node2id_cache.isEmpty());
		assertTrue(cut.id2node_cache.isEmpty());
		verify(decoratee).removeValue(aValue, false);
	}

	/**
	 * The isBNode request must be forwarded to the decoratee. 
	 */
	@Test
	public void isBNode() {
		cut.isBNode(id);
		verify(decoratee).isBNode(id);
	}

	/**
	 * The isLiteral request must be forwarded to the decoratee. 
	 */
	@Test
	public void isLiteral() {
		cut.isLiteral(id);
		verify(decoratee).isLiteral(id);
	}

	/**
	 * The isResource request must be forwarded to the decoratee. 
	 */
	@Test
	public void isResource() {
		cut.isResource(id);
		verify(decoratee).isResource(id);
	}
}