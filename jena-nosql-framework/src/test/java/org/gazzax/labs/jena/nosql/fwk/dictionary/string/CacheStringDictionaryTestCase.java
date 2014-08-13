package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Date;

import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link CacheStringDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheStringDictionaryTestCase {

	private CacheStringDictionary cut;
	private Dictionary<String> decoratee;

	byte[] id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	final String aValue = String.valueOf(new Date());

	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */ 
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		decoratee = mock(Dictionary.class);
		cut = new CacheStringDictionary(
				randomString(), 
				decoratee, 
				CacheStringDictionary.DEFAULT_CACHE_SIZE, 
				CacheStringDictionary.DEFAULT_CACHE_SIZE, 
				false);

		cut.initialise(STORAGE_LAYER_FACTORY);

		verify(decoratee).initialise(STORAGE_LAYER_FACTORY);
	}

	/**
	 * In case the given cache size is lesser or equal to 0, then default value apply.
	 */
	@Test
	public void defaultCacheSize() {
		assertEquals(CacheStringDictionary.DEFAULT_CACHE_SIZE, cut.cacheSize(-0));
		assertEquals(CacheStringDictionary.DEFAULT_CACHE_SIZE, cut.cacheSize(-12));
		assertEquals(12345, cut.cacheSize(12345));
	}
	
	/**
	 * In case the decoratee is null then an exception must be thrown.
	 */
	@Test
	public void decorateeIsNull() {
		try {
			cut = new CacheStringDictionary(
					randomString(), 
					null, 
					CacheStringDictionary.DEFAULT_CACHE_SIZE, 
					CacheStringDictionary.DEFAULT_CACHE_SIZE, 
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
}