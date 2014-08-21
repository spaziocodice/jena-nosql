package org.gazzax.labs.jena.nosql.fwk;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomBytes;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.gazzax.labs.jena.nosql.fwk.TestUtility.TestStorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link BIndex}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class BIndexTestCase {
	private BIndex cut;
	private String name;
	private PersistentKeyValueMap<String, byte[]> byValue;
	private PersistentKeyValueMap<byte[], String> byId;
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {
		name = randomString();
		cut = new BIndex(name);
		byValue = mock(PersistentKeyValueMap.class);
		byId = mock(PersistentKeyValueMap.class);
		cut.byId = byId;
		cut.byValue = byValue;
	}
	
	/**
	 * A getValue requests must be satisfied by the "by id" index.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void getValueMustUseByIdIndex() throws Exception {
		final byte[] id = randomBytes(8);
		cut.getValue(id);
		
		verify(byId).get(id);
		verifyNoMoreInteractions(byId);
		verifyZeroInteractions(byValue);
	}
	
	/**
	 * A getId requests must be satisfied by the "by value" index.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void getIdMustUseByValueIndex() throws Exception {
		final String value = randomString();
		cut.getId(value);
		
		verify(byValue).get(value);
		verifyNoMoreInteractions(byValue);
		verifyZeroInteractions(byId);		
	}	
	
	/**
	 * Put entry must fill both indexes.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void putEntry() throws Exception {
		final byte[] id = randomBytes(8);
		final String value = randomString();
		
		cut.putEntry(value, id);
		
		verify(byValue).put(value, id);
		verify(byId).put(id, value);
		verifyNoMoreInteractions(byValue);
		verifyZeroInteractions(byId);		
	}		
	
	/**
	 * In order to see if an id exists within the index, the "by id" index portion will be queried.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void containsId() throws Exception {
		final byte[] id = randomBytes(8);
		cut.contains(id);
		
		verify(byId).containsKey(id);
		verifyNoMoreInteractions(byId);
		verifyZeroInteractions(byValue);
	}	
	
	/**
	 * A remove will trigger a remove on both indexes.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void containsValue() throws Exception {
		final byte[] id = randomBytes(8);
		final String value = randomString();
		
		when(byValue.get(value)).thenReturn(id);
		
		cut.remove(value);
		
		verify(byValue).get(value);
		verify(byValue).remove(value);
		verify(byId).remove(id);
		verifyNoMoreInteractions(byId);
		verifyZeroInteractions(byValue);
	}		
	
	/**
	 * In case the initialisation fails an exception must be thrown.
	 */
	@Test
	public void initialisationFailure() {
		try {
			cut.initialise(new TestStorageLayerFactory() {
				@SuppressWarnings({ "rawtypes", "unchecked" })
				@Override
				public <K, V> MapDAO<K, V> getMapDAO(
						final Class<K> keyClass,
						final Class<V> valueClass, 
						final boolean isBidirectional, 
						final String name) {
					final MapDAO dao = mock(MapDAO.class);
					try {
						doThrow(StorageLayerException.class).when(dao).setDefaultValue(any());
					} catch (final StorageLayerException exception) {
						fail();
					}
					return dao;
				}
			});		
		} catch (InitialisationException expected) {
			// Nothing, this is the expected behaviour.
		}
	}

	/**
	 * Checks the correctness of the state after initialisation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void stateAfterInitialisation() throws Exception {
		cut.initialise(STORAGE_LAYER_FACTORY);
		
		assertTrue(cut.byId != null);
		assertTrue(cut.byValue != null);
		
		assertEquals(name, cut.byValue.name);
		assertEquals(TopLevelDictionary.NOT_SET, cut.byValue.defaultValue);
		assertFalse(cut.byValue.isBidirectional);

		assertEquals(name + "_REVERSE", cut.byId.name);
		assertEquals(Constants.EMPTY_STRING, cut.byId.defaultValue);
		assertFalse(cut.byId.isBidirectional);
	}
}
