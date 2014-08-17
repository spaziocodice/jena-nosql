package org.gazzax.labs.jena.nosql.fwk;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomBoolean;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.gazzax.labs.jena.nosql.fwk.TestUtility.TestStorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Case for {@link PersistentKeyValueMap}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentKeyValueMapTestCase {
	
	private PersistentKeyValueMap<String, String> map;
	
	private String defaultValue;
	private String name;
	private boolean isBidirectional;
	private MapDAO<String, String> dao;
	
	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never otherwise tests will fail.
	 */
	@Before
	public void setUp() throws Exception {
		defaultValue = randomString();
		name = randomString();
		isBidirectional = randomBoolean();
		
		map = new PersistentKeyValueMap<String, String>(
				String.class, 
				String.class, 
				name, 
				isBidirectional, 
				defaultValue);
		
		map.initialise(new TestStorageLayerFactory());
		dao = map.dao;
		
		verify(dao).setDefaultValue(defaultValue);
		verify(dao).createRequiredSchemaEntities();		
	}
	
	/**
	 * In case the initialise raises a failure, an appropriate expection must be thrown.
	 */
	@Test
	public void initialisationFailure() throws Exception {
		try {
			map.initialise(new TestStorageLayerFactory() {
				@Override
				@SuppressWarnings({ "unchecked", "rawtypes" })
				public <K, V> MapDAO<K, V> getMapDAO(Class<K> keyClass, Class<V> valueClass, boolean isBidirectional, String name) {
					final MapDAO dao = mock(MapDAO.class);
					try {
						doThrow(StorageLayerException.class).when(dao).setDefaultValue(defaultValue);
					} catch (final StorageLayerException exception) {
						fail();
					}
					return dao;
				}
			});
			fail();
		} catch (InitialisationException expected) {
			// Nothing, this is the expected behaviour
		}
	}
	
	/**
	 * Assere the initial state of the map.
	 */
	@Test
	public void checkOnInitialState() {
		assertEquals(String.class, map.k);
		assertEquals(String.class, map.v);
		assertEquals(name, map.name);
		assertEquals(isBidirectional, map.isBidirectional);
		assertEquals(defaultValue, map.defaultValue);
	}
	
	/**
	 * Key/Value classes and name are mandatory attributes when builing a new instance.
	 */
	@Test
	public void keyValueClassesAndNameAreMandatory() {
		expectConstructionTimeException(null, String.class, randomString());
		expectConstructionTimeException(String.class, null, randomString());
		expectConstructionTimeException(String.class, String.class, null);
		expectConstructionTimeException(null, null, randomString());
		expectConstructionTimeException(String.class, null, null);
		expectConstructionTimeException(null, null, null);
	}

	/**
	 * Contains must return false for null keys.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void containsWithNullKey() throws Exception {
		assertFalse(map.containsKey(null));
	}
	
	/**
	 * Contains, for non-null keys, must delegate the operation to DAO layer.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void containsDelegatesToDAO() throws Exception {
		final String key = randomString();
		map.containsKey(key);
		verify(dao).contains(key);
	}

	/**
	 * Get must return null for null keys.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void getWithNullKey() throws Exception {
		assertNull(map.get(null));
	}
	
	/**
	 * Get, for non-null keys, must delegate the operation to DAO layer.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void getDelegatesToDAO() throws Exception {
		final String key = randomString();
		map.get(key);
		verify(dao).get(key);
	}
	
	/**
	 * Put does nothing in case key or value is null.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void putWithInvalidInputParameters() throws Exception {
		map.put(null, randomString());
		map.put(randomString(), null);
		map.put(null, null);
		
		verify(dao, never()).set(anyString(), anyString());
	}
	
	/**
	 * Put, for valid values, must delegate the operation to DAO layer.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void putDelegatesToDAO() throws Exception {
		final String key = randomString();
		final String value = randomString();
		
		map.put(key, value);
		verify(dao).set(key, value);
	}

	/**
	 * Remove does nothing in case key is null.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void removeWithNullKey() throws Exception {
		map.remove(null);
		
		verify(dao, never()).delete(anyString());
	}
	
	/**
	 * Remove, for valid keys, must delegate the operation to DAO layer.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void removeDelegatesToDAO() throws Exception {
		final String key = randomString();
		
		map.remove(key);
		verify(dao).delete(key);
	}
	
	/**
	 * Internal method used for testing construction-time invariants.
	 * 
	 * @param k the key class.
	 * @param v the value class.
	 * @param name the map name. 
	 */
	private <K, V> void expectConstructionTimeException(final Class<K> k, final Class<V> v, final String name) {
		try {
			new PersistentKeyValueMap<K, V>(k, v, name, false, null);
			fail();
		} catch (IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	} 
}