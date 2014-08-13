package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link PersistentStringDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class PersistentStringDictionaryTestCase {
	
	private PersistentStringDictionary _cut;

	private BIndex _dummyIndex;
	private String _underlyingIndexName;
	
	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception {
		_dummyIndex = mock(BIndex.class);

		_underlyingIndexName = randomString();
		_cut = new PersistentStringDictionary(randomString(), _underlyingIndexName) {
			@Override
			protected BIndex createIndex() {
				return _dummyIndex;
			}
		};
		
		_cut.initialise(STORAGE_LAYER_FACTORY);
	}

	/**
	 * The underlying index name must be injected at construction time.
	 */
	@Test
	public void indexName() {
		assertEquals(_underlyingIndexName, _cut.getIndexName());
	}

	/**
	 * If the input value is null, then null will be returned as id.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDWithNullString() throws Exception {
		assertNull(_cut.getID(null, RANDOMIZER.nextBoolean()));
	}

	/**
	 * Positive test for getID() method.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDNotPreviouslySet() throws Exception {
		when(_dummyIndex.contains(any(byte[].class))).thenReturn(false);
		
		final String aValue = randomString();
		when(_dummyIndex.getId(aValue)).thenReturn(Dictionary.NOT_SET);
		
		byte[] result = _cut.getID(aValue, RANDOMIZER.nextBoolean());
		assertEquals(PersistentStringDictionary.ID_LENGTH, result.length);

		verify(_dummyIndex).getId(aValue);
		verify(_dummyIndex).putEntry(aValue, result);
	}

	/**
	 * Positive test for getID() method.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDPreviouslySet() throws Exception {
		final String aValue = randomString();
		final byte[] id = new byte[PersistentStringDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		id[0] = (byte) (Dictionary.NOT_SET[0] + 1);

		when(_dummyIndex.getId(aValue)).thenReturn(id);

		byte[] result = _cut.getID(aValue, RANDOMIZER.nextBoolean());
		assertArrayEquals(id, result);

		verify(_dummyIndex).getId(aValue);
	}

	/**
	 * A null id must return a null identifier.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithNullInput() throws Exception {
		assertNull(_cut.getValue(null, RANDOMIZER.nextBoolean()));
	}

	/**
	 * A null id must return a null identifier.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValue() throws Exception {
		final byte[] id = new byte[PersistentStringDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		final String aValue = randomString();

		when(_dummyIndex.getValue(id)).thenReturn(aValue);

		assertEquals(aValue, _cut.getValue(id, RANDOMIZER.nextBoolean()));
	}

	/**
	 * Asserts the correct length of the generated identifiers.
	 */
	@Test
	public void IDLength() {
		assertEquals(
				PersistentStringDictionary.ID_LENGTH,
				_cut.newId(randomString(), _dummyIndex).length);
	}

	/**
	 * Remove must remove the value from the underlying index.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValue() throws Exception {
		final String aValue = randomString();

		_cut.removeValue(aValue, RANDOMIZER.nextBoolean());

		verify(_dummyIndex).remove(aValue);
	}
}