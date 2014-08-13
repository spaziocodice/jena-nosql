package org.gazzax.labs.jena.nosql.fwk;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.*;
import static org.mockito.Mockito.*;

/**
 * Test case for {@link BIndex}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class BIndexTestCase {
	private BIndex cut;
	private PersistentKeyValueMap<String, byte[]> byValue;
	private PersistentKeyValueMap<byte[], String> byId;
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {
		cut = new BIndex(randomString());
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
}
