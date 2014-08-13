package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildBNode;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildLiteral;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildResource;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.decodeShort;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.fillIn;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Random;

import org.gazzax.labs.jena.nosql.fwk.Constants;
import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.util.NTriples;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;

/**
 * Test case for {@link TransientNodeDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class TransientValueDictionaryTestCase {

	final Node firstMember = buildResource("http://pippo.pluto.paperino#first");
	final Node secondMember = buildResource("http://pippo.pluto.paperino#second");
	final Node thirdMember = buildResource("http://pippo.pluto.paperino#third");
	final Node fourthMember = buildLiteral("Hello there! It's Gazza!");
	final Node fifthMember = buildBNode(String.valueOf(System.currentTimeMillis()));

	Node longLiteral;
	
	byte[] longLiteralIdGeneratedByEmbeddedDictionary;
	byte[] longLiteralId;
	
	private TransientNodeDictionary cut;
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Before
	public void setUp() throws Exception {

		longLiteralId = new byte[PersistentNodeDictionary.ID_LENGTH + 1];
		longLiteralId[0] = TransientNodeDictionary.THRESHOLD_EXCEEDED;
		longLiteralId[1] = TopLevelDictionaryBase.LITERAL_BYTE_FLAG;

		longLiteralIdGeneratedByEmbeddedDictionary = new byte[PersistentNodeDictionary.ID_LENGTH];
		longLiteralIdGeneratedByEmbeddedDictionary[0] = TopLevelDictionaryBase.LITERAL_BYTE_FLAG;
		for (int i = 1; i < PersistentNodeDictionary.ID_LENGTH; i++) {
			longLiteralIdGeneratedByEmbeddedDictionary[i] = (byte) i;
		}

		for (int i = 1; i < PersistentNodeDictionary.ID_LENGTH; i++) {
			longLiteralId[i + 1] = (byte) i;
		}

		cut = new TransientNodeDictionary(randomString())
		{
			@Override
			public void initialiseInternal(final StorageLayerFactory factory) {
				longLiteralsDictionary = new TopLevelDictionaryBase(randomString()) {
					
					@Override
					public void removeValue(final Node value, final boolean p) {
						// Nothing here
					}

					@Override
					public byte[] getIdInternal(final Node value, final boolean p) {
						return longLiteralIdGeneratedByEmbeddedDictionary;
					}

					@Override
					public void initialiseInternal(final StorageLayerFactory factory) {
						// Nothing to be done here...
					}

					@Override
					public byte[][] decompose(final byte[] compositeId) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public byte[] compose(final byte[] id1, final byte[] id2) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public void closeInternal() {
						// Never called for this test.
					}

					@Override
					public Node getValueInternal(final byte[] id, final boolean p) {
						return longLiteral;
					}

					@Override
					public boolean isBNode(final byte[] id) {
						return false;
					}

					@Override
					public boolean isLiteral(final byte[] id) {
						return false;
					}

					@Override
					public boolean isResource(final byte[] id) {
						return false;
					}
				};
			}
		};

		cut.initialise(STORAGE_LAYER_FACTORY);
		
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < cut.threshold + 1; i++) {
			builder.append('a');
		}

		longLiteral = buildLiteral(builder.toString());
	}
	
	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		assertFalse(cut.isBNode(null));
		assertFalse(cut.isBNode(new byte[] { TransientNodeDictionary.THRESHOLD_EXCEEDED }));
		assertFalse(cut.isBNode(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.LITERAL_BYTE_FLAG }));
		assertFalse(cut.isBNode(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.RESOURCE_BYTE_FLAG }));
		assertTrue(cut.isBNode(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.BNODE_BYTE_FLAG }));
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		assertFalse(cut.isLiteral(null));
		assertFalse(cut.isLiteral(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.BNODE_BYTE_FLAG }));
		assertFalse(cut.isLiteral(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.RESOURCE_BYTE_FLAG }));

		assertTrue(cut.isLiteral(new byte[] { TransientNodeDictionary.THRESHOLD_EXCEEDED }));
		assertTrue(cut.isLiteral(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.LITERAL_BYTE_FLAG }));
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		assertFalse(cut.isResource(null));
		assertFalse(cut.isResource(new byte[] { TransientNodeDictionary.THRESHOLD_EXCEEDED }));
		assertFalse(cut.isResource(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.LITERAL_BYTE_FLAG }));
		assertFalse(cut.isResource(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.BNODE_BYTE_FLAG }));

		assertTrue(cut.isResource(new byte[] { TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED, Dictionary.RESOURCE_BYTE_FLAG }));
	}

	/**
	 * Long literal dictionary cannot be null.
	 */
	@Test
	public void nullLongLiteralDictionary() {
		try {
			cut = new TransientNodeDictionary(randomString(), null, 0);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing to be done...this is the expected behaviour
		}
	}

	/**
	 * On initialisation, decoratee instance must be initialised too.
	 * 
	 * @throws InitialisationException never, otherwise the test fails.
	 */
	@Test
	public void decorateeInitialisation() throws InitialisationException {
		final TopLevelDictionary decoratee = mock(TopLevelDictionary.class);
		
		cut = new TransientNodeDictionary(randomString(), decoratee, TransientNodeDictionary.DEFAULT_THRESHOLD);
		cut.initialise(STORAGE_LAYER_FACTORY);
		
		verify(decoratee).initialise(STORAGE_LAYER_FACTORY);
	}
	
	/**
	 * Remove() method itself has no effect on the {@link TransientNodeDictionary} unless the given value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueWithoutInvolvingLongLiteralDictionary() throws Exception {
		final Node uri = buildResource("http://example.org#it");
		final TopLevelDictionary decoratee = mock(TopLevelDictionary.class);
		
		cut = new TransientNodeDictionary(randomString(), decoratee, TransientNodeDictionary.DEFAULT_THRESHOLD);
		cut.removeValue(uri, false);
		
		verify(decoratee, times(0)).removeValue(uri, false);
	}
	
	/**
	 * Remove() method itself has no effect on the {@link TransientNodeDictionary} unless the given value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueInvolvesLongLiteralDictionary() throws Exception {
		final byte[] persistentId = { TransientNodeDictionary.THRESHOLD_EXCEEDED, 2, 3, 4, 5, 6, 7, 78, 8, 9 };

		final TopLevelDictionary decoratee = mock(TopLevelDictionary.class);
		
		when(decoratee.getID(longLiteral, false)).thenReturn(persistentId);
		
		cut = new TransientNodeDictionary(randomString(), decoratee, TransientNodeDictionary.DEFAULT_THRESHOLD);
		cut.removeValue(longLiteral, false);
		
		verify(decoratee).removeValue(longLiteral, false);		
	}

	/**
	 * Passing 0 as literal threshold length actually disables the wrapped dictionary.
	 */
	@Test
	public void disableLongLiteralDictionary() {
		cut = new TransientNodeDictionary(randomString(), new PersistentNodeDictionary(randomString()), 0);
		assertEquals(Integer.MAX_VALUE, cut.threshold);
		assertTrue(cut.longLiteralsDictionary instanceof PersistentNodeDictionary);		
	}

	/**
	 * Passing -1 (or a negative number) as literal threshold length actually uses the default value.
	 */
	@Test
	public void useDefaultLongLiteralThreshold() {
		final Random random = new Random();
		for (int i = 0; i < random.nextInt(10) + 1; i++) {
			final int negativeThreshold = (random.nextInt(1000) + 1) * -1;
			cut = new TransientNodeDictionary(randomString(), new PersistentNodeDictionary(randomString()), negativeThreshold);
			assertEquals(TransientNodeDictionary.DEFAULT_THRESHOLD, cut.threshold);
			assertTrue(cut.longLiteralsDictionary instanceof PersistentNodeDictionary);					
		}
	}

	/**
	 * In case of default constructor, default values are {@link PersistentNodeDictionary} for long literals and 1K as threshold.
	 */
	@Test
	public void defaultValues() {
		cut = new TransientNodeDictionary(randomString());
		assertEquals(TransientNodeDictionary.DEFAULT_THRESHOLD, cut.threshold);
		assertTrue(cut.longLiteralsDictionary instanceof PersistentNodeDictionary);
	}
	
	/**
	 * Closing the dictionary will trigger a close() on embedded dictionary.
	 */
	@Test
	public void close() {
		final TopLevelDictionary dictionary = mock(TopLevelDictionary.class);

		cut.longLiteralsDictionary = dictionary;
		cut.close();

		verify(dictionary).close();
	}

	/**
	 * The value is a literal and its length is over the configured threshold.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDForLongLiterals() throws Exception {
		assertArrayEquals(longLiteralId, cut.getID(longLiteral, false));
	}
	
	/**
	 * Tests the identifier creation method.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getID() throws Exception {
		assertIdEncodingWithoutThresholdExceeding(
				firstMember, 
				TopLevelDictionaryBase.RESOURCE_BYTE_FLAG);
		assertIdEncodingWithoutThresholdExceeding(
				buildBNode("4356682"), 
				TopLevelDictionaryBase.BNODE_BYTE_FLAG);
		assertIdEncodingWithoutThresholdExceeding(
				buildLiteral("This is a literal"), 
				TopLevelDictionaryBase.LITERAL_BYTE_FLAG);
	}

	/**
	 * If the input value is null then the getID must return null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDWithNullValue() throws Exception {	
		assertNull(cut.getID(null, false));
	}

	/**
	 * Tests the creation of the N3 resource representation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getN3() throws Exception {
		byte[] firstMemberId = cut.getID(firstMember, false);
		assertEquals(firstMember, cut.getValue(firstMemberId, false));
	}

	/**
	 * Tests the value retrieval.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValue() throws Exception {
		byte[] firstMemberId = cut.getID(firstMember, false);
		assertEquals(firstMember, cut.getValue(firstMemberId, false));

		byte[] fourthMemberId = cut.getID(fourthMember, false);
		assertEquals(fourthMember, cut.getValue(fourthMemberId, false));

		byte[] fifthMemberId = cut.getID(fifthMember, false);
		assertTrue(cut.getValue(fifthMemberId, false).isBlank());
	}

	/**
	 * N3 for long literals.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getN3WithLongLiteral() throws Exception {
		byte[] id = cut.getID(longLiteral, false);
		assertArrayEquals(longLiteralId, id);
		assertEquals(longLiteral, cut.getValue(id, false));
	}

	/**
	 * Value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithLongLiteral() throws Exception {
		byte[] id = cut.getID(longLiteral, false);
		assertArrayEquals(longLiteralId, id);
		assertEquals(longLiteral, cut.getValue(id, false));
	}
	
	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithTwoMembers() throws Exception {
		final byte[] firstId = cut.getID(firstMember, false);
		final byte[] secondId = cut.getID(secondMember, false);
		
		final byte [] compositeId = cut.compose(firstId, secondId);
		
		final byte[][] ids = cut.decompose(compositeId);
		
		assertEquals(2, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
	}

	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithThreeMembers() throws Exception {
		final byte[] firstId = cut.getID(firstMember, false);
		final byte[] secondId = cut.getID(secondMember, false);
		final byte[] thirdId = cut.getID(thirdMember, false);
		
		final byte [] compositeId = cut.compose(firstId, secondId, thirdId);
		
		final byte[][] ids = cut.decompose(compositeId);
		
		assertEquals(3, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
		assertArrayEquals(thirdId, ids[2]);
	}

	/**
	 * No input identifier can be null when creating composite identifiers.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithNullArguments() throws Exception {	
		assertInvalidArgumentsForIdComposition(null, null);
		assertInvalidArgumentsForIdComposition(cut.getID(firstMember, false), null);
		assertInvalidArgumentsForIdComposition(null, cut.getID(firstMember, false));

		assertInvalidArgumentsForIdComposition(null, null, null);
		assertInvalidArgumentsForIdComposition(cut.getID(firstMember, false), null, null);
		assertInvalidArgumentsForIdComposition(null, cut.getID(firstMember, false), null);
		assertInvalidArgumentsForIdComposition(null, null, cut.getID(firstMember, false));
	}	
		
	/**
	 * Tests the composite identifier build with two members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithTwoParts() throws Exception {	
		final byte[] firstId = cut.getID(firstMember, false);
		final byte[] secondId = cut.getID(secondMember, false);
		
		final byte [] compositeId = cut.compose(firstId, secondId);
		
		int expectedLength = 
				2 					// how many id
				+ 2					// 1st id length
				+ firstId.length	// 1st id
				+ 2					// 2nd id length
				+ secondId.length;	// 2nd id
		assertEquals(expectedLength, compositeId.length);
		
		assertEquals(2, decodeShort(compositeId, 0));

		int offset = 2;
		assertEquals(firstId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(firstId, subarray(compositeId, offset, firstId.length));

		offset += firstId.length;
		assertEquals(secondId.length, decodeShort(compositeId, offset));
		
		offset += 2;		
		assertArrayEquals(secondId, subarray(compositeId, offset, secondId.length));
	}

	/**
	 * Tests the composite identifier build with three members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithThreeParts() throws Exception {			
		final byte[] firstId = cut.getID(firstMember, false);
		final byte[] secondId = cut.getID(secondMember, false);
		final byte[] thirdId = cut.getID(thirdMember, false);
		
		final byte [] compositeId = cut.compose(firstId, secondId, thirdId);
		
		int expectedLength = 
				2 					// how many id
				+ 2					// 1st id length
				+ firstId.length	// 1st id
				+ 2					// 2nd id length
				+ secondId.length	// 2nd id
				+ 2					// 3rd id length
				+ thirdId.length;	// 3rd id
		assertEquals(expectedLength, compositeId.length);
		
		assertEquals(3, decodeShort(compositeId, 0));

		int offset = 2;
		assertEquals(firstId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(firstId, subarray(compositeId, offset, firstId.length));

		offset += firstId.length;
		assertEquals(secondId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(secondId, subarray(compositeId, offset, secondId.length));

		offset += secondId.length;
		assertEquals(thirdId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(thirdId, subarray(compositeId, offset, thirdId.length));	
	}
	
	/**
	 * Internal method used for assert identifier encoding over several kind of resources.
	 * Note that this method tests only identifiers generated without involving the internal dictionary 
	 * (the one used for long literals).
	 * 
	 * @param value the value to be encoded.
	 * @param marker a byte that indicates the kind of resource we are encoding.
	 * @throws Exception never otherwise the test fails.
	 */
	private void assertIdEncodingWithoutThresholdExceeding(final Node value, final byte marker) throws Exception {
		final String n3 = NTriples.asNt(value);
		final byte[] binary = n3.getBytes(Constants.CHARSET_UTF8);
		final byte[] expected = new byte[binary.length + 2];
		expected[0] = TransientNodeDictionary.THRESHOLD_NOT_EXCEEDED;
		expected[1] = marker;
		
		fillIn(expected, 2, binary);
		
		assertArrayEquals(expected, cut.getID(value, false));
	}
	
	/**
	 * Internal method used for asserting a failure in case of invalid input arguments.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second idenrtifier.
	 */
	private void assertInvalidArgumentsForIdComposition(final byte[] id1, final byte[] id2) {
		try {
			cut.compose(id1, id2);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}
	
	/**
	 * Internal method used for asserting a failure in case of invalid input arguments.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second idenrtifier.
	 * @param id3 the third identifier.
	 */
	private void assertInvalidArgumentsForIdComposition(final byte[] id1, final byte[] id2, final byte[] id3) {
		try {
			cut.compose(id1, id2, id3);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}	
}