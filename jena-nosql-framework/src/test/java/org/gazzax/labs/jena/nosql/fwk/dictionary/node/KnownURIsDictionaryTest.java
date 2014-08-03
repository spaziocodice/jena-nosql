package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.RANDOMIZER;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildBNode;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildLiteral;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildResource;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.decodeShort;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.util.NTriples;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.vocabulary.DOAP;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * Test case for {@link KnownURIsDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class KnownURIsDictionaryTest {

	private KnownURIsDictionary cut;
	private TopLevelDictionary decoratee;
	private BIndex dummyIndex;
	private boolean isPredicate;

	private final Node firstMember = NodeFactory.createURI("http://pippo.pluto.paperino#first");

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception {
		isPredicate = RANDOMIZER.nextBoolean();

		dummyIndex = mock(BIndex.class);

		decoratee = mock(TopLevelDictionary.class);
		cut = new KnownURIsDictionary(randomString(), decoratee) {

			@Override
			protected BIndex createIndex() {
				return dummyIndex;
			}
		};

		cut.initialise(STORAGE_LAYER_FACTORY);
		verify(decoratee).initialise(STORAGE_LAYER_FACTORY);
	}

	/**
	 * Using zero-args constructor or specifying null / zero domains or as input parameter means "use default predefined domains".
	 */
	@Test
	public void useDefaultDomains() {
		assertArrayEquals(KnownURIsDictionary.DEFAULT_DOMAINS, cut.domains);

		final String[][] noDomains = { null, new String[] {} };
		for (String[] domains : noDomains) {
			cut = new KnownURIsDictionary(randomString(), decoratee, domains);
			assertArrayEquals(KnownURIsDictionary.DEFAULT_DOMAINS, cut.domains);
		}
	}

	/**
	 * Using the appropriate constructor, it is possible to define a custom set of domains.
	 */
	@Test
	public void useInjectedDomains() {
		final String[] customDomains = { FOAF.NS, DOAP.NS};

		cut = new KnownURIsDictionary(randomString(), decoratee, customDomains);

		assertArrayEquals(customDomains, cut.domains);
	}

	/**
	 * A null decoratee will raise an exception when the dictionary is intialized.
	 */
	@Test
	public void nullDecoratee() {
		try {
			cut = new KnownURIsDictionary(randomString(), null);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing. This is the expected behaviour.
		}

		try {
			cut = new KnownURIsDictionary(randomString(), null, FOAF.NS);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing. This is the expected behaviour.
		}
	}

	/**
	 * A request is received for a literal or a bnode. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithIgnorableValue() throws Exception {
		final Node[] ignorableValues = { buildLiteral(randomString()), buildBNode(randomString()) };

		for (final Node value : ignorableValues) {
			cut.getID(value, isPredicate);
			verify(decoratee).getID(value, isPredicate);
		}
	}

	/**
	 * A request is received for a URI with a namespace not belonging to managed domains. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithIgnorableURI() throws Exception {
		final String[] ignorableNamespaces = {
				"http://pippo.pluto.paperino/",
				"http://this.should.not.be.already.managed",
				"http://also.this.shouldnt"
		};

		for (final String ignorableNamespace : ignorableNamespaces) {
			assertFalse(cut.contains(ignorableNamespace));

			final Node ignorableURI = buildResource(ignorableNamespace + randomString());

			cut.getID(ignorableURI, isPredicate);
			verify(decoratee).getID(ignorableURI, isPredicate);
		}
	}

	/**
	 * A request is received for a URI with a namespace that belongs to managed domains. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithManagedURI() throws Exception {
		final String[] managedNamespaces = {
				FOAF.NS,
				RDFS.getURI(),
				OWL.NS
		};

		for (final String managedNamespace : managedNamespaces) {
			assertTrue(cut.contains(managedNamespace));

			final Node uri = buildResource(managedNamespace + randomString());

			final String n3 = NTriples.asNtURI(uri);

			// Make sure the mock index returns "Sorry, we don't have such value".
			when(dummyIndex.get(n3)).thenReturn(ValueDictionaryBase.NOT_SET);

			// 1. ask for uri.
			byte[] id = cut.getID(uri, isPredicate);

			// 2. make sure the identifier is well-formed.
			assertEquals(KnownURIsDictionary.ID_LENGTH, id.length);
			assertEquals(KnownURIsDictionary.KNOWN_URI_MARKER, id[0]);
			assertEquals(ValueDictionaryBase.RESOURCE_BYTE_FLAG, id[1]);

			// 3. make sure the decoratee wasn't involved in identifier creation.
			verify(decoratee, times(0)).getID(uri, isPredicate);
			verify(dummyIndex).putQuick(n3, id);

			reset(decoratee, dummyIndex);
		}
	}

	/**
	 * Identifiers for bnodes and literals must be handled by the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithIgnorableId() throws Exception {
		final int length = RANDOMIZER.nextInt(KnownURIsDictionary.ID_LENGTH) + 10;
		byte[] ignorableId = new byte[length];

		RANDOMIZER.nextBytes(ignorableId);

		cut.getValue(ignorableId, isPredicate);
		verify(decoratee).getValue(ignorableId, isPredicate);
	}

	/**
	 * If input identifier is null then result is null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithNullInput() throws Exception {
		assertNull(cut.getValue(null, isPredicate));
	}

	/**
	 * If input value is null then identifier is null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithNullInput() throws Exception {
		assertNull(cut.getID((Node) null, isPredicate));
	}

	/**
	 * Identifiers for managed URI must be be directly served by the dictionary, without involving the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getURIWithManagedId() throws Exception {
		final Node managedUri = buildResource(FOAF.NS + System.currentTimeMillis());

		final String n3 = NTriples.asNtURI(managedUri);

		when(dummyIndex.getQuick(any(byte[].class))).thenReturn(n3);
		when(dummyIndex.get(n3)).thenReturn(ValueDictionaryBase.NOT_SET);

		byte[] id = cut.getID(managedUri, isPredicate);

		final Node value = cut.getValue(id, isPredicate);
		assertEquals(managedUri, value);

		verify(decoratee, times(0)).getValue(id, isPredicate);
	}

	/**
	 * A remove on an ignorable value must call the remove on the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeIgnorableValue() throws Exception {
		final Node[] ignorableValues = { buildLiteral(randomString()), buildBNode(randomString()) };

		for (final Node value : ignorableValues) {
			cut.removeValue(value, isPredicate);
			verify(decoratee).removeValue(value, isPredicate);
		}
	}

	/**
	 * A remove on an ignorable URI must call the remove on the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeIgnorableURI() throws Exception {
		final String[] ignorableNamespaces = {
				"http://pippo.pluto.paperino/",
				"http://this.should.not.be.already.managed",
				"http://also.this.shouldnt"
		};

		for (final String ignorableNamespace : ignorableNamespaces) {
			assertFalse(cut.contains(ignorableNamespace));

			final Node ignorableURI = buildResource(ignorableNamespace + randomString());

			cut.removeValue(ignorableURI, isPredicate);
			verify(decoratee).removeValue(ignorableURI, isPredicate);
		}
	}

	/**
	 * Tests remove() method with managed URI.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeManagedURI() throws Exception {
		final String[] managedNamespaces = {
				FOAF.NS,
				RDFS.getURI(),
				OWL.NS};

		for (final String managedNamespace : managedNamespaces) {
			assertTrue(cut.contains(managedNamespace));

			final Node uri = buildResource(managedNamespace + randomString());
			final String n3 = NTriples.asNtURI(uri);

			cut.removeValue(uri, isPredicate);
			verify(dummyIndex).remove(n3);

			reset(dummyIndex);
		}
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
	 * Tests the composite identifier build with three members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithThreeParts() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5, 6, 7 };
		final byte[] secondId = { 11, 21, 31, 41, 51, 61, 71 };
		final byte[] thirdId = { 12, 22, 32, 42, 52, 62, 72, 82 };

		final byte[] compositeId = cut.compose(firstId, secondId, thirdId);

		int expectedLength =
				2 // how many id
						+ 2 // 1st id length
						+ firstId.length // 1st id
						+ 2 // 2nd id length
						+ secondId.length // 2nd id
						+ 2 // 3rd id length
						+ thirdId.length; // 3rd id
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
	 * Tests the composite identifier build with two members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithTwoParts() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5, 6, 7 };
		final byte[] secondId = { 11, 21, 31, 41, 51, 61, 71 };

		final byte[] compositeId = cut.compose(firstId, secondId);

		int expectedLength =
				2 // how many id
						+ 2 // 1st id length
						+ firstId.length // 1st id
						+ 2 // 2nd id length
						+ secondId.length; // 2nd id
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
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithThreeMembers() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4 };
		final byte[] secondId = { 5, 6, 7, 8 };
		final byte[] thirdId = { 9, 10, 11, 12 };

		final byte[] compositeId = cut.compose(firstId, secondId, thirdId);

		final byte[][] ids = cut.decompose(compositeId);

		assertEquals(3, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
		assertArrayEquals(thirdId, ids[2]);
	}

	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithTwoMembers() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5 };
		final byte[] secondId = { 6, 5, 4, 3, 2, 1 };

		final byte[] compositeId = cut.compose(firstId, secondId);

		final byte[][] ids = cut.decompose(compositeId);

		assertEquals(2, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
	}

	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isBNode(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		assertFalse(cut.isBNode(id));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		cut.isBNode(id);

		verify(decoratee).isBNode(id);
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isLiteral(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		assertFalse(cut.isLiteral(id));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		cut.isLiteral(id);

		verify(decoratee).isLiteral(id);
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isResource(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		cut.isResource(id);
		verify(decoratee).isResource(id);
		reset(decoratee);

		id[1] = Dictionary.RESOURCE_BYTE_FLAG + 1;
		cut.isResource(id);
		verify(decoratee).isResource(id);
		reset(decoratee);

		final byte[] tooShort = new byte[KnownURIsDictionary.ID_LENGTH - 1];
		final byte[] tooLong = new byte[KnownURIsDictionary.ID_LENGTH + 1];

		tooShort[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		tooLong[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		tooShort[1] = Dictionary.RESOURCE_BYTE_FLAG;
		tooLong[1] = Dictionary.RESOURCE_BYTE_FLAG;

		cut.isResource(tooShort);
		verify(decoratee).isResource(tooShort);
		reset(decoratee);

		cut.isResource(tooLong);
		verify(decoratee).isResource(tooLong);
		reset(decoratee);

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		id[1] = Dictionary.RESOURCE_BYTE_FLAG;

		assertTrue(cut.isResource(id));
		verify(decoratee, times(0)).isResource(id);
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