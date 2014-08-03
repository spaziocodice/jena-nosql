package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.RANDOMIZER;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.STORAGE_LAYER_FACTORY;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildBNode;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildLiteral;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildResource;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;

/**
 * Test case for {@link ThreeTieredValueDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class ThreeTieredValueDictionaryTest {

	final Node aURI = buildResource("http://pippo.pluto.paperino#first");
	final Node aLiteral = buildLiteral("Hello there! It's Gazza!");
	final Node aBNode = buildBNode(String.valueOf(System.currentTimeMillis()));
	
	private ThreeTieredValueDictionary cut;
	private Dictionary<String> namespacesDictionary;
	private Dictionary<String> localNamesDictionary;
	private TopLevelDictionary bnodesAndLiteralsDictionary;
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		namespacesDictionary = mock(Dictionary.class);
		localNamesDictionary = mock(Dictionary.class);
		bnodesAndLiteralsDictionary = mock(TopLevelDictionary.class);

		cut = new ThreeTieredValueDictionary(
				randomString(), 
				namespacesDictionary, 
				localNamesDictionary, 
				bnodesAndLiteralsDictionary);
	}

	/**
	 * Internally managed dictionaries cannot be null.
	 */
	@Test
	public void nullWrappedDictionary() {
		try {
			cut = new ThreeTieredValueDictionary(
					randomString(), 
					null, 
					localNamesDictionary, 
					bnodesAndLiteralsDictionary);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
		
		try {
			cut = new ThreeTieredValueDictionary(
					randomString(), 
					namespacesDictionary, 
					null, 
					bnodesAndLiteralsDictionary);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
		
		try {
			cut = new ThreeTieredValueDictionary(
				randomString(), 
				namespacesDictionary, 
				localNamesDictionary, 
				null);		
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}			
	}
	
	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		final byte[] id = new byte[9];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isBNode(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertFalse(cut.isBNode(id));
		verify(bnodesAndLiteralsDictionary, times(0)).isBNode(id);

		id[0] = ThreeTieredValueDictionary.MARKER + 1;
		cut.isBNode(id);

		verify(bnodesAndLiteralsDictionary).isBNode(id);
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		final byte[] id = new byte[9];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isLiteral(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertFalse(cut.isLiteral(id));
		verify(bnodesAndLiteralsDictionary, times(0)).isLiteral(id);

		id[0] = ThreeTieredValueDictionary.MARKER + 1;
		cut.isLiteral(id);

		verify(bnodesAndLiteralsDictionary).isLiteral(id);
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		final byte[] id = new byte[17];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isResource(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertTrue(cut.isResource(id));

		final byte[] tooShort = new byte[17 - 1];
		final byte[] tooLong = new byte[17 + 1];

		tooShort[0] = ThreeTieredValueDictionary.MARKER;
		tooLong[0] = ThreeTieredValueDictionary.MARKER;

		assertTrue(cut.isResource(tooShort));
		assertTrue(cut.isResource(tooLong));

	}
	/**
	 * On initialisation, decoratee instance must be initialised too.
	 * 
	 * @throws InitialisationException never, otherwise the test fails.
	 */
	@Test
	public void decorateeInitialisation() throws InitialisationException {
		cut.initialise(STORAGE_LAYER_FACTORY);

		verify(namespacesDictionary).initialise(STORAGE_LAYER_FACTORY);
		verify(localNamesDictionary).initialise(STORAGE_LAYER_FACTORY);
		verify(bnodesAndLiteralsDictionary).initialise(STORAGE_LAYER_FACTORY);
	}

	/**
	 * Close must close all managed dictionaries.
	 */
	@Test
	public void close() {
		cut.close();

		verify(namespacesDictionary).close();
		verify(localNamesDictionary).close();
		verify(bnodesAndLiteralsDictionary).close();
	}

	/**
	 * Removing a URI will remove the corresponding entries on namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeURI() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		cut.removeValue(aURI, isPredicate);

		//		final String namespace = ((URI) _aURI).getNamespace();
		//		final String localName = ((URI) _aURI).getLocalName();

		//		verify(_namespacesDictionary).removeValue(namespace, isPredicate);
		//		verify(_localNamesDictionary, times(0)).removeValue(localName, isPredicate);
		//		verify(_bnodesAndLiteralsDictionary, times(0)).removeValue(_aURI, isPredicate);
	}

	/**
	 * Removing a BNode won't involve namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeBNode() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		cut.removeValue(aBNode, isPredicate);

		verify(namespacesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(localNamesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(bnodesAndLiteralsDictionary).removeValue(aBNode, isPredicate);
	}

	/**
	 * Removing a Literal won't involve namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeLiteral() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		cut.removeValue(aLiteral, isPredicate);

		verify(namespacesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(localNamesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(bnodesAndLiteralsDictionary).removeValue(aLiteral, isPredicate);
	}
}