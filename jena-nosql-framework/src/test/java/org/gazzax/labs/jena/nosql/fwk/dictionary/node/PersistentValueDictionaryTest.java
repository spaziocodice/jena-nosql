package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.RANDOMIZER;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link PersistentValueDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentValueDictionaryTest {

	private PersistentValueDictionary cut;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		cut = new PersistentValueDictionary(randomString());
	}

	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isBNode(null));
		assertFalse("Wrong id length.", cut.isBNode(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", cut.isBNode(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = Dictionary.RESOURCE_BYTE_FLAG;
		assertFalse(cut.isBNode(id));

		id[0] = Dictionary.LITERAL_BYTE_FLAG;
		assertFalse(cut.isBNode(id));

		id[0] = Dictionary.BNODE_BYTE_FLAG;
		assertTrue(cut.isBNode(id));
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isBNode(null));
		assertFalse("Wrong id length.", cut.isLiteral(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", cut.isLiteral(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = Dictionary.RESOURCE_BYTE_FLAG;
		assertFalse(cut.isLiteral(id));

		id[0] = Dictionary.BNODE_BYTE_FLAG;
		assertFalse(cut.isLiteral(id));

		id[0] = Dictionary.LITERAL_BYTE_FLAG;
		assertTrue(cut.isLiteral(id));
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(cut.isResource(null));
		assertFalse("Wrong id length.", cut.isResource(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", cut.isResource(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = Dictionary.LITERAL_BYTE_FLAG;
		assertFalse(cut.isResource(id));

		id[0] = Dictionary.BNODE_BYTE_FLAG;
		assertFalse(cut.isResource(id));

		id[0] = Dictionary.RESOURCE_BYTE_FLAG;
		assertTrue(cut.isResource(id));
	}
}