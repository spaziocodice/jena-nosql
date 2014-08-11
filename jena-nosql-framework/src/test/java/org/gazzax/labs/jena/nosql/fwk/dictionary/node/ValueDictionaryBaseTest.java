package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.RANDOMIZER;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.buildResource;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryRuntimeContext;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;

/**
 * Test case for {@link TopLevelDictionaryBase}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class ValueDictionaryBaseTest {

	private TopLevelDictionaryBase _cut;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		_cut = spy(new TopLevelDictionaryBase(randomString()) {

			@Override
			public void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {
				// Nothing
			}

			@Override
			public Node getValueInternal(final byte[] id, final boolean p) {
				return null;
			}

			@Override
			public byte[] getIdInternal(final Node node, final boolean p) {
				return null;
			}

			@Override
			public void closeInternal() {
				// Nothing
			}

			@Override
			public void removeValue(final Node value, final boolean p) {
				// Nothing
			}

			@Override
			public byte[][] decompose(final byte[] compositeId) {
				return null;
			}

			@Override
			public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
				return null;
			}

			@Override
			public byte[] compose(final byte[] id1, final byte[] id2) {
				return null;
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
		});
	}

	/**
	 * The dictionary must own a dictionary runtime context.
	 */
	@Test
	public void dictionaryRuntimeContext() {
		final DictionaryRuntimeContext context = TopLevelDictionaryBase.RUNTIME_CONTEXTS.get();

		final DictionaryRuntimeContext mustBeTheSame = TopLevelDictionaryBase.RUNTIME_CONTEXTS.get();

		assertSame(context, mustBeTheSame);
	}

	/**
	 * getIDs must forward to the getID concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getTripleIDs() throws Exception {
		final Node s = buildResource(randomString());
		final Node p = buildResource(randomString());
		final Node o = buildResource(randomString());

		_cut.asIdentifiers(s, p, o);
		
		verify(_cut).getID(s, false);
		verify(_cut).getID(p, true);
		verify(_cut).getID(o, false);
	}

	/**
	 * getIDs must forward to the getID concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getQuadIDs() throws Exception {
		final Node s = buildResource(randomString());
		final Node p = buildResource(randomString());
		final Node o = buildResource(randomString());
		final Node c = buildResource(randomString());

		_cut.asIdentifiers(s, p, o, c);

		verify(_cut).getID(s, false);
		verify(_cut).getID(p, true);
		verify(_cut).getID(o, false);
		verify(_cut).getID(c, false);
	}
	
	/**
	 * getValues must forward to the getValue concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Ignore
	@Test
	public void getTripleStatement() throws Exception {
		byte[] s = new byte[RANDOMIZER.nextInt(25)];
		byte[] p = new byte[RANDOMIZER.nextInt(25)];
		byte[] o = new byte[RANDOMIZER.nextInt(25)];

		RANDOMIZER.nextBytes(s);
		RANDOMIZER.nextBytes(p);
		RANDOMIZER.nextBytes(o);

		_cut.asTriple(s, p, o);

		verify(_cut).getValue(s, false);
		verify(_cut).getValue(p, true);
		verify(_cut).getValue(o, false);
	}

	/**
	 * getValues must forward to the getValue concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Ignore
	@Test
	public void getQuadStatement() throws Exception {
		byte[] s = new byte[RANDOMIZER.nextInt(25)];
		byte[] p = new byte[RANDOMIZER.nextInt(25)];
		byte[] o = new byte[RANDOMIZER.nextInt(25)];
		byte[] c = new byte[RANDOMIZER.nextInt(25)];

		RANDOMIZER.nextBytes(s);
		RANDOMIZER.nextBytes(p);
		RANDOMIZER.nextBytes(o);
		RANDOMIZER.nextBytes(c);

		_cut.asQuad(s, p, o, c);

		verify(_cut).getValue(s, false);
		verify(_cut).getValue(p, true);
		verify(_cut).getValue(o, false);
		verify(_cut).getValue(c, false);
	}
}