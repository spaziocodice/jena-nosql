package org.gazzax.labs.jena.nosql.solr;

import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * A NullObject dictionary that, as the name suggests, does nothing.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class NoOpDictionary implements TopLevelDictionary {

	@Override
	public void close() {
		// Nothing to be done here...
	}

	@Override
	public byte[] getID(Node node, boolean p) throws StorageLayerException {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Node getValue(byte[] id, boolean p) throws StorageLayerException {
		// Nothing to be done here...
		return null;
	}

	@Override
	public void removeValue(Node value, boolean p) throws StorageLayerException {
		// Nothing to be done here...
	}

	@Override
	public String getName() {
		// Nothing to be done here...
		return null;
	}

	@Override
	public void initialise(final StorageLayerFactory factory) {
		// Nothing to be done here...
	}

	@Override
	public byte[][] asIdentifiers(final Node s, final Node p, final Node o) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public byte[][] asIdentifiers(final Node s, final Node p, final Node o, final Node c) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Triple asTriple(final byte[] s, final byte[] p, final byte[] o) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Quad asQuad(final byte[] s, final byte[] p, final byte[] o, final byte[] c) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public boolean isBNode(final byte[] id) {
		// Nothing to be done here...
		return false;
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		// Nothing to be done here...
		return false;
	}

	@Override
	public boolean isResource(final byte[] id) {
		// Nothing to be done here...
		return false;
	}

	@Override
	public Iterator<byte[][]> asTripleIdentifiersIterator(final Iterator<Triple> triples) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Iterator<byte[][]> asQuadIdentifiersIterator(final Iterator<Quad> quads) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Iterator<Triple> asTripleIterator(final Iterator<byte[][]> identifiers) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public Iterator<Quad> asQuadIterator(final Iterator<byte[][]> quads) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
		// Nothing to be done here...
		return null;
	}

	@Override
	public byte[][] decompose(final byte[] compositeId) {
		// Nothing to be done here...
		return null;
	}
}
