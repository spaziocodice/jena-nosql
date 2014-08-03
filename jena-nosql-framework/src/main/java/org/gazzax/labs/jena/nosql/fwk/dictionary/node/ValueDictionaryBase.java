package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.decodeShort;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.encode;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.fillIn;

import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryBase;
import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryRuntimeContext;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

import com.google.common.collect.AbstractIterator;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * Base for dictionary implementations.
 * Provides shared and common behaviour for concrete dictionary implementors.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class ValueDictionaryBase extends DictionaryBase<Node> implements TopLevelDictionary {

	protected static final ThreadLocal<DictionaryRuntimeContext> RUNTIME_CONTEXTS = new ThreadLocal<DictionaryRuntimeContext>() {
		protected DictionaryRuntimeContext initialValue() {
			return new DictionaryRuntimeContext();
		};
	};

	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public ValueDictionaryBase(final String id) {
		super(id);
	}
	
	@Override
	public byte[][] asIdentifiers(final Node s, final Node p, final Node o) throws StorageLayerException {
		return new byte[][] {
				getID(s, false),
				getID(p, true),
				getID(o, false) };
	}

	@Override
	public byte[][] asIdentifiers(final Node s, final Node p, final Node o, final Node c) throws StorageLayerException {
		return new byte[][] {
				getID(s, false),
				getID(p, true),
				getID(o, false),
				getID(c, false) };
	}

	@Override
	public Triple asTriple(final byte[] s, final byte[] p, final byte[] o) throws StorageLayerException {
		return new Triple(
				getValue(s, false),
				getValue(p, true),
				getValue(o, false));
	}

	@Override
	public Quad asQuad(final byte[] s, final byte[] p, final byte[] o, final byte[] c) throws StorageLayerException {
		return new Quad(
				getValue(c, false),
				getValue(s, false), 
				getValue(p, true), 
				getValue(o, false));
	}

	@Override
	public Iterator<byte[][]> asQuadIdentifiersIterator(final Iterator<Quad> quads) {
		return new AbstractIterator<byte[][]>() {

			@Override
			protected byte[][] computeNext() {

				if (!quads.hasNext()) {
					return endOfData();
				}

				final Quad quad = quads.next();

				try {
					return new byte[][] {
						getID(quad.getSubject(), false),
						getID(quad.getPredicate(), true),
						getID(quad.getObject(), false),
						getID(quad.getGraph(), false) };
				} catch (StorageLayerException exception) {
					log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
					return endOfData();
				}
			}
		};
	}

	@Override
	public Iterator<byte[][]> asTripleIdentifiersIterator(final Iterator<Triple> triples) {

		return new AbstractIterator<byte[][]>() {

			@Override
			protected byte[][] computeNext() {

				if (!triples.hasNext()) {
					return endOfData();
				}

				final Triple Triple = triples.next();

				try {
					return new byte[][] {
						getID(Triple.getSubject(), false),
						getID(Triple.getPredicate(), true),
						getID(Triple.getObject(), false) };
				} catch (final StorageLayerException exception) {
					log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
					return endOfData();
				}
			}
		};
	}

	@Override
	public Iterator<Quad> asQuadIterator(final Iterator<byte[][]> quads) {
		return new AbstractIterator<Quad>() {

			@Override
			protected Quad computeNext() {

				while (quads.hasNext()) {

					final byte[][] ids = quads.next();

					try {
						return asQuad(ids[0], ids[1], ids[2], ids[3]);
					} catch (StorageLayerException exception) {
						log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
						return endOfData();
					}
				}

				return endOfData();
			}
		};
	}

	@Override
	public Iterator<Triple> asTripleIterator(final Iterator<byte[][]> triples) {

		return new AbstractIterator<Triple>() {

			@Override
			protected Triple computeNext() {

				while (triples.hasNext()) {

					final byte[][] ids = triples.next();
					try {
						return asTriple(ids[0], ids[1], ids[2]);
					} catch (StorageLayerException exception) {
						log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
						return endOfData();
					}
				}

				return endOfData();
			}
		};
	}

	@Override
	public byte[][] decompose(final byte[] compositeId) {
		final short howManySubIdentifiers = decodeShort(compositeId, 0);
		byte[][] tuple = new byte[howManySubIdentifiers][];
		int offset = 2;
		for (int i = 0; i < howManySubIdentifiers; i++) {
			final int length = decodeShort(compositeId, offset);
			offset += 2;

			byte[] id = new byte[length];
			fillIn(id, 0, compositeId, offset, length);
			offset += length;

			tuple[i] = id;
		}
		return tuple;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2) {
		if (id1 == null || id2 == null) {
			throw new IllegalArgumentException("Both identifiers must be not null.");
		}
		byte[] result = new byte[2 + 2 + id1.length + 2 + id2.length];
		encode(2, result, 0);
		encode(id1.length, result, 2);
		fillIn(result, 2 + 2, id1, id1.length);
		encode(id2.length, result, 2 + 2 + id1.length);
		fillIn(result, 2 + 2 + id1.length + 2, id2, id2.length);
		return result;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
		if (id1 == null || id2 == null) {
			throw new IllegalArgumentException("All identifiers must be not null.");
		}
		byte[] result = new byte[2 + 2 + id1.length + 2 + id2.length + 2 + id3.length];
		encode(3, result, 0);
		encode(id1.length, result, 2);
		fillIn(result, 2 + 2, id1, id1.length);
		encode(id2.length, result, 2 + 2 + id1.length);
		fillIn(result, 2 + 2 + id1.length + 2, id2, id2.length);
		encode(id3.length, result, 2 + 2 + id1.length + 2 + id2.length);
		fillIn(result, 2 + 2 + id1.length + 2 + id2.length + 2, id3, id3.length);
		return result;
	}
}