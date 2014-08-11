package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.Constants.CHARSET_UTF8;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.fillIn;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asBlankNode;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asLiteral;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asURIorBlankNode;
import static org.gazzax.labs.jena.nosql.fwk.util.Utility.murmurHash3;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

import com.hp.hpl.jena.graph.Node;

/**
 * Simple implementation of a node dictionary. 
 * Uses MurmurHash3 hashing and linear probing for hash collision resolution.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentNodeDictionary extends TopLevelDictionaryBase {
	static final int ID_LENGTH = 17;

	private BIndex soIndex;
	private BIndex pIndex;

	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public PersistentNodeDictionary(final String id) {
		super(id);
	} 
	
	@Override
	public void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {		
		soIndex = new BIndex("DICT_SO");
		soIndex.initialise(factory);
		
		pIndex = new BIndex("DICT_P");	
		pIndex.initialise(factory);
	}

	@Override
	public void closeInternal() {
		// Nothing to be done here...
	}
	
	@Override
	protected Node getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		final String n3 = getN3(id, p);
		switch(id[0]){
		case RESOURCE_BYTE_FLAG:
			return asURIorBlankNode(n3);
		case LITERAL_BYTE_FLAG:
			return asLiteral(n3);
		default:
			return asBlankNode(n3);
		}
	}

	/**
	 * Returns the identifier of a given N3 resource.
	 * 
	 * @param n3 the resource (N3 representation).
	 * @param p a flag indicating if the resource is a predicate.
	 * @return the identifier of the given resource.
	 * @throws StorageLayerException in case of data access failure. 
	 */
	protected byte[] getID(final String n3, final boolean p) throws StorageLayerException {
		return (n3 == null || n3.isEmpty() || n3.charAt(0) == '?')
				? null
				: p ? pIndex.get(n3) : soIndex.get(n3);
	}

	@Override
	protected byte[] getIdInternal(final Node value, final boolean p) throws StorageLayerException {		
		final String n3 = asNt(value);
		byte[] id = null;

		synchronized (this) {
			
			id = value != null ? getID(n3, p) : null;

			if (id[0] == NOT_SET[0]) {
				final BIndex index = p ? pIndex : soIndex;
				id = newId(value, n3, index);
				index.putQuick(n3, id);
			}
		}
		return id;
	}

	/**
	 * Creates a new identifier for a given resource.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param value the resource. 
	 * @param n3 the N3 representation of resource.
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 * @throws StorageLayerException in case of data access failure. 
	 */
	private byte[] newId(final Node value, final String n3, final BIndex index) throws StorageLayerException {
		byte[] id = makeNewHashID(value, n3);
		for (int i = 0; index.contains(id) && i <= 100; i++) {

			id = resolveHashCollision(id, i);

			if (i == 100) {
				log.error(MessageCatalog._00102_UNABLE_TO_RESOLVE_COLLISION, n3, i);
			}
		}
		return id;
	}

	/**
	 * Creates a new (hash) identifier for the given resource.
	 * 
	 * @param node the resource.
	 * @param n3 the N3 representation of the resource.
	 * @return a new (hash) identifier for the given resource.
	 */
	private static byte[] makeNewHashID(final Node node, final String n3) {

		final byte[] hash = murmurHash3(n3.getBytes(CHARSET_UTF8)).asBytes();

		final ByteBuffer buffer = ByteBuffer.allocate(ID_LENGTH);

		if (node.isLiteral()) {
			buffer.put(LITERAL_BYTE_FLAG);
		} else if (node.isBlank()) {
			buffer.put(BNODE_BYTE_FLAG);
		} else {
			buffer.put(RESOURCE_BYTE_FLAG);
		}

		buffer.put(hash);
		buffer.flip();
		return buffer.array();
	}

	@Override
	public void removeValue(final Node value, final boolean p) throws StorageLayerException {

		if (value != null) {

			final String n3 = asNt(value);

			if (p) {
				pIndex.remove(n3);
			} else {
				soIndex.remove(n3);
			}
		}
	}

	/**
	 * Resolves hash collision.
	 * 
	 * @param id the computed (hash) identifier.
	 * @param step the number of step to use in algorithm.
	 * @return the resolved hash identifier.
	 */
	private byte[] resolveHashCollision(final byte[] id, final int step) {

		final ByteBuffer buffer = ByteBuffer.wrap(id);
		long hash = buffer.getLong(1);
		// linear probing
		buffer.putLong(1, ++hash).flip();
		return buffer.array();
	}

	@Override
	public byte[][] decompose(final byte[] compositeId) {
		if (compositeId != null && compositeId.length > 0) {
			final int howManyValues = compositeId.length / ID_LENGTH;
			final byte[][] tuple = new byte[howManyValues][];
			for (int i = 0; i < howManyValues; i++) {
				tuple[i] = subarray(compositeId, i * ID_LENGTH, ID_LENGTH);
			}
			return tuple;
		}
		return null;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte [] id2) {
		byte [] result = new byte [id1.length + id2.length];
		fillIn(result, 0, id1);
		fillIn(result, id1.length, id2);
		return result;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte [] id2, final byte[] id3) {
		byte [] result = new byte [id1.length + id2.length + id3.length];
		fillIn(result, 0, id1);
		fillIn(result, id1.length, id2);		
		fillIn(result, id1.length + id2.length, id3);		
		return result;
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == BNODE_BYTE_FLAG;
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == LITERAL_BYTE_FLAG;
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == RESOURCE_BYTE_FLAG;
	}

	/**
	 * Returns the N3 representation of the value associated with a given identifier.
	 * 
	 * @param id the value identifier.
	 * @param p the predicate flag.
	 * @return the N3 representation of the value associated with a given identifier.
	 * @throws StorageLayerException in case of data access failure. 
	 */
	String getN3(final byte[] id, final boolean p) throws StorageLayerException {
		if (id == null || id.length == 0) {
			return null;
		}

		final String n3 = p ? pIndex.getQuick(id) : soIndex.getQuick(id);
		if (n3 == null || n3.isEmpty()) {
			log.error(MessageCatalog._00726_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return n3;
	}
}