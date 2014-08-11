package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

/**
 * Supertype layer for dictionaries that are backed by a single {@link Index}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SingleIndexNodeDictionary extends TopLevelDictionaryBase {
	protected BIndex index;
	protected final String indexName;

	/**
	 * Builds a new dictionary with a given index name.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the index name.
	 */
	protected SingleIndexNodeDictionary(final String id, final String indexName) {
		super(id);
		this.indexName = indexName;
	}

	@Override
	protected void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {
		index = createIndex();
		index.initialise(factory);
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
				: index.get(n3);
	}
	
	/**
	 * Resolves hash collision.
	 * 
	 * @param id the computed (hash) identifier.
	 * @param step the number of step to use in algorithm.
	 * @return the resolved hash identifier.
	 */
	protected byte[] resolveHashCollision(final byte[] id, final int step) {

		final ByteBuffer buffer = ByteBuffer.wrap(id);
		long hash = buffer.getLong(1);
		buffer.putLong(1, ++hash).flip();
		return buffer.array();
	}
	
	/**
	 * Creates a new identifier for a given resource.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param n3 the N3 representation of resource.
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 * @throws StorageLayerException in case of data access failure.
	 */
	protected byte[] newId(final String n3, final BIndex index) throws StorageLayerException {
		byte[] id = makeNewHashID(n3);
		for (int i = 0; index.contains(id) && i <= 100; i++) {

			id = resolveHashCollision(id, i);

			if (i == 100) {
				log.error(MessageCatalog._00102_UNABLE_TO_RESOLVE_COLLISION, n3, i);
			}
		}
		return id;
	}

	/**
	 * Makes a new hash identifier.
	 * 
	 * @param n3 the N3 representation of a given resource.
	 * @return the identifier associated with a given resource.
	 */
	protected abstract byte[] makeNewHashID(final String n3);

	/**
	 * Creates the underlying index.
	 * 
	 * @return the underlying index.
	 */
	protected BIndex createIndex() {
		return new BIndex(indexName);
	}
	
	/**
	 * Returns the N3 representation of the value associated with a given identifier.
	 * 
	 * @param id the value identifier.
	 * @param p the predicate flag.
	 * @return the N3 representation of the value associated with a given identifier.
	 * @throws StorageLayerException in case of data access layer factory. 
	 */
	protected String getN3(final byte[] id, final boolean p) throws StorageLayerException {
		final String n3 = index.getQuick(id);
		if (n3 == null || n3.isEmpty()) {
			log.error(MessageCatalog._00726_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return n3;
	}	
}