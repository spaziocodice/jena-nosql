package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.concat;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Node_URI;

import static org.gazzax.labs.jena.nosql.fwk.util.Utility.*;

/**
 * A dictionary with a good compromise between I/O and in-memory work.
 * Basically, values are managed differently depending on their nature:
 * 
 * <ul>
 * 	<li>Namespaces and local names are managed using two different (String) dictionaries;</li>
 * 	<li>Blank nodes and literals are managed using a dedicated {@link TopLevelDictionary}.</li>
 * </ul>
 *  
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class ThreeTieredNodeDictionary extends TopLevelDictionaryBase {
	static final byte MARKER = 30;

	private final Dictionary<String> namespaces;
	private final Dictionary<String> localNames;

	private final TopLevelDictionary bNodesAndLiterals;
	
	/**
	 * Builds a new dictionary with given (sub)dictionaries.
	 * 
	 * @param id the dictionary identifier.
	 * @param namespaces the dictionary that will be used for namespaces.
	 * @param localNames the dictionary that will be used for local names.
	 * @param bNodesAndLiterals the dictionary that will be used for local names and other kind of resources.
	 */
	public ThreeTieredNodeDictionary(
			final String id,
			final Dictionary<String> namespaces,
			final Dictionary<String> localNames,
			final TopLevelDictionary bNodesAndLiterals) {
		super(id);
		
		if (namespaces == null || localNames == null || bNodesAndLiterals == null) {
			throw new IllegalArgumentException(MessageCatalog._00165_NULL_DECORATEE_DICT);
		}
		
		this.namespaces = namespaces;
		this.localNames = localNames;
		this.bNodesAndLiterals = bNodesAndLiterals;
	}
	
	@Override
	protected void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {		
		namespaces.initialise(factory);
		localNames.initialise(factory);
		bNodesAndLiterals.initialise(factory);
	}

	@Override
	protected byte[] getIdInternal(final Node value, final boolean p) throws StorageLayerException {
		if (value.isURI()) {
			final Node_URI uri = (Node_URI) value;
			byte[] namespaceId = namespaces.getID(namespace(uri), p);
			byte[] localNameId = localNames.getID(localName(uri), p);
			return concat(MARKER, namespaceId, localNameId);
		} else {
			return bNodesAndLiterals.getID(value, p);
		}
	}

	@Override
	protected Node getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		if (id[0] == MARKER) {
			return NodeFactory.createURI(
					new StringBuilder()
					.append(namespaces.getValue(subarray(id, 1, 8), p))
					.append(localNames.getValue(subarray(id, 9, id.length - 9), p))
					.toString());
		} else {
			return bNodesAndLiterals.getValue(id, p);
		}
	}

	@Override
	public void removeValue(final Node value, final boolean p) throws StorageLayerException {
		if (value != null && !value.isURI()) {
			bNodesAndLiterals.removeValue(value, p);
		}
	}

	@Override
	protected void closeInternal() {
		namespaces.close();
		localNames.close();
		bNodesAndLiterals.close();
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] != MARKER && bNodesAndLiterals.isBNode(id);
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id[0] != MARKER && bNodesAndLiterals.isLiteral(id);
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id[0] == MARKER;
	}
}