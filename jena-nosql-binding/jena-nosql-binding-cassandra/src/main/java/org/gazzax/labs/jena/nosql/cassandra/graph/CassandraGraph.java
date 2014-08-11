package org.gazzax.labs.jena.nosql.cassandra.graph;

import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * Cassandra {@link Graph} implementation.
 * 
 * FIXME: maybe this could be Storage unaware and therefore should fall within the framework module. 
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraGraph extends GraphBase {
	
	private final TripleIndexDAO dao;
	private final TopLevelDictionary dictionary;
	private final Node name;
	
	public CassandraGraph(final StorageLayerFactory factory) {
		this(null, factory);
	}
	
	public CassandraGraph(final Node name, final StorageLayerFactory factory) {
		this.name = name;
		this.dao = factory.getTripleIndexDAO();
		this.dictionary = factory.getDictionary();
	}
	
	@Override
	public void performAdd(final Triple triple) {
		try {
			final byte [][] ids = 
				(name == null)
				 	? dictionary.asIdentifiers(triple.getSubject(), triple.getPredicate(), triple.getObject())
				 	: dictionary.asIdentifiers(triple.getSubject(), triple.getPredicate(), triple.getObject(), name);		
			dao.insertTriple(ids);
			dao.executePendingMutations();
		} catch (final StorageLayerException exception) {
			throw new AddDeniedException("", triple);
		}
	}
	
	@Override
	public void performDelete(final Triple triple) {
		try {
			final byte [][] identifiers = 
				(name == null)
					? dictionary.asIdentifiers(triple.getSubject(), triple.getPredicate(), triple.getObject())
					: dictionary.asIdentifiers(triple.getSubject(), triple.getPredicate(), triple.getObject(), name);			

			if (triple.isConcrete()) {
				dao.deleteTriple(identifiers);
			} else if ( (triple.getSubject() == Node.ANY) && (triple.getPredicate() == Node.ANY) && (triple.getObject() == Node.ANY)){
				clear();
			} else {
				// TODO: batch size must be configurable
				dao.deleteTriples(query(identifiers), 1000);
			}	
		} catch (final StorageLayerException exception) {
			throw new AddDeniedException("", triple);
		}
	}
	
	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		// TODO Auto-generated method stub
		return null;
	}
	
	Iterator<byte[][]> query(final byte[][] query) {
		return null;
	}
}