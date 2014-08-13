package org.gazzax.labs.jena.nosql.cassandra.graph;

import java.util.ArrayList;
import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.log.MessageFactory;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * Cassandra {@link Graph} implementation.
 * 
 * FIXME: maybe this could be Storage-unaware and therefore should belog to the framework module. 
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraGraph extends GraphBase {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(CassandraGraph.class));
	
	private final static Iterator<byte[][]> EMPTY_ITERATOR = new ArrayList<byte[][]>(0).iterator();
	
	private final TripleIndexDAO dao;
	private final TopLevelDictionary dictionary;
	private final Node name;
	
	/**
	 * Builds a new unnamed graph with the given factory.
	 * 
	 * @param factory the storage layer factory.
	 */
	public CassandraGraph(final StorageLayerFactory factory) {
		this(null, factory);
	}
	
	/**
	 * Builds a new named graph with the given data.
	 * 
	 * @param name the graph name.
	 * @param factory the storage layer factory.
	 */	
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
			final String message = MessageFactory.createMessage(MessageCatalog._00101_UNABLE_TO_ADD_TRIPLE, triple);
			LOGGER.error(message, exception);
			throw new AddDeniedException(message, triple);
		}
	}
	
	@Override
	public void performDelete(final Triple triple) {
		try {
			final byte [][] identifiers = 
				(name == null)
					? dictionary.asIdentifiers(
							triple.getSubject(), 
							triple.getPredicate(), 
							triple.getObject())
					: dictionary.asIdentifiers(
							triple.getSubject(), 
							triple.getPredicate(), 
							triple.getObject(), 
							name);			

			if (triple.isConcrete()) {
				dao.deleteTriple(identifiers);
			} else if (triple.getSubject().isConcrete() && 
					triple.getPredicate().isConcrete() && 
					triple.getObject().isConcrete()){
				clear();
			} else {
				// TODO: batch size must be configurable
				dao.deleteTriples(query(identifiers), 1000);
			}	
		} catch (final StorageLayerException exception) {
			final String message = MessageFactory.createMessage(MessageCatalog._00100_UNABLE_TO_DELETE_TRIPLE, triple);
			LOGGER.error(message, exception);
			throw new DeleteDeniedException(message, triple);
		}
	}
	
	@Override
    public void clear()
	{
	    dao.clear();
        getEventManager().notifyEvent(this, GraphEvents.removeAll ) ;	
	}
	
	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		// TODO Auto-generated method stub
		return null;
	}
	
	Iterator<byte[][]> query(final byte[][] query) throws StorageLayerException {
		return (query != null && query.length >= 3) 
					? dao.query(query)
					: EMPTY_ITERATOR;
	}
}