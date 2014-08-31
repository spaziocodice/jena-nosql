package org.gazzax.labs.jena.nosql.fwk.graph;

import java.util.ArrayList;
import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.log.MessageFactory;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * NoSQL Graph implementation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class NoSqlGraph extends GraphBase {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(NoSqlGraph.class));
	
	private static final Iterator<byte[][]> EMPTY_IDS_ITERATOR = new ArrayList<byte[][]>(0).iterator();
	private static final ExtendedIterator<Triple> EMPTY_TRIPLES_ITERATOR = WrappedIterator.createNoRemove(new ArrayList<Triple>(0).iterator());
	
	private final GraphDAO<byte[][], byte[][]> dao;
	private final TopLevelDictionary dictionary;
	private final Node name;
	
	/**
	 * Builds a new unnamed graph with the given factory.
	 * 
	 * @param factory the storage layer factory.
	 */
	public NoSqlGraph(final StorageLayerFactory factory) {
		this(null, factory);
	}
	
	/**
	 * Builds a new named graph with the given data.
	 * 
	 * @param name the graph name.
	 * @param factory the storage layer factory.
	 */	
	@SuppressWarnings("unchecked")
	public NoSqlGraph(final Node name, final StorageLayerFactory factory) {
		this.name = name;
		this.dao = name != null ? factory.getGraphDAO(name) : factory.getGraphDAO();
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
			} else if (!triple.getSubject().isConcrete() && !triple.getPredicate().isConcrete() && !triple.getObject().isConcrete()) {
				clear();
			} else {
				dao.deleteTriples(query(identifiers));
			}	
		} catch (final StorageLayerException exception) {
			final String message = MessageFactory.createMessage(MessageCatalog._00100_UNABLE_TO_DELETE_TRIPLE, triple);
			LOGGER.error(message, exception);
			throw new DeleteDeniedException(message, triple);
		}
	}
	
	@Override
    public void clear() {
	    dao.clear();
        getEventManager().notifyEvent(this, GraphEvents.removeAll);
	}
	
	@Override
	protected ExtendedIterator<Triple> graphBaseFind(final TripleMatch pattern) {
		try {
			final byte [][] identifiers = 
				(name == null)
					? dictionary.asIdentifiers(
							pattern.getMatchSubject(), 
							pattern.getMatchPredicate(), 
							pattern.getMatchObject())
					: dictionary.asIdentifiers(
							pattern.getMatchSubject(), 
							pattern.getMatchPredicate(), 
							pattern.getMatchObject(), 
							name);		
			return WrappedIterator.createNoRemove(dictionary.asTripleIterator(query(identifiers)));
		} catch (StorageLayerException exception) {
			LOGGER.error(MessageCatalog._00010_DATA_ACCESS_LAYER_FAILURE, exception);
			return EMPTY_TRIPLES_ITERATOR;
		}
	}
	
	/**
	 * Executes a query using a given triple pattern.
	 *  
	 * @param query the query (as pattern).
	 * @return an iterator of resulting triples.
	 * @throws StorageLayerException in case of storage access layer.
	 */
	Iterator<byte[][]> query(final byte[][] query) throws StorageLayerException {
		return (query != null && query.length >= 3) 
					? dao.query(query)
					: EMPTY_IDS_ITERATOR;
	}
	
	@Override
	public void close() {
		dictionary.close();
		closed = true;
	}
}