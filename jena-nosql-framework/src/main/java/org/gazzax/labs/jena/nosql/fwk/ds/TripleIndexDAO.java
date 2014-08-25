package org.gazzax.labs.jena.nosql.fwk.ds;

import java.util.Iterator;
import java.util.List;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;

/**
 * Data Access Object that encapsulates the interaction with a triple index.
 * Each concrete storage implementation must define here how to perform basic triple operations.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @param <T> how this DAO represents a triple.
 */
public interface TripleIndexDAO<T> {
	/**
	 * Inserts a triple.
	 * 
	 * @param identifiers the incoming triple.
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	void insertTriple(final T identifiers) throws StorageLayerException;

	/**
	 * Removes a triple from the storage.
	 * 
	 * @param identifiers the incoming triple.
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	void deleteTriple(final T identifiers) throws StorageLayerException;

	/**
	 * Removes the given triples from the storage.
	 * 
	 * @param identifiers the incoming triples.
	 * @param batchSize how many delete will be grouped in a single chunks in order to optimize the command execution.
	 * @return the triples that have been removed.
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	List<T> deleteTriples(
			final Iterator<T> identifiers, 
			final int batchSize) throws StorageLayerException;

	/**
	 * When a command defines multiple mutations, then at the end the requestor is supposed to call this method.
	 * 
	 * FIXME: Definitely don't like this "client" responsibility: it could forget to call this method.
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	void executePendingMutations() throws StorageLayerException;

	/**
	 * Clears the storage (i.e. remove all triples from the storage).
	 */
	void clear();

	/**
	 * Executes a given query.
	 * 
	 * @param query the pattern query.
	 * @return an iterator over query results.
	 * @throws StorageLayerException in case of storage access failure.
	 */
	Iterator<T> query(T query) throws StorageLayerException;
}