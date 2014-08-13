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
 */
public interface TripleIndexDAO {
	/**
	 * Inserts a triple.
	 * 
	 * @param identifiers the incoming triple as an array of identifiers (s,p,o or s,p,o,c)
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	void insertTriple(final byte[][] identifiers) throws StorageLayerException;

	/**
	 * Removes a triple from the storage.
	 * 
	 * @param identifiers the incoming triple as an array of identifiers (s,p,o or s,p,o,c)
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	void deleteTriple(final byte[][] identifiers) throws StorageLayerException;

	/**
	 * Removes the given triples from the storage.
	 * 
	 * @param identifiers the incoming triples. Each triple is an array of identifiers (s,p,o or s,p,o,c)
	 * @param batchSize how many delete will be grouped in a single chunks in order to optimize the command execution.
	 * @return the triples that have been removed.
	 * @throws StorageLayerException in case of storage layer access failure.
	 */
	List<byte[][]> deleteTriples(
			final Iterator<byte[][]> identifiers, 
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

	Iterator<byte[][]> query(byte[][] query) throws StorageLayerException;
}