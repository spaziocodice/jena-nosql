package org.gazzax.labs.jena.nosql.cassandra.dao;

import static org.gazzax.labs.jena.nosql.cassandra.Table.O_SPC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.PO_SC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.S_POC;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * Cassandra 2x (CQL-based) implementation of {@link TripleIndexDAO}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraTripleIndexDAO implements TripleIndexDAO {
	protected final static byte[] EMPTY_VAL = new byte[0]; 
	
	protected final Session session;
	
	protected final ThreadLocal<BatchStatement> batchStatements = new ThreadLocal<BatchStatement>() {
		protected BatchStatement initialValue() {
			return new BatchStatement();
		}
	};
	
	private PreparedStatement _insertSPOCStatement;
	private PreparedStatement _insertOSPCStatement;
	private PreparedStatement _insertPOSCStatement;

	private PreparedStatement _deleteSPOCStatement;
	private PreparedStatement _deleteOSPCStatement;
	private PreparedStatement _deletePOSCStatement;

	private PreparedStatement _clearSPOCStatement;
	private PreparedStatement _clearOSPCStatement;
	private PreparedStatement _clearPOSCStatement;
	private PreparedStatement _clearNSPOStatement;
	private PreparedStatement _clearNPOSStatement;
	private PreparedStatement _clearDSPOStatement;
	private PreparedStatement _clearDPOSStatement;

	// Filled with the 8 query types used for the triple store.
	// To calculate the position of a query in the array, convert the triple pattern to binary.
	// If S is variable, add 4, if P is variable, add 2, if O is variable, add 1.
	private PreparedStatement[] _queries;
	
	protected final TopLevelDictionary dictionary;
	
	/**
	 * Buils a new dao with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param dictionary the dictionary currently used in the owning store instance.
	 */
	public CassandraTripleIndexDAO(final Session session, final TopLevelDictionary dictionary) {
		this.session = session;
		this.dictionary = dictionary;
		prepareStatements();
	}


	/* (non-Javadoc)
	 * @see org.gazzax.labs.jena.nosql.cassandra.dao.TripleIndexDAO#insertTriple(byte[][])
	 */
	@Override
	public void insertTriple(final byte[][] ids) throws StorageLayerException {
		// insert in CF_PO_SC
		final BoundStatement poscStatement = _insertPOSCStatement.bind();

		poscStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[1]));
		poscStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[2]));
		poscStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[0]));

		if (ids.length == 4) {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		poscStatement.setBytesUnsafe(4, ByteBuffer.wrap(ids[1]));

		batchStatements.get().add(poscStatement);

		// insert in CF_S_POC
		BoundStatement spocStatement = _insertSPOCStatement.bind();

		spocStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[0]));
		spocStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[1]));
		spocStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(spocStatement);

		// insert in CF_O_SPC
		final BoundStatement ospcStatement = _insertOSPCStatement.bind();

		ospcStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[2]));
		ospcStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[0]));
		ospcStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[1]));

		if (ids.length == 4) {
			ospcStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			ospcStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}
		
		batchStatements.get().add(ospcStatement);
	}


	/* (non-Javadoc)
	 * @see org.gazzax.labs.jena.nosql.cassandra.dao.TripleIndexDAO#deleteTriple(byte[][])
	 */
	@Override
	public void deleteTriple(final byte[][] ids) throws StorageLayerException {
		internalDelete(ids);
		executePendingMutations();
	}
	
	/* (non-Javadoc)
	 * @see org.gazzax.labs.jena.nosql.cassandra.dao.TripleIndexDAO#deleteTriples(java.util.Iterator, int)
	 */
	@Override
	public List<byte[][]> deleteTriples(
			final Iterator<byte[][]> nodes, 
			final int batchSize) throws StorageLayerException {

		final List<byte[][]> deleted = new ArrayList<byte[][]>(batchSize);
		
		while (nodes.hasNext()) {
			for (int i = 0; i < batchSize && nodes.hasNext(); i++) {

				byte[][] ids = nodes.next();

				// check if valid triple or quad
				if (ids == null || ids.length < 3) {
					continue;
				}

				internalDelete(ids);
				
				deleted.add(ids);
				executePendingMutations();
			}
		}
		
		return deleted;
	}

	@Override
	public void executePendingMutations() throws StorageLayerException {
		try {
			session.execute(batchStatements.get());
			batchStatements.get().clear();
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}


//	public Iterator<byte[][]> query(final byte[][] query, final int limit) throws StorageLayerException {
//		final BoundStatement statement = _queries[getQueryIndex(query)].bind();
//
//		// Fill the query
//		int queryVariableIndex = 0;
//		for (int i = 0; i < 3; i++) {
//			if (!isVariable(query[i])) {
//				statement.setBytesUnsafe(queryVariableIndex++, ID_SERIALIZER.serialize(query[i]));
//			}
//		}
//
//		// Set the limit, it is always the last variable
//		statement.setInt(queryVariableIndex, limit);
//
//		// Execute query and convert result set to ids
//		return new SPOCResultIterator(_session.executeAsync(statement), true);
//	}

	/* (non-Javadoc)
	 * @see org.gazzax.labs.jena.nosql.cassandra.dao.TripleIndexDAO#clear()
	 */
	@Override
	public void clear() {
		session.execute(_clearSPOCStatement.bind());
		session.execute(_clearOSPCStatement.bind());
		session.execute(_clearPOSCStatement.bind());
		session.execute(_clearNSPOStatement.bind());
		session.execute(_clearNPOSStatement.bind());
		session.execute(_clearDSPOStatement.bind());
		session.execute(_clearDPOSStatement.bind());
	}
	
	/**
	 * Prepares statements.
	 */
	protected void prepareStatements() {
		final int ttl = -1;//_factory.getTtl();
		
		_insertSPOCStatement = session.prepare("INSERT INTO " + S_POC + "(s, p, o, c) VALUES (?, ?, ?, ?)");
		_insertOSPCStatement = session.prepare("INSERT INTO " + O_SPC + "(o, s, p, c) VALUES (?, ?, ?, ?)");
		_insertPOSCStatement = session.prepare("INSERT INTO " + PO_SC + "(p, o, s, c, p_index) VALUES (?, ?, ?, ?, ?)");

		_deleteSPOCStatement = session.prepare("DELETE FROM " + S_POC + " WHERE s = ? AND p = ? AND o = ? AND c = ?");
		_deleteOSPCStatement = session.prepare("DELETE FROM " + O_SPC + " WHERE o = ? AND s = ? AND p = ? AND c = ?");
		_deletePOSCStatement = session.prepare("DELETE FROM " + PO_SC + " WHERE p = ? AND o = ? AND s = ? AND c = ?");

		_clearSPOCStatement = session.prepare("TRUNCATE " + S_POC);
		_clearOSPCStatement = session.prepare("TRUNCATE " + O_SPC);
		_clearPOSCStatement = session.prepare("TRUNCATE " + PO_SC);

		// Querying
		_queries = new PreparedStatement[8];
		_queries[0] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ? AND p = ? AND o = ? LIMIT ?"); // (s, p, o)
		_queries[1] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ? AND p = ?           LIMIT ?"); // (s, p, ?)
		_queries[2] = session.prepare("SELECT s, p, o, c FROM " + O_SPC + " WHERE s = ?           AND o = ? LIMIT ?"); // (s, ?, o)
		_queries[3] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ?                     LIMIT ?"); // (s, ?, ?)
		_queries[4] = session.prepare("SELECT s, p, o, c FROM " + PO_SC + " WHERE           p = ? AND o = ? LIMIT ?"); // (?, p, o)
		_queries[5] = session.prepare("SELECT s, p, o, c FROM " + PO_SC + " WHERE           p_index = ?     LIMIT ?"); // (?, p, ?)
		_queries[6] = session.prepare("SELECT s, p, o, c FROM " + O_SPC + " WHERE                     o = ? LIMIT ?"); // (?, ?, o)
		_queries[7] = session.prepare("SELECT s, p, o, c FROM " + S_POC + "                                 LIMIT ?"); // (?, ?, ?)
	}	
	
//	/**
//	 * Returns the index of the prepared statement to handle a given triple pattern query.
//	 * 
//	 * @param triplePattern The triple pattern query.
//	 * @return The index of the prepared statement to handle a given triple pattern query.
//	 */
//	int getQueryIndex(final byte[][] triplePattern) {
//		int index = 0;
//
//		if (isVariable(triplePattern[0])) {
//			index += 4;
//		}
//
//		if (isVariable(triplePattern[1])) {
//			index += 2;
//		}
//
//		if (isVariable(triplePattern[2])) {
//			index += 1;
//		}
//
//		return index;
//	}	
	
	/**
	 * Returns the index of the prepared statement to handle the range query with the given parameters.
	 * 
	 * @param reverse True if the result should be returned reversed, false if it should be returned normally.
	 * @param subjectIsVariable True if the subject of the query is variable, false if it is set.
	 * @param typeIsDouble True if the type of the range is double, false if it is date.
	 * @param upperBoundIsOpen True if the upper bound is smaller-than relation, false if it is a smaller-than-or-equal-to relation.
	 * @param lowerBoundIsOpen True if the lower bound is greater-than relation, false if it is a greater-than-or-equal-to relation.
	 * @return The index of the prepared statement.
	 */
	int getRangeQueryIndex(
			final boolean reverse, 
			final boolean subjectIsVariable, 
			final boolean typeIsDouble, 
			final boolean upperBoundIsOpen,
			final boolean lowerBoundIsOpen) {
		int index = 0;

		if (reverse) {
			index += 16;
		}

		if (subjectIsVariable) {
			index += 8;
		}

		if (typeIsDouble) {
			index += 4;
		}

		if (upperBoundIsOpen) {
			index += 2;
		}

		if (lowerBoundIsOpen) {
			index += 1;
		}

		return index;
	}	
	
	/**
	 * Internal method used for reuse delete stuff.
	 * 
	 * @param ids the triple identifiers.
	 * @param rangesEnabled if ranges have been enabled on the current store.
	 * @throws StorageLayerException in case of data access failure.
	 */
	void internalDelete(final byte [][]ids) throws StorageLayerException {
		// delete in CF_PO_SC
		final BoundStatement poscStatement = _deletePOSCStatement.bind();
		poscStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[1]));
		poscStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[2]));
		poscStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[0]));

		if (ids.length == 4) {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(poscStatement);

		// delete in CF_S_POC
		final BoundStatement spocStatement = _deleteSPOCStatement.bind();
		spocStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[0]));
		spocStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[1]));
		spocStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(spocStatement);

		// delete in CF_O_SPC
		final BoundStatement ospcStatement = _deleteOSPCStatement.bind();
		ospcStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[2]));
		ospcStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[0]));
		ospcStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[1]));

		if (ids.length == 4) {
			ospcStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			ospcStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(ospcStatement);
	}
}