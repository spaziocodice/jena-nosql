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
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
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
	
	private PreparedStatement insertSPOCStatement;
	private PreparedStatement insertOSPCStatement;
	private PreparedStatement insertPOSCStatement;

	private PreparedStatement deleteSPOCStatement;
	private PreparedStatement deleteOSPCStatement;
	private PreparedStatement deletePOSCStatement;

	private PreparedStatement clearSPOCStatement;
	private PreparedStatement clearOSPCStatement;
	private PreparedStatement clearPOSCStatement;
	private PreparedStatement clearNSPOStatement;
	private PreparedStatement clearNPOSStatement;
	private PreparedStatement clearDSPOStatement;
	private PreparedStatement clearDPOSStatement;

	private PreparedStatement[] queries;
	
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

	@Override
	public void insertTriple(final byte[][] ids) throws StorageLayerException {
		final BoundStatement poscStatement = insertPOSCStatement.bind();

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

		final BoundStatement spocStatement = insertSPOCStatement.bind();

		spocStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[0]));
		spocStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[1]));
		spocStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(spocStatement);

		final BoundStatement ospcStatement = insertOSPCStatement.bind();

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

	@Override
	public void deleteTriple(final byte[][] ids) throws StorageLayerException {
		internalDelete(ids);
		executePendingMutations();
	}
	
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

	@Override
	public void clear() {
		session.execute(clearSPOCStatement.bind());
		session.execute(clearOSPCStatement.bind());
		session.execute(clearPOSCStatement.bind());
		session.execute(clearNSPOStatement.bind());
		session.execute(clearNPOSStatement.bind());
		session.execute(clearDSPOStatement.bind());
		session.execute(clearDPOSStatement.bind());
	}
	
	/**
	 * Initializes PreparedStatements.
	 */
	protected void prepareStatements() {
		
		insertSPOCStatement = session.prepare("INSERT INTO " + S_POC + "(s, p, o, c) VALUES (?, ?, ?, ?)");
		insertOSPCStatement = session.prepare("INSERT INTO " + O_SPC + "(o, s, p, c) VALUES (?, ?, ?, ?)");
		insertPOSCStatement = session.prepare("INSERT INTO " + PO_SC + "(p, o, s, c, p_index) VALUES (?, ?, ?, ?, ?)");

		deleteSPOCStatement = session.prepare("DELETE FROM " + S_POC + " WHERE s = ? AND p = ? AND o = ? AND c = ?");
		deleteOSPCStatement = session.prepare("DELETE FROM " + O_SPC + " WHERE o = ? AND s = ? AND p = ? AND c = ?");
		deletePOSCStatement = session.prepare("DELETE FROM " + PO_SC + " WHERE p = ? AND o = ? AND s = ? AND c = ?");

		clearSPOCStatement = session.prepare("TRUNCATE " + S_POC);
		clearOSPCStatement = session.prepare("TRUNCATE " + O_SPC);
		clearPOSCStatement = session.prepare("TRUNCATE " + PO_SC);

		queries = new PreparedStatement[8];
		queries[0] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ? AND p = ? AND o = ? LIMIT ?");  
		queries[1] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ? AND p = ?           LIMIT ?");  
		queries[2] = session.prepare("SELECT s, p, o, c FROM " + O_SPC + " WHERE s = ? AND o = ? LIMIT ?");  
		queries[3] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " WHERE s = ? LIMIT ?");  
		queries[4] = session.prepare("SELECT s, p, o, c FROM " + PO_SC + " WHERE p = ? AND o = ? LIMIT ?"); 
		queries[5] = session.prepare("SELECT s, p, o, c FROM " + PO_SC + " WHERE p_index = ?     LIMIT ?"); 
		queries[6] = session.prepare("SELECT s, p, o, c FROM " + O_SPC + " WHERE o = ? LIMIT ?"); 
		queries[7] = session.prepare("SELECT s, p, o, c FROM " + S_POC + " LIMIT ?");  
	}	
	
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
		final BoundStatement poscStatement = deletePOSCStatement.bind();
		poscStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[1]));
		poscStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[2]));
		poscStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[0]));

		if (ids.length == 4) {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			poscStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(poscStatement);

		final BoundStatement spocStatement = deleteSPOCStatement.bind();
		spocStatement.setBytesUnsafe(0, ByteBuffer.wrap(ids[0]));
		spocStatement.setBytesUnsafe(1, ByteBuffer.wrap(ids[1]));
		spocStatement.setBytesUnsafe(2, ByteBuffer.wrap(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ByteBuffer.wrap(EMPTY_VAL));
		}

		batchStatements.get().add(spocStatement);
		
		final BoundStatement ospcStatement = deleteOSPCStatement.bind();
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