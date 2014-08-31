package org.gazzax.labs.jena.nosql.cassandra.dao;

import static org.gazzax.labs.jena.nosql.cassandra.Table.O_SPC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.PO_SC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.S_POC;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.AbstractIterator;

/**
 * Cassandra 2x (CQL-based) implementation of {@link GraphDAO}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraTripleIndexDAO implements GraphDAO<byte[][], byte[][]> {
	protected static final byte[] EMPTY_VAL = new byte[0]; 
	protected static final String SELECT_SPOC_FROM = "SELECT s, p, o, c FROM ";
	
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

	private PreparedStatement[] queries;
	
	private int deletionBatchSize;
	
	/**
	 * Buils a new {@link CassandraTripleIndexDAO} with the given data.
	 * 
	 * @param deletionBatchSize the batch size used in deletions.
	 * @param session The connection to Cassandra.
	 */
	public CassandraTripleIndexDAO(
			final Session session, 
			final int deletionBatchSize) {
		this.session = session;
		this.deletionBatchSize = deletionBatchSize;
		
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
	public List<byte[][]> deleteTriples(final Iterator<byte[][]> nodes) throws StorageLayerException {

		final List<byte[][]> deleted = new ArrayList<byte[][]>(deletionBatchSize);
		
		while (nodes.hasNext()) {
			for (int i = 0; i < deletionBatchSize && nodes.hasNext(); i++) {

				byte[][] ids = nodes.next();
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
	public Iterator<byte[][]> query(final byte[][] query) throws StorageLayerException {
		int qindex = (query[0] == null) ? 4 : 0;
		qindex += (query[1] == null) ? 2 : 0;
		qindex += (query[2] == null) ? 1 : 0;
		
		final BoundStatement statement = queries[qindex].bind();			
		int index = 0;
		for (final byte[] binding : query) {
			if (binding != null) {
				statement.setBytesUnsafe(index++, ByteBuffer.wrap(binding));
			}
		}

		final Iterator<Row> iterator = session.executeAsync(statement).getUninterruptibly().iterator();
		return new AbstractIterator<byte[][]>() {
			@Override
			protected byte[][] computeNext() {
				return iterator.hasNext() ? asByteArray(iterator.next()) : endOfData();
			}
		};
	}	
	
	/**
	 * Transforms the given row in a byte array containing term identifiers.
	 * 
	 * @param row the row.
	 * @return a byte array containing term identifiers.
	 */
	private byte[][] asByteArray(final Row row) {
		final byte[] s = Bytes.getArray(row.getBytesUnsafe(0));
		final byte[] p = Bytes.getArray(row.getBytesUnsafe(1));
		final byte[] o = Bytes.getArray(row.getBytesUnsafe(2));
		final ByteBuffer c = row.getBytesUnsafe(3);
		return (c != null)
			? new byte[][] {s, p, o}
			: new byte[][] {s, p, o, Bytes.getArray(c)};
	}
	
	@Override
	public void clear() {
		session.execute(clearSPOCStatement.bind());
		session.execute(clearOSPCStatement.bind());
		session.execute(clearPOSCStatement.bind());
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

		queries = new PreparedStatement[] {
				session.prepare(SELECT_SPOC_FROM + S_POC + " WHERE s = ? AND p = ? AND o = ?"),
				session.prepare(SELECT_SPOC_FROM + S_POC + " WHERE s = ? AND p = ?"),
				session.prepare(SELECT_SPOC_FROM + O_SPC + " WHERE s = ? AND o = ?"),  
				session.prepare(SELECT_SPOC_FROM + S_POC + " WHERE s = ?"),
				session.prepare(SELECT_SPOC_FROM + PO_SC + " WHERE p = ? AND o = ?"),
				session.prepare(SELECT_SPOC_FROM + PO_SC + " WHERE p_index = ?"),
				session.prepare(SELECT_SPOC_FROM + O_SPC + " WHERE o = ?"),
				session.prepare(SELECT_SPOC_FROM + S_POC)
		};
	}
		
	/**
	 * Internal method used for reuse delete stuff.
	 * 
	 * @param ids the triple identifiers.
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

	@Override
	public long countTriples() throws StorageLayerException {
		// TODO Auto-generated method stub
		return 0;
	}
}