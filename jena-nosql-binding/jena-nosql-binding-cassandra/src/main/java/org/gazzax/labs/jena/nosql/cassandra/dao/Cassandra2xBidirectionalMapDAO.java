package org.gazzax.labs.jena.nosql.cassandra.dao;

import static org.gazzax.labs.jena.nosql.fwk.util.Utility.murmurHash3;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.gazzax.labs.jena.nosql.cassandra.serializer.Serializer;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.AbstractIterator;

/**
 * A DAO that is able to do bidirectional lookups with any value size.
 * This is a workaround for a limit of Cassandra. In Cassandra, anything that gets indexed must not be greater than 64KB.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class Cassandra2xBidirectionalMapDAO<K, V> implements MapDAO<K, V> {

	private static final Log LOG = new Log(LoggerFactory.getLogger(Cassandra2xBidirectionalMapDAO.class));

	private final Session session;
	private final String tableName;

	private final Serializer<K> _keySerializer;
	private final Serializer<V> _valueSerializer;

	private V defaultValue;

	private PreparedStatement _insertStatement;
	private PreparedStatement _deleteStatement;
	private PreparedStatement _getValueStatement;
	private PreparedStatement _getKeyStatement;
	private PreparedStatement _getAllStatement;
	private PreparedStatement _checkHashStatement;

	/**
	 * Creates a new bidirectional DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param ttl The TTL for the entries.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xBidirectionalMapDAO(
			final Session session, 
			final String tableName, 
			final Serializer<K> keySerializer, 
			final Serializer<V> valueSerializer) {
		this.tableName = tableName;
		this.session = session;
		_keySerializer = keySerializer;
		_valueSerializer = valueSerializer;
	}

	@Override
	public void createRequiredSchemaEntities() throws StorageLayerException {
		session.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (key BLOB, valueHash LONG, value BLOB, PRIMARY KEY (key))"
				+ " WITH compaction = {'class': 'LeveledCompactionStrategy'}"
				+ " AND compression = {'sstable_compression' : 'SnappyCompressor'}");

		session.execute("CREATE INDEX IF NOT EXISTS " + tableName + "_value_index ON " + tableName + " (valueHash)");

		_insertStatement = session.prepare("INSERT INTO " + tableName + " (key, valueHash, value) VALUES (?, ?, ?)");

		_deleteStatement = session.prepare("DELETE FROM " + tableName + " WHERE key = ?");
		_getValueStatement = session.prepare("SELECT value FROM " + tableName + " WHERE key = ?");
		_getKeyStatement = session.prepare("SELECT key FROM " + tableName + " WHERE valueHash = ?");
		_getAllStatement = session.prepare("SELECT key, value FROM " + tableName);
		_checkHashStatement = session.prepare("SELECT value FROM " + tableName + " WHERE valueHash = ?");
	}

	@Override
	public boolean contains(final K key) {
		BoundStatement getValueStatement = _getValueStatement.bind();
		getValueStatement.setBytesUnsafe(0, _keySerializer.serialize(key));

		return session.execute(getValueStatement).getAvailableWithoutFetching() > 0;
	}

	@Override
	public V get(final K key) {
		BoundStatement getValueStatement = _getValueStatement.bind();
		getValueStatement.setBytesUnsafe(0, _keySerializer.serialize(key));

		ByteBuffer result = session.execute(getValueStatement).one().getBytesUnsafe(0);

		if (result != null) {
			return _valueSerializer.deserialize(result);
		} else {
			return null;
		}
	}

	@Override
	public K getKey(final V value) {
		if (value == null) {
			return null;
		}

		long valueHash = getValueHash(value);
		BoundStatement getKeyStatement = _getKeyStatement.bind(valueHash);

		ByteBuffer result = session.execute(getKeyStatement).one().getBytesUnsafe(0);

		if (result != null) {
			return _keySerializer.deserialize(result);
		} else {
			return null;
		}
	}

	@Override
	public Iterator<K> keyIterator() {
		BoundStatement getAllStatement = _getAllStatement.bind();
		final Iterator<Row> rowIterator = session.execute(getAllStatement).iterator();

		return new AbstractIterator<K>() {
			@Override
			protected K computeNext() {
				if (rowIterator.hasNext()) {
					return _keySerializer.deserialize(rowIterator.next().getBytesUnsafe(0));
				} else {
					return endOfData();
				}
			}
		};
	}

	@Override
	public Set<K> keySet() {
		Set<K> keys = new HashSet<K>();

		for (Iterator<K> iter = keyIterator(); iter.hasNext();) {
			keys.add(iter.next());
		}

		return keys;
	}

	@Override
	public void setDefaultValue(final V defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * Calculates the hash of this value. If the calculated hash collides, the collision is solved.
	 * If the given object is already inserted, the hash of the inserted object will be returned.
	 * 
	 * @param value The value to be hashed.
	 * @return A non-colliding hash of the value.
	 */
	private long getValueHash(final V value) {
		byte[] serializedValue = _valueSerializer.serializeDirect(value);
		long hash = murmurHash3(serializedValue).asLong();

		boolean hashFound = false;

		for (int iterations = 0; iterations < 100 && !hashFound; hash++, iterations++) {
			BoundStatement checkHashStatement = _checkHashStatement.bind(hash);
			Row result = session.execute(checkHashStatement).one();

			if (result == null) {
				hashFound = true;
			} else {
				V mappedValue = _valueSerializer.deserialize(result.getBytesUnsafe(0));

				if (_valueSerializer.isEqual(value, mappedValue)) {
					hashFound = true;
				}
			}
		}

		if (hashFound) {
			return hash;
		} else {
			LOG.error(MessageCatalog._00098_COULD_NOT_GET_HASH, value);
			return -1;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) {
		BatchStatement batchStatement = new BatchStatement();
		
		for (K key : keys) {
			BoundStatement deleteStatement = _deleteStatement.bind();
			deleteStatement.setBytesUnsafe(0, _keySerializer.serialize(key));
			batchStatement.add(deleteStatement);
		}
		
		session.execute(batchStatement);
	}

	@Override
	public void set(final K key, final V value) {
		BoundStatement insertStatement = _insertStatement.bind();
		insertStatement.setBytesUnsafe(0, _keySerializer.serialize(key));
		insertStatement.setLong(1, getValueHash(value));
		insertStatement.setBytesUnsafe(2, _valueSerializer.serialize(value));
		session.execute(insertStatement);
	}

	@Override
	public void setAll(final Map<K, V> pairs) {
		for (Map.Entry<K, V> e : pairs.entrySet()) {
			set(e.getKey(), e.getValue());
		}
	}
}