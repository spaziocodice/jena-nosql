package org.gazzax.labs.jena.nosql.cassandra.dao;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.gazzax.labs.jena.nosql.cassandra.serializer.Serializer;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.AbstractIterator;

/**
 * A map DAO that operates on a Cassandra table.
 * 
 * @author Sebastian Schmidt
 * @since 1.0.0
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class Cassandra2xMapDAO<K, V> implements MapDAO<K, V> {

	private final Log _log = new Log(LoggerFactory.getLogger(getClass()));

	private final Session session;
	private final String tableName;
	private final boolean isBidirectional;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;
	private V defaultValue;
	private K defaultKey;

	private PreparedStatement _insertStatement;
	private PreparedStatement _deleteStatement;
	private PreparedStatement _getValueStatement;
	private PreparedStatement _getKeyStatement;
	private PreparedStatement _getAllStatement;

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param name the name of the table that the DAO should operate on.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xMapDAO(final Session session, final String name, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
		this(session, name, false, keySerializer, valueSerializer);
	}

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param bidirectional True if the DAO should allow reverse lookups. Note that reverse lookups for values > 64KB are not possible.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xMapDAO(
			final Session session, final String tableName, final boolean bidirectional,
			final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
		this.session = session;
		this.tableName = tableName;
		this.isBidirectional = bidirectional;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public void createRequiredSchemaEntities() throws StorageLayerException {
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + tableName + " (key BLOB, value BLOB, PRIMARY KEY (key))"
				+ " WITH compaction = {'class': 'LeveledCompactionStrategy'}"
				+ " AND compression = {'sstable_compression' : 'SnappyCompressor'}");

		if (isBidirectional) {
			session.execute("CREATE INDEX IF NOT EXISTS " + tableName + "_value_index ON " + tableName + " (value)");
			_getKeyStatement = session.prepare("SELECT key FROM " + tableName + " WHERE value = ?");
		}

		_insertStatement = session.prepare("INSERT INTO " + tableName + " (key, value) VALUES (?, ?)");

		_deleteStatement = session.prepare("DELETE FROM " + tableName + " WHERE key = ?");
		_getValueStatement = session.prepare("SELECT value FROM " + tableName + " WHERE key = ?");
		_getAllStatement = session.prepare("SELECT key, value FROM " + tableName);
	}

	@Override
	public boolean contains(final K key) {
		final BoundStatement containsStatement = _getValueStatement.bind(keySerializer.serialize(key));
		return session.execute(containsStatement).one() != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) {
		if (keys == null || keys.length == 0) {
			return;
		}

		final BatchStatement batchStatement = new BatchStatement();

		for (K key : keys) {
			if (key != null) {
				batchStatement.add(_deleteStatement.bind(keySerializer.serialize(key)));
			}
		}

		session.execute(batchStatement);
	}

	@Override
	public V get(final K key) {
		final ByteBuffer serializedKey = keySerializer.serialize(key);
		final Row result = session.execute(_getValueStatement.bind(serializedKey)).one();

		return result != null ? valueSerializer.deserialize(result.getBytesUnsafe(0)) : defaultValue;
	}

	@Override
	public K getKey(final V value) {
		if (!isBidirectional) {
			throw new IllegalStateException("Map is not bidirectional");
		}

		final ByteBuffer serializedValue = valueSerializer.serialize(value);
		final Row result = session.execute(_getKeyStatement.bind(serializedValue)).one();
		return result != null ? keySerializer.deserialize(result.getBytesUnsafe(0)) : defaultKey;
	}

	@Override
	public Iterator<K> keyIterator() {
		final Iterator<Row> resultIterator = session.execute(_getAllStatement.bind()).iterator();

		return new AbstractIterator<K>() {
			@Override
			protected K computeNext() {
				return resultIterator.hasNext() 
						? keySerializer.deserialize(resultIterator.next().getBytesUnsafe(0)) 
						: endOfData();
			}
		};
	}

	@Override
	public Set<K> keySet() {
		final Set<K> keys = new HashSet<K>();

		for (Iterator<K> iter = keyIterator(); iter.hasNext();) {
			keys.add(iter.next());
		}

		return keys;
	}

	/**
	 * Returns a {@link BoundStatement} to insert the given key/value pair.
	 * @param key The key.
	 * @param value The value.
	 * @return A BoundStatement to insert the given key/value pair.
	 */
	private BoundStatement insertStatement(final K key, final V value) {
		return _insertStatement.bind(keySerializer.serialize(key), valueSerializer.serialize(value));
	}

	@Override
	public void set(final K key, final V value) {
		session.execute(insertStatement(key, value));
	}

	@Override
	public void setAll(final Map<K, V> pairs) {
		if (pairs.isEmpty()) {
			return;
		}

		final BatchStatement batchStatement = new BatchStatement();

		for (final Map.Entry<K, V> entry : pairs.entrySet()) {
			batchStatement.add(insertStatement(entry.getKey(), entry.getValue()));
		}

		try {
			session.execute(batchStatement);
		} catch (Exception exception) {
			_log.error("failed to insert batch of " + pairs.size() + " dictionary entries", exception);
		}
	}

	@Override
	public void setDefaultValue(final V defaultValue) {
		this.defaultValue = defaultValue;
	}
}