package org.gazzax.labs.jena.nosql.cassandra.dao;

import java.nio.ByteBuffer;

import org.gazzax.labs.jena.nosql.cassandra.CoDec;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * A map DAO that operates on a Cassandra table.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class Cassandra2xMapDAO<K, V> implements MapDAO<K, V> {

	protected final Log logger = new Log(LoggerFactory.getLogger(getClass()));

	protected final Session session;
	protected final String name;
	protected final boolean isBidirectional;
	protected final CoDec<K> keySerializer;
	protected final CoDec<V> valueSerializer;
	protected V defaultValue;
	private K defaultKey;

	protected PreparedStatement insertStatement;
	protected PreparedStatement deleteStatement;
	protected PreparedStatement getValueStatement;
	protected PreparedStatement getKeyStatement;
	protected PreparedStatement getAllStatement;

	/**
	 * Creates a new {@link Cassandra2xMapDAO}.
	 * 
	 * @param session The connection to Cassandra.
	 * @param name the name of the table that the DAO should operate on.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xMapDAO(
			final Session session, 
			final String name, 
			final CoDec<K> keySerializer, 
			final CoDec<V> valueSerializer) {
		this(session, name, false, keySerializer, valueSerializer);
	}

	/**
	 * Creates a new {@link Cassandra2xMapDAO}.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param bidirectional True if the DAO should allow reverse lookups. Note that reverse lookups for values > 64KB are not possible.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xMapDAO(
			final Session session, final String tableName, final boolean bidirectional,
			final CoDec<K> keySerializer, final CoDec<V> valueSerializer) {
		this.session = session;
		this.name = tableName;
		this.isBidirectional = bidirectional;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public void createRequiredSchemaEntities() throws StorageLayerException {
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + name + " (key BLOB, value BLOB, PRIMARY KEY (key))"
				+ " WITH compaction = {'class': 'LeveledCompactionStrategy'}"
				+ " AND compression = {'sstable_compression' : 'SnappyCompressor'}");

		if (isBidirectional) {
			session.execute("CREATE INDEX IF NOT EXISTS " + name + "_value_index ON " + name + " (value)");
			getKeyStatement = session.prepare("SELECT key FROM " + name + " WHERE value = ?");
		}

		insertStatement = session.prepare("INSERT INTO " + name + " (key, value) VALUES (?, ?)");

		deleteStatement = session.prepare("DELETE FROM " + name + " WHERE key = ?");
		getValueStatement = session.prepare("SELECT value FROM " + name + " WHERE key = ?");
		getAllStatement = session.prepare("SELECT key, value FROM " + name);
	}

	@Override
	public boolean contains(final K key) {
		final BoundStatement containsStatement = getValueStatement.bind(keySerializer.serialize(key));
		return session.execute(containsStatement).one() != null;
	}

	@Override
	public V get(final K key) {
		final ByteBuffer serializedKey = keySerializer.serialize(key);
		final Row result = session.execute(getValueStatement.bind(serializedKey)).one();

		return result != null ? valueSerializer.deserialize(result.getBytesUnsafe(0)) : defaultValue;
	}

	@Override
	public K getKey(final V value) {
		if (!isBidirectional) {
			throw new IllegalStateException("This MapDAO doesn't allow bidirectionality.");
		}

		final ByteBuffer serializedValue = valueSerializer.serialize(value);
		final Row result = session.execute(getKeyStatement.bind(serializedValue)).one();
		return result != null ? keySerializer.deserialize(result.getBytesUnsafe(0)) : defaultKey;
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
				batchStatement.add(deleteStatement.bind(keySerializer.serialize(key)));
			}
		}

		session.execute(batchStatement);
	}

	/**
	 * Creates the insert statement and bounds the given parameters.
	 * 
	 * @param key the key.
	 * @param value the value.
	 * @return the insert {@link BoundStatement}.
	 */
	protected BoundStatement insertStatement(final K key, final V value) {
		return insertStatement.bind(keySerializer.serialize(key), valueSerializer.serialize(value));
	}

	@Override
	public void set(final K key, final V value) {
		session.execute(insertStatement(key, value));
	}

	@Override
	public void setDefaultValue(final V defaultValue) {
		this.defaultValue = defaultValue;
	}
}