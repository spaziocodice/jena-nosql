package org.gazzax.labs.jena.nosql.cassandra.dao;

import static org.gazzax.labs.jena.nosql.fwk.util.Utility.murmurHash3;

import java.nio.ByteBuffer;

import org.gazzax.labs.jena.nosql.cassandra.CoDec;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * A DAO that is able to do bidirectional lookups with any value size.
 * This is a workaround for a limit of Cassandra. In Cassandra, anything that gets indexed must not be greater than 64KB.
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
public class Cassandra2xBidirectionalMapDAO<K, V> extends Cassandra2xMapDAO<K, V> {
	private PreparedStatement checkHashStatement;

	/**
	 * Creates a new {@link Cassandra2xBidirectionalMapDAO}.
	 * 
	 * @param session The connection to Cassandra.
	 * @param name The name of the table that the DAO should operate on.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xBidirectionalMapDAO(
			final Session session, 
			final String name, 
			final CoDec<K> keySerializer, 
			final CoDec<V> valueSerializer) {
		super(session, name, true, keySerializer, valueSerializer);
	}

	@Override
	public void createRequiredSchemaEntities() throws StorageLayerException {
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + name + " (key BLOB, valueHash LONG, value BLOB, PRIMARY KEY (key))"
				+ " WITH compaction = {'class': 'LeveledCompactionStrategy'}"
				+ " AND compression = {'sstable_compression' : 'SnappyCompressor'}");

		session.execute("CREATE INDEX IF NOT EXISTS " + name + "_value_index ON " + name + " (valueHash)");

		insertStatement = session.prepare("INSERT INTO " + name + " (key, valueHash, value) VALUES (?, ?, ?)");

		deleteStatement = session.prepare("DELETE FROM " + name + " WHERE key = ?");
		getValueStatement = session.prepare("SELECT value FROM " + name + " WHERE key = ?");
		getKeyStatement = session.prepare("SELECT key FROM " + name + " WHERE valueHash = ?");
		getAllStatement = session.prepare("SELECT key, value FROM " + name);
		this.checkHashStatement = session.prepare("SELECT value FROM " + name + " WHERE valueHash = ?");
	}

	@Override
	public K getKey(final V value) {
		if (value == null) {
			return null;
		}
		
		final ByteBuffer result = session.execute(getKeyStatement.bind(getValueHash(value))).one().getBytesUnsafe(0);
		return (result != null) ? keySerializer.deserialize(result) : null;
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
		final byte[] serializedValue = valueSerializer.serializeDirect(value);
		long hash = murmurHash3(serializedValue).asLong();

		boolean hashFound = false;

		for (int iterations = 0; iterations < 100 && !hashFound; hash++, iterations++) {
			final Row result = session.execute(checkHashStatement.bind(hash)).one();

			if (result == null) {
				hashFound = true;
			} else {
				final V mappedValue = valueSerializer.deserialize(result.getBytesUnsafe(0));

				if (valueSerializer.isEqual(value, mappedValue)) {
					hashFound = true;
				}
			}
		}

		if (hashFound) {
			return hash;
		} else {
			logger.error(MessageCatalog._00098_COULD_NOT_GET_HASH, value);
			return -1;
		}
	}

	@Override
	protected BoundStatement insertStatement(final K key, final V value) {
		return insertStatement.bind(keySerializer.serialize(key), getValueHash(value), valueSerializer.serialize(value));
	}
}