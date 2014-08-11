package org.gazzax.labs.jena.nosql.fwk;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.google.common.collect.AbstractIterator;

/**
 * A map implementations that read and write key/value pairs from a persistent storage.
 * Although the name and the behaviour recall the {@link Map} interface, this class doesn't implement
 * that interface because 
 * 
 * <ul>
 * 	<li>we don't need such complexity.</li>
 * 	<li>we don't need polymorphism between this map and the standard {@link Map}s found in the java API</li>
 * </ul>
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see https://code.google.com/p/cumulusrdf
 * @param <K> the key kind / type managed by the underlying map structure.
 * @param <V> the value kind / type managed by the underlying map structure.
 */
public class PersistentMap<K, V> implements Initialisable {

	private final class Entry implements Map.Entry<K, V> {

		private final K key;
		private V value;

		/**
		 * Builds a new entry.
		 * 
		 * @param k the entry key.
		 * @param v the entry value.
		 */
		Entry(final K k, final V v) {
			key = k;
			value = v;
		}

		@Override
		public boolean equals(final Object obj) {

			if (obj == this) {
				return true;
			}

			@SuppressWarnings("unchecked")
			final Map.Entry<K, V> e = (Map.Entry<K, V>) obj;
			final Object k1 = getKey();
			final Object k2 = e.getKey();

			if (k1 == k2 || (k1 != null && k1.equals(k2))) {

				final Object v1 = getValue();
				final Object v2 = e.getValue();

				if (v1 == v2 || (v1 != null && v1.equals(v2))) {
					return true;
				}
			}

			return false;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public int hashCode() {
			return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
		}

		@Override
		public V setValue(final V value) {
			this.value = value;
			return value;
		}

		@Override
		public String toString() {
			return "[key = " + key + ", value = " + value + "]";
		}
	}

	private Class<K> k;
	private Class<V> v;

	private MapDAO<K, V> dao;

	private final boolean isBidirectional;
	private final String name;
	private final V defaultValue;

	/**
	 * Builds a new persistent map with a given name.
	 * 
	 * @param name the map name.
	 * @param k the key class.
	 * @param v the value class.
	 * @param bidirectional if this map is bidirectional.
	 * @param defaultValue the default value that will be returned in case of empty search result.
	 */
	public PersistentMap(
			final Class<K> k, 
			final Class<V> v, 
			final String name, 
			final boolean bidirectional,
			final V defaultValue) {
		this.k = k;
		this.v = v;
		this.name = name;
		this.isBidirectional = bidirectional;
		this.defaultValue = defaultValue;
	}

	/**
	 * Clears all entries from the underlying persistent entity.
	 * 
	 * @throws StorageLayerException in case of data access failure.
	 */
	public void clear() throws StorageLayerException {
		for (final K key : keySet()) {
			remove(key);
		}
	}

	/**
	 * Returns true if this map contains the given key.
	 * 
	 * @param key the key.
	 * @return true if this map contains the given key, false otherwise.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public boolean containsKey(final K key) throws StorageLayerException {
		return (key != null && dao.contains(key));
	}

	/**
	 * Returns true if this map contains the given value.
	 * 
	 * @param value the value.
	 * @return true if this map contains the given value, false otherwise.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public boolean containsValue(final V value) throws StorageLayerException {

		if (value == null) {
			return false;
		}

		if (isBidirectional) {
			return getKeyQuick(value) != null;
		} else {
			for (final K key : dao.keySet()) {
				if (dao.get(key).equals(value)) {
					return true;
				}
			}

			return false;
		}
	}

	/**
	 * Returns a set containing all keys within thid map.
	 * 
	 * @return a set containing all keys within thid map.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public Set<Map.Entry<K, V>> entrySet() throws StorageLayerException {

		final Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();

		for (final K key : keySet()) {
			entrySet.add(new Entry(key, get(key)));
		}

		return entrySet;
	}

	/**
	 * Returns the value associated with the given key.
	 * 
	 * @param key the key.
	 * @return the value associated with the given key.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public V get(final K key) throws StorageLayerException {
		return (key != null) ? dao.get(key) : null;
	}

	/**
	 * Returns the key associated with the given value.
	 * 
	 * @param value the value.
	 * @return the key associated with the given value.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public K getKeyQuick(final V value) throws StorageLayerException {
		return dao.getKey(value);
	}

	@Override
	public void initialise(final StorageLayerFactory factory) throws InitialisationException {
		dao = factory.getMapDAO(k, v, isBidirectional, name);
		try {
			dao.setDefaultValue(defaultValue);
			dao.createRequiredSchemaEntities();
		} catch (final StorageLayerException exception) {
			throw new InitialisationException(exception);
		}
	}

	/**
	 * Returns an iterator over the keys of this map.
	 * 
	 * @return an iterator over the keys of this map.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public Iterator<K> keyIterator() throws StorageLayerException {
		return dao.keyIterator();
	}

	/**
	 * Returns a set containing all the keys of this map.
	 * 
	 * @return a set containing all the keys of this map.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public Set<K> keySet() throws StorageLayerException {
		return dao.keySet();
	}

	/**
	 * Puts the given entries into this map. If a mapping already exists for a
	 * key, it will be replaced with the new value.
	 * 
	 * @param m the entries to insert.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public void putAll(final Map<? extends K, ? extends V> m) throws StorageLayerException {

		for (final Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Puts the given entry key/value into this map. If a mapping already exists
	 * for that key, it will be replaced with the new value.
	 * <p>
	 * In contrast to {@link #put}, this does not return the old value.
	 * 
	 * @param key the key.
	 * @param value the value.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public void put(final K key, final V value) throws StorageLayerException {
		if (key == null || value == null) {
			return;
		}

		dao.set(key, value);
	}

	/**
	 * Removes the entry with the given key from the map.
	 * 
	 * @param key the key.
	 * @throws StorageLayerException in case of data access failure.
	 */
	@SuppressWarnings("unchecked")
	public void remove(final K key) throws StorageLayerException {
		if (key == null) {
			return;
		}

		dao.delete(key);
	}

	/**
	 * Returns an iterator over the values of this map.
	 * 
	 * @return an iterator over the values of this map.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public Iterator<V> valueIterator() throws StorageLayerException {

		final Iterator<K> _key_iter = keyIterator();

		return new AbstractIterator<V>() {

			@Override
			public V computeNext() {

				try {
					if (!_key_iter.hasNext()) {
						return endOfData();
					}

					K key = _key_iter.next();

					if (key == null) {
						return endOfData();
					}

					return get(key);
				} catch (final StorageLayerException exception) {
					throw new RuntimeException(exception);
				}
			}
		};
	}

	/**
	 * Returns a collection containing all values of this map.
	 * 
	 * @return a collection containing all values of this map.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public Collection<V> values() throws StorageLayerException {

		final List<V> values = new LinkedList<V>();

		for (final K key : keySet()) {

			final Object val = get(key);

			if (val != null) {
				values.add(get(key));
			}
		}

		return values;
	}
}