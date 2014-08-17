package org.gazzax.labs.jena.nosql.fwk;

import java.util.Map;

import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.log.MessageFactory;

import static org.gazzax.labs.jena.nosql.fwk.util.Strings.*;

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
class PersistentKeyValueMap<K, V> implements Initialisable {
	Class<K> k;
	Class<V> v;

	MapDAO<K, V> dao;

	final boolean isBidirectional;
	final String name;
	final V defaultValue;

	/**
	 * Builds a new persistent map with a given name.
	 * 
	 * @param name the map name.
	 * @param k the key class.
	 * @param v the value class.
	 * @param bidirectional if this map is bidirectional.
	 * @param defaultValue the default value that will be returned in case of empty search result.
	 */
	PersistentKeyValueMap(
			final Class<K> k, 
			final Class<V> v, 
			final String name, 
			final boolean bidirectional,
			final V defaultValue) {
		if (k == null) {
			throw new IllegalArgumentException(
				MessageFactory.createMessage(
						MessageCatalog._00098_INVALID_PMAP_ATTRIBUTE, "Class<K>", null));
		}
		
		if (v == null) {
			throw new IllegalArgumentException(
				MessageFactory.createMessage(
						MessageCatalog._00098_INVALID_PMAP_ATTRIBUTE, "Class<V>", null));
		}

		if (isNullOrEmptyString(name)) {
			throw new IllegalArgumentException(
					MessageFactory.createMessage(
							MessageCatalog._00098_INVALID_PMAP_ATTRIBUTE, "name", name));
		}
		
		this.k = k;
		this.v = v;
		this.name = name;
		this.isBidirectional = bidirectional;
		this.defaultValue = defaultValue;
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
	 * Returns the value associated with the given key.
	 * 
	 * @param key the key.
	 * @return the value associated with the given key.
	 * @throws StorageLayerException in case of data access failure.
	 */
	public V get(final K key) throws StorageLayerException {
		return key != null ? dao.get(key) : null;
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
}