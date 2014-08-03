package org.gazzax.labs.jena.nosql.fwk.factory;

import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;

public abstract class StorageLayerFactory {
	/**
	 * Returns the {@link MapDAO}.
	 * 
	 * @param <K> the key class.
	 * @param <V> the value class.
	 * @param keyClass the key class used in the returned {@link MapDAO}.
	 * @param valueClass the key class used in the returned {@link MapDAO}.
	 * @param isBidirectional a flag indicating if we want a bidirectional {@link MapDAO} instance.
	 * @param mapName the name that will identify the map.
	 * @return a new {@link MapDAO} instance.
	 */
	public abstract <K, V> MapDAO<K, V> getMapDAO(
			Class<K> keyClass, 
			Class<V> valueClass,
			boolean isBidirectional, 
			String mapName);
}
