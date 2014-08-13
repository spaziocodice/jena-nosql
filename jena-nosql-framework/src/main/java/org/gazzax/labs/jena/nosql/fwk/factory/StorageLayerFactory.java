package org.gazzax.labs.jena.nosql.fwk.factory;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.gazzax.labs.jena.nosql.fwk.BIndex;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configurable;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.configuration.DefaultConfigurator;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.graph.NoSqlGraph;

import com.hp.hpl.jena.graph.Graph;

/**
 * Main entry / extension point of the jena-nosql framework.
 * It is an AbstractFactory that defines 
 * 
 * <ul>
 * 	<li>behaviour and members of a family of products that interact with a specific kind of storage.</li>
 * 	<li>factory methods for creating concrete factories (i.e factories bound to a specific storage).</li>
 * </ul>
 * 
 * Basically, whenever you want build a new jena storage implementation (backed by a NoSQL), you should create a new module and provide
 * a concrete implementation of this factory.
 * 
 * @see https://en.wikipedia.org/wiki/Abstract_factory_pattern
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class StorageLayerFactory implements Configurable {
	private static StorageLayerFactory default_factory;
	static 
	{
		final ServiceLoader<StorageLayerFactory> loader = ServiceLoader.load(StorageLayerFactory.class);
		final Iterator<StorageLayerFactory> iterator = loader.iterator();
		default_factory = iterator.hasNext() ? iterator.next() : null;
	}
	
	/**
	 * Returns the {@link MapDAO}.
	 * A {@link MapDAO} instance is required in order to manage the persistent logic of a {@link BIndex}.
	 * 
	 * @param <K> the key class.
	 * @param <V> the value class.
	 * @param keyClass the key class used in the returned {@link MapDAO}.
	 * @param valueClass the key class used in the returned {@link MapDAO}.
	 * @param isBidirectional a flag indicating if we want a bidirectional {@link MapDAO} instance.
	 * @param name the name that will identify the map.
	 * @return a new {@link MapDAO} instance.
	 */
	public abstract <K, V> MapDAO<K, V> getMapDAO(
			Class<K> keyClass, 
			Class<V> valueClass,
			boolean isBidirectional, 
			String name);
	
	/**
	 * Returns the Data Access Object for interacting with the triple index.
	 * 
	 * @return the Data Access Object for interacting with the triple index.
	 */
	public abstract TripleIndexDAO getTripleIndexDAO();

	/**
	 * Returns the {@link Graph} specific implementation associated with the underlying kind of storage.
	 * 
	 * @return the {@link Graph} specific implementation associated with the underlying kind of storage.
	 */
	public Graph getGraph() {
		return new NoSqlGraph(this);
	}	
	
	/**
	 * Factory method for obtaining a concrete factory.
	 * As you can see, the method doesn't allow to specificy the factory. That because it uses the {@link ServiceLoader} mechanism to do that.
	 * 
	 * So implementors should provide a service loader information under the META-INF/services folder of their module. Specifically a file must
	 * exists under that folder; it must have the FQDN name of this class (org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory) as name.
	 * 
	 * @see http://docs.oracle.com/javase/tutorial/ext/basics/spi.html
	 * @param configuration the confifuration that will be injected into the new factory.
	 * @return a concrete factory.
	 */
	public static StorageLayerFactory getFactory(final Configuration<Map<String, Object>> configuration) {
		try {
			configuration.configure(default_factory);
			return default_factory;
		} catch (final Exception exception) {
			// TODO: LOG
			throw new RuntimeException(exception);
		}
	}
	
	/**
	 * Creates a concrete factory with using the default configuration procedures.
	 * 
	 * @return a concrete factory with using the default configuration procedures.
	 */
	public static StorageLayerFactory getFactory() {
		return getFactory(new DefaultConfigurator());
	}
	
	/**
	 * Returns the front dictionary that will be in use.
	 * 
	 * @return the front dictionary that will be in use.
	 */
	public abstract TopLevelDictionary getDictionary();
	
	/**
	 * Returns the handler used for closing client connections.
	 * 
	 * @return the handler used for closing client connections.
	 */
	public abstract ClientShutdownHook getClientShutdownHook();
	
	/**
	 * Each concrete factory should return here descriptive information about the underlying storage.
	 * 
	 * @return a descriptive info about the storage behind this factory.
	 */
	public abstract String getInfo();
}