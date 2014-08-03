package org.gazzax.labs.jena.nosql.fwk.dictionary;

import java.util.concurrent.atomic.AtomicLong;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.mx.ManageableDictionary;
import org.gazzax.labs.jena.nosql.fwk.mx.ManagementRegistrar;
import org.slf4j.LoggerFactory;

/**
 * Supertype layer for all dictionaries.
 * Makes a wide use of template method pattern in order to enforce some common behaviour like:
 * 
 * <ul>
 * 	<li>Basic metrics count</li>
 * 	<li>MBean registration / unregistration</li>
 * </ul>
 * 
 * Although is possible to build a dictionary from scratch, in case you need 
 * an additional dictionary implementation, it is strongly recommended to derive from 
 * this class.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 * @param <V> the concrete value kind managed by this dictionary.
 */
public abstract class DictionaryBase<V> implements Dictionary<V>, ManageableDictionary  {
	protected final Log log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected final String id;
	protected final AtomicLong idLookupsCount = new AtomicLong();
	protected final AtomicLong valueLookupsCount = new AtomicLong();

	/**
	 * Builds a new dictionary with the given identifier.
	 * 
	 * @param id the dictionary identifier.
	 */
	public DictionaryBase(final String id) {
		this.id = id;
	}
	
	/**
	 * Returns the identifier associated with this dictionary.
	 * 
	 * @return the identifier associated with this dictionary.
	 */
	public String getId() {
		return id;
	}
	
	@Override
	public final byte[] getID(final V value, final boolean p) throws StorageLayerException {
		idLookupsCount.incrementAndGet();

		if (value == null) {
			return null;
		}
		
		return getIdInternal(value, p);
	};
	
	@Override
	public final V getValue(final byte[] id, final boolean p) throws StorageLayerException {
		valueLookupsCount.incrementAndGet();
		if (id == null) {
			return null;
		}
		
		return getValueInternal(id, p);
	}
	
	@Override
	public long getValueLookupsCount() {
		return valueLookupsCount.get();
	}

	@Override
	public long getIdLookupsCount() {
		return idLookupsCount.get();
	}	
	
	@Override
	public final void close() {
		ManagementRegistrar.unregisterDictionary(this);
		closeInternal();
	}
	
	@Override
	public final void initialise(final StorageLayerFactory factory) throws InitialisationException {
		initialiseInternal(factory);
		
		try {
			ManagementRegistrar.registerDictionary(this);
		} catch (Exception exception) {
			log.error(MessageCatalog._00111_MBEAN_ALREADY_REGISTERED, id);
			throw new InitialisationException(exception);
		}
	}

	/**
	 * Internal method where each concrete implementor must define its own shutdown procedure.
	 */
	protected abstract void closeInternal();	
	
	/**
	 * Internal method where each concrete implementor must define its own initialisation procedure.
	 * 
	 * @param factory the data access layer factory.
	 * @throws InitialisationException in case of initialisation failure.
	 */
	protected abstract void initialiseInternal(StorageLayerFactory factory) throws InitialisationException;
	
	/**
	 * Internal method where each concrete implementor must define for retrieving identifiers.
	 * 
	 * @param value the value.
	 * @param p the predicate flag.
	 * @return the identifier associated with the given value.
	 * @throws StorageLayerException in case of data access failure.
	 */
	protected abstract byte[] getIdInternal(V value, boolean p) throws StorageLayerException;
	
	/**
	 * Internal method where each concrete implementor must define for retrieving values.
	 * 
	 * @param id the identifier.
	 * @param p the predicate flag.
	 * @return the value associated with the given identifier.
	 * @throws StorageLayerException in case of data access failure.
	 */
	protected abstract V getValueInternal(byte[] id, boolean p) throws StorageLayerException;	
}