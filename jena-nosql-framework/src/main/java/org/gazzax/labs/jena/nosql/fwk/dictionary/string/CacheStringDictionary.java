package org.gazzax.labs.jena.nosql.fwk.dictionary.string;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.DictionaryRuntimeContext;
import org.gazzax.labs.jena.nosql.fwk.dictionary.CacheStrategy;
import org.gazzax.labs.jena.nosql.fwk.dictionary.Dictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.mx.ManageableCacheDictionary;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

/**
 * A dictionary decorator that adds caching capability to another string dictionary. 
 * Note that although this class "is a" dictionary, due to its Decorator nature, 
 * is supposed to be used in conjunction with a concrete dictionary.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @see http://en.wikipedia.org/wiki/Decorator_pattern
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CacheStringDictionary extends StringDictionaryBase implements ManageableCacheDictionary {
	/**
	 * First level strategy allows caching only for results that directly comes from the next decoratee in the chain.
	 * This is useful in case you have an articulated decorator chain and you want to activate several caches in the chain
	 * (with different configurations, for example).
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.1.0
	 */
	class FirstLevelCacheStrategy implements CacheStrategy<String> {

		@Override
		public void cacheId(final ByteBuffer id, final String value) {
			final DictionaryRuntimeContext context = RUNTIME_CONTEXTS.get();
			if (context.isFirstLevelResult != null && context.isFirstLevelResult) {
				context.isFirstLevelResult = null;
				id2node_cache.put(id, value);
			}
		}

		@Override
		public void cacheValue(final String value, final byte[] id) {
			final DictionaryRuntimeContext context = RUNTIME_CONTEXTS.get();
			if (context.isFirstLevelResult != null && context.isFirstLevelResult) {
				context.isFirstLevelResult = null;
				node2id_cache.put(value, id);
			}
		}
	}

	/**
	 * Cumulative strategy basically caches everything, regardless the concrete decoratee that provided the value or the identifier.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.1.0
	 */
	class CumulativeCacheStrategy implements CacheStrategy<String> {

		@Override
		public void cacheId(final ByteBuffer id, final String value) {
			id2node_cache.put(id, value);
			RUNTIME_CONTEXTS.get().isFirstLevelResult = null;
		}

		@Override
		public void cacheValue(final String value, final byte[] id) {
			node2id_cache.put(value, id);
			RUNTIME_CONTEXTS.get().isFirstLevelResult = null;
		}
	}

	static final int DEFAULT_CACHE_SIZE = 1000;
	
	final ConcurrentMap<ByteBuffer, String> id2node_cache;
	final ConcurrentMap<String, byte[]> node2id_cache;
	final CacheStrategy<String> cacheStrategy;

	private final Dictionary<String> decoratee;

	private final int idCacheMaxSize;
	private final int valueCacheMaxSize;
	private final AtomicLong idHitsCount = new AtomicLong();
	private final AtomicLong valueHitsCount = new AtomicLong();
	private final AtomicLong idEvictionsCount = new AtomicLong();
	private final AtomicLong valueEvictionsCount = new AtomicLong();
	
	private final EvictionListener<ByteBuffer, String> idEvictionListener = new EvictionListener<ByteBuffer, String>() {
		@Override
		public void onEviction(final ByteBuffer key, final String value) {
			idEvictionsCount.incrementAndGet();
		}
	};

	private final EvictionListener<String, byte[]> valueEvictionListener = new EvictionListener<String, byte[]>() {
		@Override
		public void onEviction(final String key, final byte[] value) {
			valueEvictionsCount.incrementAndGet();
		}
	};

	/**
	 * Builds and initializes a cache capability on top of a given dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 * @param idCacheSize the identifier cache size. In case <=0 It defaults to {@link #DEFAULT_CACHE_SIZE}
	 * @param valueCacheSize the value cache size. In case <=0 It defaults to {@link #DEFAULT_CACHE_SIZE}
	 * @param isFirstLevelCache a boolean that marks this cache as first level (or cumulative).
	 */
	public CacheStringDictionary(
			final String id, 
			final Dictionary<String> decoratee, 
			final int idCacheSize, 
			final int valueCacheSize, 			
			final boolean isFirstLevelCache) {
		super(id);

		if (decoratee == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}

		this.decoratee = decoratee;		
		idCacheMaxSize = cacheSize(idCacheSize);
		valueCacheMaxSize = cacheSize(valueCacheSize);
		id2node_cache = new ConcurrentLinkedHashMap
				.Builder<ByteBuffer, String>()
				.maximumWeightedCapacity(idCacheMaxSize)
				.listener(idEvictionListener)
				.build();
		node2id_cache = new ConcurrentLinkedHashMap
				.Builder<String, byte[]>()
				.maximumWeightedCapacity(valueCacheMaxSize)
				.listener(valueEvictionListener)
				.build();
		cacheStrategy = isFirstLevelCache ? new FirstLevelCacheStrategy() : new CumulativeCacheStrategy();
	}

	@Override
	protected void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {
		decoratee.initialise(factory);
	}

	@Override
	public void removeValue(final String value, final boolean p) throws StorageLayerException {
		decoratee.removeValue(value, p);
	}

	@Override
	protected void closeInternal() {
		id2node_cache.clear();
		node2id_cache.clear();
		decoratee.close();
	}

	@Override
	protected byte[] getIdInternal(final String value, final boolean p) throws StorageLayerException {
		byte[] id = node2id_cache.get(value);

		if (id == null) {
			id = decoratee.getID(value, p);
			cacheStrategy.cacheValue(value, id);
		} else {
			idHitsCount.incrementAndGet();
		}

		return id;
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		final ByteBuffer key = ByteBuffer.wrap(id);
		String value = id2node_cache.get(key);

		if (value == null) {
			value = decoratee.getValue(id, p);
			cacheStrategy.cacheId(key, value);
		} else {
			valueHitsCount.incrementAndGet();
		}
		return value;
	}
	
	/**
	 * Computes the cache size according with a given input.
	 * If input size is <=0 then default value for cache size will be used.
	 * 
	 * @param inputSize the input cache size.
	 * @return the cache size according with a given input.
	 */
	int cacheSize(final int inputSize) {
		return inputSize > 0 ? inputSize : DEFAULT_CACHE_SIZE;
	}

	@Override
	public int getIdCacheMaxSize() {
		return idCacheMaxSize;
	}

	@Override
	public int getValueCacheMaxSize() {
		return valueCacheMaxSize;
	}

	@Override
	public int getCachedIdentifiersCount() {
		return id2node_cache.size();
	}

	@Override
	public int getCachedValuesCount() {
		return node2id_cache.size();
	}

	@Override
	public boolean isCumulativeCache() {
		return cacheStrategy instanceof CumulativeCacheStrategy;
	}

	@Override
	public long getIdHitsCount() {
		return idHitsCount.get();
	}

	@Override
	public double getIdHitsRatio() {
		final double hitsCount = idHitsCount.get();
		if (hitsCount != 0) {
			return (hitsCount / idLookupsCount.get()) * 100;
		}
		return 0;
	}

	@Override
	public long getIdEvictionsCount() {
		return idEvictionsCount.get();
	}

	@Override
	public long getValueHitsCount() {
		return valueHitsCount.get();
	}

	@Override
	public double getValueHitsRatio() {
		final double hitsCount = valueHitsCount.get();
		if (hitsCount != 0) {
			return (hitsCount / valueLookupsCount.get()) * 100;
		}
		return 0;
	}

	@Override
	public long getValueEvictionsCount() {
		return valueEvictionsCount.get();
	}
}