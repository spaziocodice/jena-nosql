package org.gazzax.labs.jena.nosql.solr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.ClientShutdownHook;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.solr.dao.SolrGraphDAO;
import org.gazzax.labs.jena.nosql.solr.graph.SolrGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;

/**
 * Concrete factory for creating SOLR-based domain and data access objects.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrStorageLayerFactory extends StorageLayerFactory {
	/**
	 * A SolrServer (proxy) factory.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	static interface SolrProxyFactory {
		/**
		 * Creates and configure an instance of {@link SolrServer} using the given configuration.
		 * 
		 * @param proxyRole a prefix (indexer.searcher) that will be prepended to configuration attributes in order to get the appropriate settings 
		 * @param configuration the Jena-NoSql configuration.
		 * @return a configured instance of {@link SolrServer}.
		 */
		SolrServer createAndConfigure(String proxyRole, Configuration<Map<String, Object>> configuration);
	};
	
	private final static Map<Class<? extends SolrServer>, SolrProxyFactory> FACTORIES = new HashMap<Class<? extends SolrServer>, SolrProxyFactory>();
	
	static {
		FACTORIES.put(HttpSolrServer.class, new SolrProxyFactory() {
			@Override
			public SolrServer createAndConfigure(final String role, final Configuration<Map<String, Object>> configuration) {
				return new HttpSolrServer(configuration.getParameter(role + ADDRESS, DEFAULT_ADDRESS));
			}
		});

		FACTORIES.put(ConcurrentUpdateSolrServer.class, new SolrProxyFactory() {
			@Override
			public SolrServer createAndConfigure(final String role, final Configuration<Map<String, Object>> configuration) {
				return new ConcurrentUpdateSolrServer(
						configuration.getParameter(role + ADDRESS, DEFAULT_ADDRESS), 
						configuration.getParameter(role + QUEUE_SIZE, DEFAULT_QUEUE_SIZE),
						configuration.getParameter(role + THREAD_COUNT, DEFAULT_THREAD_COUNT));
			}
		});
		
		FACTORIES.put(LBHttpSolrServer.class, new SolrProxyFactory() {
			@Override
			public SolrServer createAndConfigure(final String proxyRole, final Configuration<Map<String, Object>> configuration) {
				
				final List<String> servers = new ArrayList<String>();
				for (int i = 0;; i++) {					
					final String server = configuration.getParameter(
							new StringBuilder(proxyRole)
								.append(i)
								.append(ADDRESS)
								.toString(), 
								null);
					if (server == null) {
						break;
					}
					
					servers.add(server);
				}
				
				try {
					return (!servers.isEmpty() 
						? new LBHttpSolrServer(servers.toArray(new String[servers.size()]))
						: new HttpSolrServer(DEFAULT_ADDRESS));
				} catch (final Exception exception) {
					return new HttpSolrServer(DEFAULT_ADDRESS);
				}
			}
		});
	}
	
	private final TopLevelDictionary dictionary = new NoOpDictionary();

	private final static String ADDS_COMMIT_WITHIN = "adds-commit-within-msecs";
	private final static String DELETES_COMMIT_WITHIN = "deletes-commit-within-msecs";
	private final static int DEFAULT_COMMIT_WITHIN = Integer.valueOf(1);
	private final static int DEFAULT_QUEUE_SIZE = Integer.valueOf(1);
	private final static int DEFAULT_THREAD_COUNT = Integer.valueOf(1);

	private final static String ADDRESS = "-address";
	private final static String DEFAULT_ADDRESS = "http://127.0.0.1:8080/solr/store";

	private final static String QUEUE_SIZE = "-queue-size";	
	private final static String THREAD_COUNT = "-thread-count";	
	
	private final static String INDEXER_PROXY_CLASS = "indexer-class";
	private final static String SEARCHER_PROXY_CLASS = "searcher-class";
	
	private SolrServer indexer;
	private SolrServer searcher;
	
	private int addCommitWithinMsecs;
	private int deleteCommitWithinMsecs;

	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		addCommitWithinMsecs = configuration.getParameter(ADDS_COMMIT_WITHIN, DEFAULT_COMMIT_WITHIN);
		deleteCommitWithinMsecs = configuration.getParameter(DELETES_COMMIT_WITHIN, DEFAULT_COMMIT_WITHIN);
		
		try {
			indexer = FACTORIES.get(
					Class.forName(configuration.getParameter(
							INDEXER_PROXY_CLASS, 
							ConcurrentUpdateSolrServer.class.getName())))
					.createAndConfigure("indexer", configuration);
		} catch (final Exception exception) {
			indexer = FACTORIES.get(ConcurrentUpdateSolrServer.class.getName())
					.createAndConfigure("indexer", configuration);
		}
		
		try {
			searcher = FACTORIES.get(
				Class.forName(configuration.getParameter(
						SEARCHER_PROXY_CLASS, 
						HttpSolrServer.class.getName())))
				.createAndConfigure("indexer", configuration);
		} catch (final Exception exception) {
			searcher = FACTORIES.get(HttpSolrServer.class.getName())
					.createAndConfigure("indexer", configuration);
			
		}
	}

	@Override
	public <K, V> MapDAO<K, V> getMapDAO(
			final Class<K> keyClass,
			final Class<V> valueClass, 
			final boolean isBidirectional, 
			final String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Graph getGraph() {
		return new SolrGraph(this);
	}	
	
	@Override
	public Graph getGraph(final Node graphNode) {
		return new SolrGraph(graphNode, this);
	}	
	
	@Override
	public GraphDAO<Triple, TripleMatch> getGraphDAO(final Node name) {
		return new SolrGraphDAO(indexer, searcher, name, addCommitWithinMsecs, deleteCommitWithinMsecs);
	}
	
	@Override
	public GraphDAO<Triple, TripleMatch> getGraphDAO() {
		return new SolrGraphDAO(indexer, searcher, addCommitWithinMsecs, deleteCommitWithinMsecs);
	}
	
	@Override
	public TopLevelDictionary getDictionary() {
		return dictionary;
	}

	@Override
	public ClientShutdownHook getClientShutdownHook() {
		return new SolrClientShutdownHook(indexer, searcher);
	}

	@Override
	public String getInfo() {
		return "Jena-nosql Apache SOLR binding";
	}
}