package org.gazzax.labs.jena.nosql.solr;

import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
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
	private final TopLevelDictionary dictionary = new NoOpDictionary();
	private SolrServer solr;
	
	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		deletionBatchSize = configuration.getParameter("delete-batch-size", Integer.valueOf(1000));
		
		final String address = configuration.getParameter("solr-address", "http://127.0.0.1:8080/solr/store");
		try {
			solr = (SolrServer) Class.forName(configuration.getParameter("solr-server-class", HttpSolrServer.class.getName()))
					.getConstructor(String.class)
					.newInstance(address);
			
		} catch (final Exception exception) {
			throw new IllegalArgumentException(exception);
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
		return new SolrGraph(this, deletionBatchSize);
	}	
	
	@Override
	public Graph getGraph(Node graphNode) {
		return new SolrGraph(graphNode, this, deletionBatchSize);
	}	
	
	@Override
	public GraphDAO<Triple, TripleMatch> getGraphDAO(final Node name) {
		return new SolrGraphDAO(solr, name);
	}
	
	@Override
	public GraphDAO<Triple, TripleMatch> getGraphDAO() {
		return new SolrGraphDAO(solr);
	}
	
	@Override
	public TopLevelDictionary getDictionary() {
		return dictionary;
	}

	@Override
	public ClientShutdownHook getClientShutdownHook() {
		return new SolrClientShutdownHook(solr);
	}

	@Override
	public String getInfo() {
		return "Jena-nosql Apache SOLR binding";
	}
}
