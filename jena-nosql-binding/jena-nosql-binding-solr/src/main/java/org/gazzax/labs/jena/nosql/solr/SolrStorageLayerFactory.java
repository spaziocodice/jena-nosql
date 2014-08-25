package org.gazzax.labs.jena.nosql.solr;

import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.ClientShutdownHook;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.graph.NoSqlGraph;
import org.gazzax.labs.jena.nosql.solr.dao.SolrTripleIndexDAO;
import org.gazzax.labs.jena.nosql.solr.graph.SolrGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;

/**
 * Concrete factory for creating SOLR-based domain and data access objects.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrStorageLayerFactory extends StorageLayerFactory {
	private SolrServer solr;
	
	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		// TODO Auto-generated method stub
	}

	@Override
	public <K, V> MapDAO<K, V> getMapDAO(
			final Class<K> keyClass,
			final Class<V> valueClass, 
			final boolean isBidirectional, 
			final String name) {
		return null;
	}

	@Override
	public Graph getGraph() {
		return new SolrGraph(this);
	}	
	
	@Override
	public Graph getGraph(Node graphNode) {
		return new SolrGraph(graphNode, this);
	}	
	
	@Override
	public TripleIndexDAO getTripleIndexDAO() {
		return new SolrTripleIndexDAO(solr);
	}

	@Override
	public TopLevelDictionary getDictionary() {
		return null;
	}

	@Override
	public ClientShutdownHook getClientShutdownHook() {
		return null;
	}

	@Override
	public String getInfo() {
		return null;
	}
}
