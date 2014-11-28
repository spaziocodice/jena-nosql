package org.gazzax.labs.jena.nosql.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.gazzax.labs.jena.nosql.fwk.factory.ClientShutdownHook;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.slf4j.LoggerFactory;

/**
 * A {@link ClientShutdownHook} for SOLR clients.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrClientShutdownHook implements ClientShutdownHook {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(ClientShutdownHook.class));
	private final SolrServer indexer;
	private final SolrServer searcher;
	
	/**
	 * Builds a new SOLR Client shutdown hook.
	 * 
	 * @param indexer the SOLR proxy that will be used for index data.
	 * @param searcher the SOLR proxy that will be used for issuing queries.
	 */
	public SolrClientShutdownHook(final SolrServer indexer, final SolrServer searcher) {
		this.indexer = indexer;
		this.searcher = searcher;
	}
	
	@Override
	public void close() {
		try {
			indexer.shutdown();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00099_CLIENT_SHUTDOWN_FAILURE, exception);
		}

		try {
			searcher.shutdown();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00099_CLIENT_SHUTDOWN_FAILURE, exception);
		}
}
}
