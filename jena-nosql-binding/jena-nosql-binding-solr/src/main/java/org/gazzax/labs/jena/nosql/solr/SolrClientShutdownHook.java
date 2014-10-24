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
	private final SolrServer facade;
	
	/**
	 * Builds a new SOLR Client shutdown hook.
	 * 
	 * @param connection the connection to SOLR.
	 */
	public SolrClientShutdownHook(final SolrServer connection) {
		this.facade = connection;
	}
	
	@Override
	public void close() {
		try {
			facade.shutdown();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00099_CLIENT_SHUTDOWN_FAILURE, exception);
		}
	}
}
