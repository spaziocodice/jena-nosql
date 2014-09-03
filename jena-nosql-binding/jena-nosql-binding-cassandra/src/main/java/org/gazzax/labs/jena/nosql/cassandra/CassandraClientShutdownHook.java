package org.gazzax.labs.jena.nosql.cassandra;

import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.ClientShutdownHook;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

/**
 * A {@link ClientShutdownHook} for Cassandra clients.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraClientShutdownHook implements ClientShutdownHook {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(ClientShutdownHook.class));
	private final Session session;
	final TopLevelDictionary dictionary;
	
	/**
	 * Builds a new Cassandra Client shutdown hook.
	 * 
	 * @param session the connection to Cassandra.
	 * @param dictionary the dictionary in use.
	 */
	public CassandraClientShutdownHook(final Session session, final TopLevelDictionary dictionary) {
		this.session = session;
		this.dictionary = dictionary;
	}
	
	@Override
	public void close() {
		try {
			session.getCluster().close();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00099_CLIENT_SHUTDOWN_FAILURE, exception);
		}
		dictionary.close();
	}
}
