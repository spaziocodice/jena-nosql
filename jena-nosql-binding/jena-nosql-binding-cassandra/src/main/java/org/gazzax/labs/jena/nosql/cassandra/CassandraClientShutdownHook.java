package org.gazzax.labs.jena.nosql.cassandra;

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
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(ClientShutdownHook.class));
	private final Session session;
	
	/**
	 * Builds a new Cassandra Client shutdown hook.
	 * 
	 * @param session the connection to Cassandra.
	 */
	public CassandraClientShutdownHook(final Session session) {
		this.session = session;
	}
	
	@Override
	public void close() {
		try {
			session.getCluster().close();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00099_CLIENT_SHUTDOWN_FAILURE, exception);
		}
	}
}
