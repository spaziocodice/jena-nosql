package org.gazzax.labs.jena.nosql.fwk;

import static org.junit.Assert.assertTrue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;

/**
 * Supertype layer for SPARQL CONSTRUCT tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SparqlConstructIntegrationTestCase extends SparqlIntegrationTestCase {	
	@Override
	protected void executeTestWithFile(final String filename) throws Exception {
		final Query query = QueryFactory.create(queryString(filename + ".rq"));
		QueryExecution execution = null;
		QueryExecution inMemoryExecution = null;
		
		try {
			assertTrue(
					(execution = QueryExecutionFactory.create(query, dataset))
						.execConstruct()
						.isIsomorphicWith(
								(inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset))
								.execConstruct()));
		} finally {
			// CHECKSTYLE:OFF
			if (execution != null) { execution.close(); }
			if (inMemoryExecution != null) { inMemoryExecution.close(); }
			// CHECKSTYLE:ON
		}					
	}	
}
