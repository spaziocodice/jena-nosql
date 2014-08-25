package org.gazzax.labs.jena.nosql.fwk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * Supertype layer for all SPARQL integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SparqlSelectIntegrationTestCase extends SparqlIntegrationTestCase {	
	@Override
	protected void executeTestWithFile(final String filename) throws Exception {
		final Query query = QueryFactory.create(queryString(filename + ".rq"));
		final QueryExecution execution = QueryExecutionFactory.create(query, dataset);
		final ResultSet rs = execution.execSelect();
		
		final String s = ResultSetFormatter.asText(rs, query).trim();

		assertEquals(
				results(filename + ".rs").trim(),
				s.trim());
		execution.close();
	}	
	
	/**
	 * Builds a string (from the file associated with this test) with the expected query results.
	 * 
	 * @param resultsFileName the results filename.
	 * @return a string (from the file associated with this test) with the expected query results.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String results(final String resultsFileName) throws IOException {
		return readFile(resultsFileName);
	}	
}
