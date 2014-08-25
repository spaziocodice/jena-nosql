package org.gazzax.labs.jena.nosql.fwk;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Supertype layer for all SPARQL integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SparqlConstructIntegrationTestCase extends SparqlIntegrationTestCase {	
	@Override
	protected void executeTestWithFile(final String filename) throws Exception {
		final Query query = QueryFactory.create(queryString(filename + ".rq"));
		final QueryExecution execution = QueryExecutionFactory.create(query, dataset);
		final Model rs = execution.execConstruct();
		
		final Model model = ModelFactory.createDefaultModel().read(new File(EXAMPLES_DIR + File.separator + chapter() + File.separator, filename + ".rs").toURI().toString(), DUMMY_BASE_URI, "TTL");

		assertTrue(rs.isIsomorphicWith(model));
	}	
}
