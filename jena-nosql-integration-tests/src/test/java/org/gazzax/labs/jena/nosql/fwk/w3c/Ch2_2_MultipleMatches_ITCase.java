package org.gazzax.labs.jena.nosql.fwk.w3c;

import org.gazzax.labs.jena.nosql.fwk.SparqlIntegrationTestCase;

/**
 * SPARQL Integration test with examples taken from http://www.w3.org/TR/sparql11-query
 * 
 * @see http://www.w3.org/TR/sparql11-query/#MultipleMatches
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Ch2_2_MultipleMatches_ITCase extends SparqlIntegrationTestCase {
	@Override
	protected String testFilename() {
		return "chapter_2.2_ex1";
	}
}
