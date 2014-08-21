package org.gazzax.labs.jena.nosql.fwk.w3c;

import org.gazzax.labs.jena.nosql.fwk.SparqlIntegrationTestCase;
import org.junit.Test;

/**
 * SPARQL Integration test with examples taken from http://www.w3.org/TR/sparql11-query.
 * 
 * @see http://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Ch2_3_MatchingRDFLiterals_ITCase extends SparqlIntegrationTestCase {
	@Override
	protected String testFilename() {
		return "chapter_2.3_ex1";
	}
	
	/**
	 * Executes the 2nd test of the chapter.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void example2() throws Exception {
		executeTestWithFile("chapter_2.3_ex2");
	}
}
