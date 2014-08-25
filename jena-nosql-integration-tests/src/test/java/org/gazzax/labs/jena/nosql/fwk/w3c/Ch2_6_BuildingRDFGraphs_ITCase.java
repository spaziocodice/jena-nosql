package org.gazzax.labs.jena.nosql.fwk.w3c;

import org.gazzax.labs.jena.nosql.fwk.SparqlConstructIntegrationTestCase;

/**
 * SPARQL Integration test with examples taken from http://www.w3.org/TR/sparql11-query.
 * 
 * @see http://www.w3.org/TR/sparql11-query/#constructGraph
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Ch2_6_BuildingRDFGraphs_ITCase extends SparqlConstructIntegrationTestCase {
	@Override
	protected String chapter() {
		return "2.6";
	}
}