package org.gazzax.labs.jena.nosql.fwk;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Supertype layer for all SPARQL integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SparqlIntegrationTestCase {
	protected static final String EXAMPLES_DIR = "src/test/resources/w3c/";
	
	private Dataset dataset;
	private StorageLayerFactory factory;
	
	@Before
	public final void setUp() {
		factory = StorageLayerFactory.getFactory();
		dataset = DatasetFactory.create(factory.getDatasetGraph());
		
		load(testFilename() + ".ttl");
	}
	
	/**
	 * In case the conctete test consists in just 1 example, this method returns the filename associated with that example.
	 * 
	 * @return the name of the file associated with the example.
	 */
	protected abstract String testFilename();
	
	
	/**
	 * Executes the test.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void example1() throws Exception {
		executeTestWithFile(testFilename());
	}
	
	@After
	public void tearDown() {
		factory.getTripleIndexDAO().clear();
		dataset.close();
		factory.getClientShutdownHook().close();
	}
	
	/**
	 * Reads a query from the file associated with this test and builds a query string.
	 * 
	 * @param filename the filename.
	 * @return the query string associated with this test.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	private String queryString(final String filename) throws IOException {
		return readFile(filename);
	}
	
	/**
	 * Builds a string (from the file associated with this test) with the expected query results.
	 * 
	 * @return a string (from the file associated with this test) with the expected query results.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	private String results(final String resultsFileName) throws IOException {
		return readFile(resultsFileName);
	}
	
	/**
	 * Builds a string from a given file.
	 * 
	 * @param filename the filename (without path).
	 * @return a string with the file content.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	private String readFile(final String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(new File(EXAMPLES_DIR, filename).toURI())));
	}
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 */
	private void load(final String datafileName) {
		final Model model = dataset.getDefaultModel().read(new File(EXAMPLES_DIR, datafileName).toURI().toString(), DUMMY_BASE_URI, "TTL");
		assertFalse(model.isEmpty());
	}
	
	/**
	 * Internal method used to execute a query and assert corresponding results.
	 * In case the test has just one method, there's nothing to do, the subclass already inherits 
	 * the predefined {@link #executeTest()}. 
	 * 
	 * Otherwise, if a test case includes more than one test, then that concrete subclass needs to define test methods 
	 * and call this method to execute and check queries.
	 * 
	 * @param filename the filename.
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	protected void executeTestWithFile(final String filename) throws Exception {
		final Query query = QueryFactory.create(queryString(filename + ".rq"));
		final QueryExecution execution = QueryExecutionFactory.create(query, dataset);
		final ResultSet rs = execution.execSelect();
		
		final String s = ResultSetFormatter.asText(rs, query).trim();
		System.out.println(s);
		assertEquals(
				results(filename + ".rs").trim(),
				s.trim());
		execution.close();
	}	
}
