package org.gazzax.labs.jena.nosql.fwk;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;
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
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Supertype layer for all SPARQL integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SparqlIntegrationTestCase {
	protected static final String EXAMPLES_DIR = "src/test/resources/w3c/";
	
	protected Dataset dataset;
	protected StorageLayerFactory factory;
	
	/**
	 * Setup fixture for this test.
	 */
	@Before
	public final void setUp() {
		factory = StorageLayerFactory.getFactory();
		dataset = DatasetFactory.create(factory.getDatasetGraph());
		
		load("data.ttl");
	}
	
	/**
	 * Returns the chapter the test refers to (e.g. 2.1, 2.2, 3.2).
	 * 
	 * @return the chapter the test refers to.
	 */
	protected abstract String chapter();
	
	/**
	 * Executes the first test.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void test() throws Exception {
		for (int i = 1; i < howManyExamples() + 1; i++) {
			executeTestWithFile("ex" + i);			
		}
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
	protected abstract void executeTestWithFile(final String filename) throws Exception;
	
	/**
	 * Returns how many examples belong to this test.
	 * 
	 * @return how many examples belong to this test.
	 */
	protected int howManyExamples() {
		return 1;
	}
	
	/**
	 * Shutdown procedure for this test.
	 */
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
	protected String queryString(final String filename) throws IOException {
		return readFile(filename);
	}
	
	/**
	 * Builds a string from a given file.
	 * 
	 * @param filename the filename (without path).
	 * @return a string with the file content.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String readFile(final String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(new File(EXAMPLES_DIR + File.separator + chapter() + File.separator, filename).toURI())));
	}
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 */
	protected void load(final String datafileName) {
		final Model model = dataset.getDefaultModel().read(new File(EXAMPLES_DIR + File.separator + chapter() + File.separator, datafileName).toURI().toString(), DUMMY_BASE_URI, "TTL");
		assertFalse(model.isEmpty());
	}
}
