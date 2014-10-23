/*
 * This Test case makes use of some examples from 
 * 
 * "Learning SPARQL - Querying and Updating with SPARQL 1.1" by Bob DuCharme
 * 
 * Publisher: O'Reilly
 * Author: Bob DuCharme
 * ISBN: 978-1-449-37143-2
 * http://www.learningsparql.com/
 * http://shop.oreilly.com/product/0636920030829.do
 * 
 * We warmly appreciate and thank the author and O'Reilly for such permission.
 * 
 */
package org.gazzax.labs.jena.nosql.fwk.learningSPARQL;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.eheh;
import static org.gazzax.labs.jena.nosql.fwk.learningSPARQL.MisteryGuest.misteryGuest;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;

/**
 * SPARQL integration test.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LearningSparql_ITCase {
	protected final String examplesDir = "src/test/resources/LearningSPARQLExamples";

	protected final Log log = new Log(LoggerFactory.getLogger(LearningSparql_ITCase.class));
	
	protected Dataset dataset;
	protected Dataset memoryDataset;
	
	protected StorageLayerFactory factory;
	
	static final List<MisteryGuest> DATA = new ArrayList<MisteryGuest>();
	
	/**
	 * Fills the data and queries map.
	 */
	@BeforeClass
	public static void init() {
		DATA.add(misteryGuest("ex003.rq", "ex002.ttl"));
		DATA.add(misteryGuest("ex006.rq", "ex002.ttl"));
		DATA.add(misteryGuest("ex007.rq", "ex002.ttl"));
		DATA.add(misteryGuest("ex008.rq", "ex002.ttl"));
		DATA.add(misteryGuest("ex010.rq", "ex002.ttl"));

		DATA.add(misteryGuest("ex013.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex015.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex017.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex019.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex021.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex047.rq", "ex012.ttl"));
		DATA.add(misteryGuest("ex052.rq", "ex050.ttl", "foaf.rdf"));
		DATA.add(misteryGuest("ex055.rq", "", "ex054.ttl"));
		DATA.add(misteryGuest("ex057.rq", "", "ex054.ttl"));
		DATA.add(misteryGuest("ex059.rq", "", "ex054.ttl"));
		DATA.add(misteryGuest("ex061.rq", "", "ex054.ttl"));
		DATA.add(misteryGuest("ex063.rq", "", "ex054.ttl"));
		DATA.add(misteryGuest("ex065.rq", "!BOUND", "ex054.ttl"));
		DATA.add(misteryGuest("ex067.rq", "FILTER NOT EXISTS", "ex054.ttl"));
		DATA.add(misteryGuest("ex068.rq", "MINUS", "ex054.ttl"));
		DATA.add(misteryGuest("ex070.rq", "Multiple tables", "ex069.ttl"));
		DATA.add(misteryGuest("ex070.rq", "Multiple tables with split datasets", "ex072.ttl", "ex073.ttl", "ex368.ttl"));
		DATA.add(misteryGuest("ex075.rq", "Bind either", "ex074.ttl"));
		DATA.add(misteryGuest("ex077.rq", "Bind either", "ex074.ttl"));
		DATA.add(misteryGuest("ex078.rq", "Property paths", "ex074.ttl"));
		DATA.add(misteryGuest("ex080.rq", "Property paths", "ex074.ttl"));
		DATA.add(misteryGuest("ex082.rq", "Property paths", "ex074.ttl"));
		DATA.add(misteryGuest("ex083.rq", "Property paths", "ex074.ttl"));
		DATA.add(misteryGuest("ex084.rq", "Property paths", "ex074.ttl"));
		DATA.add(misteryGuest("ex086.rq", "Querying blank nodes", "ex041.ttl"));
		DATA.add(misteryGuest("ex088.rq", "Querying blank nodes", "ex041.ttl"));
	}
	
	/**
	 * Setup fixture for this test.
	 */
	@Before
	public final void setUp() {
		factory = StorageLayerFactory.getFactory();
		dataset = DatasetFactory.create(factory.getDatasetGraph());
		memoryDataset = DatasetFactory.createMem();
	}
	
	/**
	 * Shutdown procedure for this test.
	 */
	@After
	public void tearDown() {
		clearDatasets();
		factory.getClientShutdownHook().close();
	}
	
	@Test
	public void select() throws Exception {
		for (final MisteryGuest data : DATA) {
			log.info("Running " + data.description + " test...");
			
			load(data);
			
			final Query query = QueryFactory.create(queryString(data.query));
			QueryExecution execution = null;
			QueryExecution inMemoryExecution = null;
			
			try {
				assertTrue(
						data.datasets + ", " + data.query,
						ResultSetCompare.isomorphic(
								(execution = QueryExecutionFactory.create(query, dataset)).execSelect(),
								(inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset)).execSelect()));
				
				if (log.isDebugEnabled()) {
					QueryExecution secondExecution = null;
					log.debug("\n" + ResultSetFormatter.asText(
							(secondExecution = (QueryExecutionFactory.create(query, dataset))).execSelect()));
					
					secondExecution.close();
				}
				
			} finally {
				clearDatasets();
				execution.close();
				inMemoryExecution.close();
			}	
		}
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
		return new String(Files.readAllBytes(Paths.get(source(filename))));
	}	
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 */
	protected void load(final MisteryGuest data) {
		final Model model = dataset.getDefaultModel();
		final Model memoryModel = memoryDataset.getDefaultModel();
		
		for (final String datafileName : data.datasets) {
			final String dataURL = source(datafileName).toString();
			final String lang = datafileName.endsWith("ttl") ? "TTL" : null;
			model.read(dataURL, DUMMY_BASE_URI, lang);
			memoryModel.read(dataURL, DUMMY_BASE_URI, lang);
		}
		
		eheh();
		
		assertFalse(data.datasets + ", " + data.query, model.isEmpty());
		assertTrue(data.datasets + ", " + data.query, model.isIsomorphicWith(memoryModel));
	}

	/**
	 * Returns the URI of a given filename.
	 * 
	 * @param filename the filename.
	 * @return the URI (as string) of a given filename.
	 */
	URI source(final String filename) {
		return new File(examplesDir, filename).toURI();
	}	
	
	void clearDatasets() {
		dataset.getDefaultModel().removeAll();
		memoryDataset.getDefaultModel().removeAll();
		eheh();
	}
}
