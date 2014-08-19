package org.gazzax.labs.jena.nosql.fwk.w3c;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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

public class Ex1ITCase {
	protected static final String EXAMPLES_DIR = "src/test/resources/w3c/";
	
	private Dataset dataset;
	private StorageLayerFactory factory;
	
	@Before
	public void setUp() {
		factory = StorageLayerFactory.getFactory();
		dataset = DatasetFactory.create(factory.getDatasetGraph());
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(EXAMPLES_DIR, "chapter_2.1_ex1.ttl")));
			final Model model = dataset.getDefaultModel().read(reader, DUMMY_BASE_URI, "TTL");
			assertEquals(1, model.size());
		} catch (final Exception exception) {
			// TODO: handle exception
		}
	}
	
	@Test
	public void test() {
		final Query query = QueryFactory.create(query("chapter_2.1_ex1.rq"));
		final QueryExecution execution = QueryExecutionFactory.create(query, dataset);
		final ResultSet rs = execution.execSelect();
		assertEquals(ResultSetFormatter.asText(rs, query).trim(), results("chapter_2.1_ex1.rs").trim());
		execution.close();
	}
	
	@After
	public void tearDown() {
		factory.getClientShutdownHook().close();
	}
	
	private String query(final String sourcefileName) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(EXAMPLES_DIR, sourcefileName)));
			String actLine = null;
			final StringBuilder builder = new StringBuilder();
			while ( (actLine = reader.readLine()) != null) {
				builder.append(actLine);
			}
			return builder.toString();
		} catch (final Exception exception) { 
			throw new RuntimeException(exception);
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private String results(final String resultsFileName) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(EXAMPLES_DIR, resultsFileName)));
			String actLine = null;
			final StringBuilder builder = new StringBuilder();
			int i = 0;
			while ( (actLine = reader.readLine()) != null) {
				if (i > 0) {
					builder.append(System.getProperty("line.separator"));
				}
				builder.append(actLine);
				i++;
			}
			
			System.out.println(builder);
			
			return builder.toString();
		} catch (final Exception exception) { 
			throw new RuntimeException(exception);
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
}
