package org.gazzax.labs.jena.nosql.fwk;

import java.io.File;
import java.io.FileReader;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

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
 * A simple standalone app for testing / understanding client / server interaction.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SimpleTestClient {
	/** 
	 * Starts this client.
	 * 
	 * @param args the command line arguments.
	 */   
	public static void main(final String[] args) { 
		StorageLayerFactory factory = null;
		Dataset dataset;
		QueryExecution execution = null; 
		try { 
			factory = StorageLayerFactory.getFactory();
			dataset = DatasetFactory.create(factory.getDatasetGraph());
			
			Model model = dataset.getDefaultModel(); 
//			model.read(new FileReader(new File("/work/workspaces/jena-nosql/jena-nosql/jena-nosql-integration-tests/src/test/resources/w3c/2.3/data.ttl")), "http://ba.s.d", "TTL");
			
//			Thread.sleep(1000);
			 
			String q = 
					"PREFIX : <http://learningsparql.com/ns/papers#> " +
					"PREFIX c: <http://learningsparql.com/ns/citations#> " +
					"SELECT ?s " + 
					"WHERE { ?s c:cites :paperA . }";
			
			final Query query = QueryFactory.create(q);
			execution = QueryExecutionFactory.create(query, model);
			
			ResultSet rs = execution.execSelect();
			
			System.out.println(ResultSetFormatter.asText(rs));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (execution != null) execution.close();
			factory.getClientShutdownHook().close();
		}
	}
}
