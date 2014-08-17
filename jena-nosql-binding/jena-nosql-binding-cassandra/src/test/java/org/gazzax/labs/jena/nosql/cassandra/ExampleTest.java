package org.gazzax.labs.jena.nosql.cassandra;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.graph.NoSqlDatasetGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

// TODO : TO BE REMOVED
public class ExampleTest {
	
	public static void main(String[] args) throws Exception {
		// Instantiate the appropriate StorageLayerFactory using ServiceLoader
		// Behind the scenes it will be a CassandraStorageLayerFactory but I don't want to hard code "Cassandra", 
		// this has to work also with others NoSQL (e.g. SOLR, HBase)
		
		// Later I can provide a factory method for forcing a specific StorageLayerFactory impl.
		final StorageLayerFactory factory = StorageLayerFactory.getFactory();
		
//		// Once obtained a concrete StorageLayerFactory, as the name suggests, it acts as a factory for all storage-specific
//		// implementations of family members (Dictionary, Graph and so on)
//		final Graph graph = factory.getGraph(); 
//		final Model model = ModelFactory.createModelForGraph(graph); 
//
//		Statement st = model.createStatement(
//				model.createResource("http://rdf.gx.org/id/resources#me"),
//				FOAF.name,
//				model.createLiteral("Andrea Gazzarini"));
//
//		Statement st1 = model.createStatement(
//				model.createResource("http://rdf.gx.org/id/resources#me"),
//				FOAF.name,
//				model.createLiteral("Gazza"));
//		
//		model.add(st);
//		model.add(st1);

		
		Dataset dataset = DatasetImpl.wrap(new NoSqlDatasetGraph(factory));
		
		QueryExecution query = QueryExecutionFactory.create("SELECT ?p ?o WHERE { <http://rdf.gx.org/id/resources#me> ?p ?o .}", dataset);
		ResultSet rs = query.execSelect();
		while (rs.hasNext()) {
			final QuerySolution solution = rs.nextSolution();
			System.out.println("?o = " + solution.get("o"));
		}
		
//		NodeIterator iterator = model.listObjects();
//		while (iterator.hasNext()) {
//			System.out.println(iterator.next());
//		}
//		
//		StmtIterator stiterator = model.listStatements(null, FOAF.name, (RDFNode)null);
//		while (stiterator.hasNext()) {
//			System.out.println(stiterator.next());
//		}
//			
//		model.removeAll();		

//		StmtIterator stiterator = model.listStatements();
//		while (stiterator.hasNext()) {
//			System.out.println(stiterator.next());
//		}
		
		factory.getClientShutdownHook().close(); // This is a CassandraClientShutdownHook
	}
}