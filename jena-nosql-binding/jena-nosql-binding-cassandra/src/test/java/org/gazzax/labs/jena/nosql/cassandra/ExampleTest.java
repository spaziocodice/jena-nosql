package org.gazzax.labs.jena.nosql.cassandra;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

public class ExampleTest {
	
	public static void main(String[] args) throws Exception {
		// Instantiate the appropriate StorageLayerFactory using ServiceLoader
		// Behind the scenes it will be a CassandraStorageLayerFactory but I don't want to hard code "Cassandra", 
		// this has to work also with others NoSQL (e.g. SOLR, HBase)
		
		// Later I can provide a factory method for forcing a specific StorageLayerFactory impl.
		final StorageLayerFactory factory = StorageLayerFactory.getFactory();
		
		// Once obtained a concrete StorageLayerFactory, as the name suggests, it acts as a factory for all storage-specific
		// implementations of family members (Dictionary, Graph and so on)
		final Graph graph = factory.getGraph(); 
		final Model model = ModelFactory.createModelForGraph(graph); 

		Statement st = model.createStatement(
				model.createResource("http://rdf.gx.org/id/resources#me"),
				FOAF.name,
				model.createLiteral("Andrea Gazzarini"));

		Statement st1 = model.createStatement(
				model.createResource("http://rdf.gx.org/id/resources#him"),
				FOAF.name,
				model.createLiteral("Gazzax"));
		
		model.add(st);
		model.add(st1);

		NodeIterator iterator = model.listObjects();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());
		}
		
		StmtIterator stiterator = model.listStatements(null, FOAF.name, (RDFNode)null);
		while (stiterator.hasNext()) {
			System.out.println(stiterator.next());
		}
			
		model.removeAll();		

		stiterator = model.listStatements();
		while (stiterator.hasNext()) {
			System.out.println(stiterator.next());
		}
		
		factory.getClientShutdownHook().close(); // This is a CassandraClientShutdownHook
	}
}