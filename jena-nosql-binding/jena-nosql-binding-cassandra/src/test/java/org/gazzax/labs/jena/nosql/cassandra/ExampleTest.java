package org.gazzax.labs.jena.nosql.cassandra;

import java.io.FileReader;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ExampleTest {
	
	public static void main(String[] args) throws Exception {
		// Instantiate the appropriate StorageLayerFactory using ServiceLoader
		// Behind the scenes it will be a CassandraStorageLayerFactory but I don't want to hard code "Cassandra", 
		// this has to work also with others NoSQL (e.g. SOLR, HBase)
		
		// Later I can provide a factory method for forcing a specific StorageLayerFactory impl.
		final StorageLayerFactory factory = StorageLayerFactory.getFactory();
		
		// Once obtained a concrete StorageLayerFactory, as the name suggests, it acts as a factory for all storage-specific
		// implementations of family members (Dictionary, Graph and so on)
		final Graph graph = factory.getGraph(); // This is a CassandraGraph
		final Model model = ModelFactory.createModelForGraph(graph); 
		
		model.read(new FileReader("/work/data/rdf/sample.nt"), "http://base.example.org", "N3");	

		factory.getClientShutdownHook().close(); // This is a CassandraClientShutdownHook
	}
}