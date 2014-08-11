package org.gazzax.labs.jena.nosql.cassandra;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

// CHECKSTYLE:OFF
public class ExampleTest {
	public static void main(String[] args) {
		
		final StorageLayerFactory factory = StorageLayerFactory.getFactory();
		final Graph graph = factory.getGraph();
		
		graph.add(new Triple(
				NodeFactory.createURI("http://rdf.gx.org/id/resources#me"),
				FOAF.name.asNode(),
				NodeFactory.createLiteral("Andrea Gazzarini")
				));
//		storage.close();
	}
}
//CHECKSTYLE:ON