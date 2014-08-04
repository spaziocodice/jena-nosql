package org.gazzax.labs.jena.nosql.fwk;

import java.util.Random;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

public class TestUtility {

	public final static StorageLayerFactory STORAGE_LAYER_FACTORY = null;
	public final static Random RANDOMIZER = new Random();
	
	public static String randomString() {
		final long value = RANDOMIZER.nextLong();
		return String.valueOf(value < 0 ? value * -1 : value);
	}
	
	public static void main(String[] args) {
		Node n = NodeFactory.createURI("http://xmlns.com/foaf/0.1#952994123989279397");
		System.out.println(n.getNameSpace());
	}
	
	public static Node buildResource(final String uriOrLocalName) {
		return NodeFactory.createURI(
				uriOrLocalName.startsWith("http") 
					? uriOrLocalName 
					: "http://gazzax.rdf.org/" + uriOrLocalName);
	}
	
	public static Node buildLiteral(final String label) {
		return NodeFactory.createLiteral(label);
	}
	
	public static Node buildBNode(final String id) {
		return NodeFactory.createAnon(AnonId.create(id));
	}	
}