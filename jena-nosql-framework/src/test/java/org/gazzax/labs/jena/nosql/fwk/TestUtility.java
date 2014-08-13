package org.gazzax.labs.jena.nosql.fwk;

import java.util.Random;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

/**
 * A bunch of test utilities.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TestUtility {
	public final static StorageLayerFactory STORAGE_LAYER_FACTORY = null;
	public final static Random RANDOMIZER = new Random();
	
	/**
	 * Produces a random string.
	 * 
	 * @return a random string.
	 */
	public static String randomString() {
		final long value = RANDOMIZER.nextLong();
		return String.valueOf(value < 0 ? value * -1 : value);
	}
	
	/**
	 * Produces a random byte array.
	 * 
	 * @return a random byte array.
	 */
	public static byte[] randomBytes(final int size) {
		final byte[] value = new byte[size];
		RANDOMIZER.nextBytes(value);
		return value;
	}
	
	/**
	 * Builds a new URI.
	 * If the input parameter starts with http then it is directly used as URI value. Otherwise
	 * if it is a local name, a common prefix will be prepended.
	 * 
	 * @param uriOrLocalName the URI or local name.
	 * @return a new URI.
	 */
	public static Node buildResource(final String uriOrLocalName) {
		return NodeFactory.createURI(
				uriOrLocalName.startsWith("http") 
					? uriOrLocalName 
					: "http://gazzax.rdf.org/" + uriOrLocalName);
	}
	
	/**
	 * Builds a literal with the given label.
	 * 
	 * @param label the label.
	 * @return a literal with the given label.
	 */
	public static Node buildLiteral(final String label) {
		return NodeFactory.createLiteral(label);
	}
	
	/**
	 * Builds a blank node with the given id.
	 * 
	 * @param id the node id.
	 * @return a blank node with the given id.
	 */
	public static Node buildBNode(final String id) {
		return NodeFactory.createAnon(AnonId.create(id));
	}	
}