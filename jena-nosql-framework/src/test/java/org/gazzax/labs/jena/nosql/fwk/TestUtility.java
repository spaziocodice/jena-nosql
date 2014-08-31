package org.gazzax.labs.jena.nosql.fwk;

import static org.mockito.Mockito.mock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.ClientShutdownHook;
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
public abstract class TestUtility {
	/**
	 * {@link StorageLayerFactory} implementation for tests.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 *
	 */
	public static class TestStorageLayerFactory extends StorageLayerFactory {
		
		@Override
		public void accept(final Configuration<Map<String, Object>> configuration) {
			// Nothing
		}
		
		@Override
		@SuppressWarnings("rawtypes")
		public GraphDAO getGraphDAO(final Node name) {
			return mock(GraphDAO.class);
		}
		
		@Override
		@SuppressWarnings("rawtypes")
		public GraphDAO getGraphDAO() {
			return mock(GraphDAO.class);
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public <K, V> MapDAO<K, V> getMapDAO(final Class<K> keyClass, final Class<V> valueClass, final boolean isBidirectional, final String name) {
			return mock(MapDAO.class);
		}
		
		@Override
		public String getInfo() {
			return randomString();
		}
		
		@Override
		public TopLevelDictionary getDictionary() {
			return mock(TopLevelDictionary.class);
		}
		
		@Override
		public ClientShutdownHook getClientShutdownHook() {
			return mock(ClientShutdownHook.class);
		}
	};
	
	public static final StorageLayerFactory STORAGE_LAYER_FACTORY = new TestStorageLayerFactory();
	
	public static final Random RANDOMIZER = new Random();
	public static final String DUMMY_BASE_URI = "http://example.org/";
	
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
	 * Produces a random boolean.
	 * 
	 * @return a random boolean.
	 */
	public static boolean randomBoolean() {
		return RANDOMIZER.nextBoolean();
	}
	
	/**
	 * Produces a random byte array.
	 * 
	 * @param size the size of the returned array.
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
	
	/**
	 * Creates a dummy configuration (TEST ONLY).
	 * 
	 * @param file the file that will hold this dummy configuration.
	 * @return a dummy configuration persieted to a (most probably) tmp file.
	 * @throws Exception in case any failure occurred.
	 */
	public static Map<String, Object> createSampleConfiguration(final File file) throws Exception {
		final Map<String, Object> configuration = new HashMap<String, Object>();
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file));
			for (int i = 0; i < RANDOMIZER.nextInt(10) + 2; i++) {
				final String key = String.valueOf(i);
				final String value = randomString();
				
				configuration.put(key, value);
				writer.write(key + ": \"" + value + "\"");
				writer.newLine();
			}
		} finally {
			writer.flush();
			writer.close();
		}
		return configuration;
	}
}