package org.gazzax.labs.jena.nosql.fwk;

import org.junit.Before;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.*;
/**
 * Test Case for {@link PersistentKeyValueMap}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentKeyValueMapTestCase {
	
	private PersistentKeyValueMap<String, String> map;
	
	private String defaultValue;
	
	@Before
	public void setUp() {
		defaultValue = randomString();
		map = new PersistentKeyValueMap<String, String>(String.class, String.class, randomString(), false, defaultValue);
	}
}
