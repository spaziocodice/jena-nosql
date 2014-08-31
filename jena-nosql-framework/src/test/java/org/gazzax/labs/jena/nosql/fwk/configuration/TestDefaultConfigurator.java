package org.gazzax.labs.jena.nosql.fwk.configuration;

import static org.gazzax.labs.jena.nosql.fwk.TestUtility.createSampleConfiguration;
import static org.gazzax.labs.jena.nosql.fwk.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link DefaultConfigurator}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TestDefaultConfigurator {

	private DefaultConfigurator cut;
	private Configurable dummyTarget;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		cut = new DefaultConfigurator();
		dummyTarget = new Configurable() {
			@Override
			public void accept(
					final Configuration<Map<String, Object>> configuration) {
				// Nothing, this is a stupid stub.
			}
		};
	}

	/**
	 * Just after creating the instance, the active state must correspond to
	 * strategy one.
	 */
	@Test
	public void initialStateMustBeStrategyOne() {
		assertSame(cut.tryWithConfigurationFile, cut.currentState);
	}

	/**
	 * A call to getParameter cannot be issued before intialisation.
	 */
	@Test
	public void callInInvalidState() {
		try {
			cut.getParameter(randomString(), randomString());
			fail();
		} catch (final IllegalStateException expected) {
			// Nothing, this is the expected behaviour.
		}
	}

	/**
	 * Strategy #1 (configuration file in a system property).
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void strategyOneSucceeds() throws Exception {
		final File configurationFile = File.createTempFile(randomString(), ".yaml");
		final Map<String, Object> configuration = createSampleConfiguration(configurationFile);
		
		System.setProperty(
				DefaultConfigurator.CONFIGURATION_FILE_SYSTEM_PROPERTY,
				configurationFile.getAbsolutePath());
		
		cut.configure(dummyTarget);
		
		assertSame(cut.configurationHasBeenLoaded, cut.currentState);
		checkConfiguration(configuration, cut.parameters);
	}

	/**
	 * Strategy #2 (configuration directory in a system property with a fixed filename).
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void strategyTwoSucceeds() throws Exception {
		final File tmpConfigurationFile = File.createTempFile(randomString(), ".yaml");
		final File configurationFile = new File(tmpConfigurationFile.getParent(), DefaultConfigurator.CONFIGURATION_FILENAME);
		configurationFile.renameTo(configurationFile);
		final Map<String, Object> configuration = createSampleConfiguration(configurationFile);
		
		
		System.clearProperty(DefaultConfigurator.CONFIGURATION_FILE_SYSTEM_PROPERTY);
		System.setProperty(
				DefaultConfigurator.ETC_DIR_SYSTEM_PROPERTY,
				configurationFile.getParentFile().getAbsolutePath());
		
		cut.configure(dummyTarget);
		
		assertSame(cut.configurationHasBeenLoaded, cut.currentState);
		checkConfiguration(configuration, cut.parameters);
	}
	
	/**
	 * Internal method used for comparing two configurations.
	 * 
	 * @param src the first configuration.
	 * @param trgt the second configuration.
	 */
	private void checkConfiguration(final Map<String, Object> src, final Map<String, Object> trgt) {
		assertTrue(src.size() > 1);
		assertEquals(src.size(), trgt.size());
	
		for (String key : src.keySet()) {		
			final Integer k = Integer.parseInt(key);
			assertTrue("Key " + key + " found in src but not in target", trgt.containsKey(k));
			trgt.remove(k);
		}
		
		assertTrue(trgt.isEmpty());
	}	
}