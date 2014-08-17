package org.gazzax.labs.jena.nosql.fwk.log;

/**
 * Message catalog.
 * An interface that (hopefully) enumerates all log messages.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface MessageCatalog {
	String PREFIX = "<JNSQL";
	String _00030_INVALID_CONFIGURATION_STATE = PREFIX + "-00030> : Configuration is not yet ready for use...maybe it hasn't been initialised.";
	String _00031_TRYING_ST_1 = PREFIX + "-00031> : Strategy I - trying system property %s (if it is there, it should point to a valid configuration file path).";
	String _00032_CONFIGURATION_FILE_NOT_FOUND = PREFIX + "-00032> : Configuration strategy with file declared in system property %s indicates a file (%s) that cannot be found.";
	String _00033_TRYING_ST_2 = PREFIX + "-00033> : Strategy II - trying system property %s (if it is there, it should point to a valid configuration directory).";
	String _00034_CONFIGURATION_DIR_STRATEGY = PREFIX + "-00034> : Configuring module using %s.";
	String _00035_CONFIGURATION_DIR_FILE_CANNOT_BE_READ = PREFIX + "-00035> : Configuration strategy with directory declared in system property %s fails because there's no such property or file cannot be read.";
	String _00036_CONFIGURATION_FILE_NOT_FOUND = PREFIX + "-00036> : Configuration strategy with directory declared in system property %s results in a file (%s) that cannot be found.";
	String _00037_TRYING_ST_3 = PREFIX + "-00037> : Strategy III - trying classpath resource with name %s";
	String _00038_USING_CLASSPATH_RESOURCE = PREFIX + "-00038> : Configuring module using classpath resource %s.";
	String _00039_CLASSPATH_STRATEGY_FAILURE = PREFIX + "-00039> : Unable to use a classpath resource to configure the module (probably that resource cannot be found).";
	String _00040_USING_ST_4 = PREFIX + "-00040> : Strategy IV - using default embedded classpath resource with name %s";
	String _00096_CONFIGURATION_FILE_STRATEGY_FAILURE = PREFIX + "-00096> : Configuration strategy with file declared in system property %s fails because there's no such property or file cannot be read.";
	String _00097_CONFIGURATION_FILE_STRATEGY = PREFIX + "-00097> : Configuring module using configuration file declared in system property %s (%s).";
	String _00098_INVALID_PMAP_ATTRIBUTE = PREFIX + "-00098> : Cannot build a valid instance of PersistentKeyValueMap: attribute %s is mandatory (was >%s<).";
	String _00099_CLIENT_SHUTDOWN_FAILURE = PREFIX + "-00099> : Unable to properly execute the shutdown procedure for client connection. See below for further details.";
	String _00100_UNABLE_TO_DELETE_TRIPLE = PREFIX + "-00100> : Unable to delete the following triple %s. See below for further details.";
	String _00101_UNABLE_TO_ADD_TRIPLE = PREFIX + "-00101> : Unable to add the following triple %s. See below for further details.";
	String _00102_UNABLE_TO_RESOLVE_COLLISION = PREFIX + "-00102> : Unable to resolve collision for node %s after %s tries.";
	String _00726_NODE_NOT_FOUND_IN_DICTIONARY = PREFIX + "-00726> : Node %s not found in dictionary.";
	String _00165_NULL_DECORATEE_DICT = PREFIX + "-00165> : Null dictionary decoratee.";
	String _00010_DATA_ACCESS_LAYER_FAILURE = PREFIX + "-00010> : Data access failure. See below for further details.";
	String _00011_UTF8_NOT_SUPPORTED = PREFIX + "-00011> : UTF-8 encoding not supported.";
	String _00098_COULD_NOT_GET_HASH = PREFIX + "-00098> : Could not get hash for value: %s";
	String _00111_MBEAN_REGISTERED = PREFIX + "-00111> : Management Interface for #%s has been registered with Management Server.";
	String _00166_MBEAN_ALREADY_REGISTERED = PREFIX + "-00166> : A Management Interface with ID #%s already exists on Management Server.";
	String _00167_MBEAN_UNREGISTERED = PREFIX + "-00167> : Management Interface with ID #%s has been unregistered from Management Server.";
	String _00168_UNABLE_TO_UNREGISTER_MBEAN = PREFIX + "-00168> : Unable to unregister the management interface with name #%s.";
}