package org.gazzax.labs.jena.nosql.fwk.mx;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.slf4j.LoggerFactory;

/**
 * Utility class for registering / unregistering MX beans.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class ManagementRegistrar {
	
	static final MBeanServer MX_SERVER = ManagementFactory.getPlatformMBeanServer();
	static final Log LOGGER = new Log(LoggerFactory.getLogger(ManagementRegistrar.class));
	static final String DOMAIN = "Jena-NoSQL:";

	/**
	 * Registers a value dictionary management interface.
	 * 
	 * @param dictionary the dictionary.
	 * @throws JMException in case of registration failure.
	 */
	public static void registerDictionary(final ManageableDictionary dictionary) throws JMException {
		register(dictionary, createDictionaryObjectName(dictionary.getName()));
	}

	/**
	 * General purposes registration method.
	 * Note that we usually prefer specific registration methods.
	 * 
	 * @param manageable the manageable instance to be registered.
	 * @param name the name of the management bean.
	 * @throws JMException in case of registration failure.
	 */
	public static void register(final Manageable manageable, final ObjectName name) throws JMException {
		if (MX_SERVER.isRegistered(name)) {
			throw new InstanceAlreadyExistsException();
		}
		
		MX_SERVER.registerMBean(manageable, name);
		LOGGER.info(MessageCatalog._00111_MBEAN_REGISTERED, manageable.getName());
	}
	
	/**
	 * Unregisters a dictionary management interface.
	 * 
	 * @param dictionary the dictionary.
	 */
	public static void unregisterDictionary(final ManageableDictionary dictionary) {
		unregister(createDictionaryObjectName(dictionary.getName()));
	}
	/**
	 * General purposes unregistration method.
	 * Note that we usually prefer specific registration methods.
	 * 
	 * @param name the name of the management bean.
	 */
	public static void unregister(final ObjectName name) {
		try {
			if (MX_SERVER.isRegistered(name)) {
				MX_SERVER.unregisterMBean(name);
			}

			LOGGER.info(MessageCatalog._00167_MBEAN_UNREGISTERED, name);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00168_UNABLE_TO_UNREGISTER_MBEAN, name, exception);
		}
	}
	
	/**
	 * ObjectNames (i.e. management names) factory for dictionaries.
	 * 
	 * @param id the dictionary identifier.
	 * @return the {@link ObjectName} associated with the given identifier. 
	 */
	static ObjectName createDictionaryObjectName(final String id) {
		try {
			return new ObjectName(DOMAIN + "Type=Dictionary,ID=" + id);
		} catch (final Exception exception) {
			throw new RuntimeException(exception);
		}
	}
}