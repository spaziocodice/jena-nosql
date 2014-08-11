package org.gazzax.labs.jena.nosql.fwk.mx;

import javax.management.MXBean;

/**
 * Dictionary common management interface.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@MXBean
public interface ManageableDictionary extends Manageable {
	/**
	 * The total number of identifier lookups occurred since this dictionary has been created.
	 * 
	 * @return the total number of identifier lookups occurred since this dictionary has been created.
	 */
	long getIdLookupsCount();
	
	/**
	 * The total number of value lookups occurred since this dictionary has been created.
	 * 
	 * @return the total number of value lookups occurred since this dictionary has been created.
	 */
	long getValueLookupsCount();	
}