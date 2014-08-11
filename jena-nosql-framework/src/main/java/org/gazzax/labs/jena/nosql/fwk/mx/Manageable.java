package org.gazzax.labs.jena.nosql.fwk.mx;


/**
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Manageable {
	/**
	 * Returns a mnemonic code that identifies this manageable instance.
	 * 
	 * @return a mnemonic code that identifies this manageable instance.
	 */
	String getName();
}