package org.gazzax.labs.jena.nosql.fwk.mx;

import javax.management.MXBean;

import org.gazzax.labs.jena.nosql.fwk.dictionary.node.KnownURIsDictionary;

/**
 * Management inteface for {@link KnownURIsDictionary}.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
@MXBean
public interface ManageableKnownURIsDictionary extends ManageableDictionary {	
	/**
	 * How many getID requests referred to a know URIs.
	 * Note that if this dictionary is wrapped (i.e. decorated) within a cache dictionary, 
	 * this metric will report a hit for *each* well known URIs. That is, if n requests 
	 * have been executed with the same well known URI, then this attribute will have 1 as value.
	 * 
	 * @return how many requests referred to a know URIs.
	 */
	long getIdKnownURIsHitsCount();
	
	/**
	 * How many getValue requests referred to a know URIs.
	 * Note that if this dictionary is wrapped (i.e. decorated) within a cache dictionary, 
	 * this metric will report a hit for *each* well known URIs. That is, if n requests 
	 * have been executed with the same well known URI, then this attribute will have 1 as value.
	 * 
	 * @return how many requests referred to a know URIs.
	 */
	long getValueKnownURIsHitsCount();	
}