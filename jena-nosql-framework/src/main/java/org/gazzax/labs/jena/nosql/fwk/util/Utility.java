package org.gazzax.labs.jena.nosql.fwk.util;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.hp.hpl.jena.graph.Node;

/**
 * Booch utility for shared functions.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Utility {
	private static final HashFunction MURMUR_HASH_3 = Hashing.murmur3_128();

	/**
	 * Returns true if the given byte array represents a variable. The incoming
	 * value is considered a variable if
	 * 
	 * <ul>
	 * <li>Array is null</li>
	 * <li>Array is not null but empty (i.e. size is 0)</li>
	 * <ul>
	 * 
	 * @param value the value to be checked.
	 * @return true if the given value represents a variable.
	 */
	public static boolean isVariable(final byte[] value) {
		return value == null || value.length == 0;
	}

	/**
	 * Returns true if the given value is a variable. A value is considered a
	 * variable if it is null.
	 * 
	 * @param value the value that will be checked.
	 * @return true if the given value is a variable.
	 */
	public static boolean isVariable(final Node value) {
		return value == null;
	}

	/**
	 * Hashes the given byte[] with MurmurHash3.
	 * 
	 * @param input the input
	 * @return the hash of the input
	 */
	public static HashCode murmurHash3(final byte[] input) {
		return MURMUR_HASH_3.hashBytes(input);
	}
}