package org.gazzax.labs.jena.nosql.fwk;

/**
 * Thrown in case of storage access / interaction failure.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class StorageLayerException extends Exception {
	private static final long serialVersionUID = -1040014582243655390L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param cause the exception cause.
	 */
	public StorageLayerException(final Throwable cause) {
		super(cause);
	}
}