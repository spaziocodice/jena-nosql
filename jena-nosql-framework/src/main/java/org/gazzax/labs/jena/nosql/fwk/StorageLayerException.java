package org.gazzax.labs.jena.nosql.fwk;

import java.io.File;

/**
 * Thrown in case of storage access / interaction failure.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class StorageLayerException extends Exception {

	private static final long serialVersionUID = -1040014582243655390L;
	
	public static void main(String[] args) {
		File f  = new File("");
		System.out.println(f.getAbsolutePath());
		System.out.println(f.exists());
	}
}