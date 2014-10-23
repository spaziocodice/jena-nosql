package org.gazzax.labs.jena.nosql.fwk.learningSPARQL;

/**
 * A simple value object encapsulating data (i.e. filenames) for a given test.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class MisteryGuest {
	final String [] datasets;
	final String query;
	final String description;
	
	/**
	 * Builds a new test bundle with the given data.
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param description a brief description about the data / query.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 */
	private MisteryGuest(final String queryFilename, final String description, final String ... datasetsFilenames) {
		this.datasets = datasetsFilenames;
		this.description = description;
		this.query = queryFilename;
	}
	
	/**
	 * Factory method. 
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param description a brief description about the data / query.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 * @return new {@link MisteryGuest} instance.
	 */
	static MisteryGuest misteryGuest(final String queryFilename, final String description, final String ... datasetsFilenames) {
		return new MisteryGuest(queryFilename, description, datasetsFilenames);
	}
}