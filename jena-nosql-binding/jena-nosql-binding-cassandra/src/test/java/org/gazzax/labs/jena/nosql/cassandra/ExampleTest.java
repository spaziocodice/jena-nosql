package org.gazzax.labs.jena.nosql.cassandra;

import org.gazzax.labs.jena.nosql.fwk.Storage;

public class ExampleTest {
	public static void main(String[] args) {
		final Storage storage = new CassandraStorage();
		storage.close();
	}
}
