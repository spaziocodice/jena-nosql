package org.gazzax.labs.jena.nosql.fwk.graph;

import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * NoSQL implementation of Jena Dataset.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class NoSqlDatasetGraph extends DatasetGraphCaching {

	private final StorageLayerFactory factory;
	
	/**
	 * Builds a new Dataset graph with the given factory.
	 * 
	 * @param factory the storage layer (abstract) factory.
	 */
	public NoSqlDatasetGraph(final StorageLayerFactory factory) {
		this.factory = factory;
	}
	
	@Override
	public Iterator<Node> listGraphNodes() {
		return namedGraphs.keys();
	}

	@Override
	protected void _close() {
		factory.getDictionary().close();
	}

	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return new NoSqlGraph(graphNode, factory);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new NoSqlGraph(factory);
	}

	@Override
	protected boolean _containsGraph(Node graphNode) {
		return false;
	}

	@Override
	protected void addToDftGraph(Node s, Node p, Node o) {
		getDefaultGraph().add(new Triple(s,p,o));
	}

	@Override
	protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void deleteFromDftGraph(Node s, Node p, Node o) {
		getDefaultGraph().delete(new Triple(s,p,o));

	}

	@Override
	protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub

	}

	@Override
	protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
		return triples2quads(Quad.tripleInQuad, getDefaultGraph().find(s, p, o));
	}

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}

}
