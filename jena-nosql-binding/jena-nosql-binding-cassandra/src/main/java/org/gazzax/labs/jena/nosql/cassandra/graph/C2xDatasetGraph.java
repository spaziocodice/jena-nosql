package org.gazzax.labs.jena.nosql.cassandra.graph;

import java.util.Iterator;

import org.openjena.atlas.lib.Closeable;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.util.Context;

public class C2xDatasetGraph extends DatasetGraphCaching implements Closeable {
	final Store store;
	final Context context;
	final ReificationStyle reificationStyle;
	
	public C2xDatasetGraph(
			final Store store, 
			final Context context, 
			final ReificationStyle reificationStyle,
			final Graph defaultGraph) {
		this.store = store;
		this.context = context;
		this.reificationStyle = reificationStyle;
		this.defaultGraph = defaultGraph;
	}
	
	@Override
	public Iterator<Node> listGraphNodes() {
		return store.listGraphNodes();
	}

	@Override
	protected void _close() {
		store.close();
	}

	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return new CGraph(graphNode, store, reificationStyle);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new CGraph(store, reificationStyle);
	}

	@Override
	protected boolean _containsGraph(final Node graphNode) {
		return store.containsGraph(graphNode);
	}

	@Override
	protected void addToDftGraph(final Node s, final Node p, final Node o) {
		Helper.addToDftGraph(this, s, p, o);
	}

	@Override
	protected void addToNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		Helper.addToNamedGraph(this, g, s, p, o);
	}

	@Override
	protected void deleteFromDftGraph(final Node s, final Node p, final Node o) {
		Helper.deleteFromDftGraph(this, s, p, o);
	}

	@Override
	protected void deleteFromNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		Helper.deleteFromNamedGraph(this, g, s, p, o);
	}

	@Override
	protected Iterator<Quad> findInDftGraph(final Node s, final Node p, final Node o) {
		return Helper.findInDftGraph(this, s, p, o );
	}

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		return Helper.findInSpecificNamedGraph(this, g, s, p, o);
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
		return Helper.findInAnyNamedGraphs(this, s, p, o);
	}
}