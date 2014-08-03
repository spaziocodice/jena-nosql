package org.gazzax.labs.jena.nosql.cassandra.graph;

import org.gazzax.labs.jena.nosql.fwk.Storage;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.graph.GraphBase2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class CGraph extends GraphBase2 {

	final Storage storage;
	final ReificationStyle reificationStyle;
	final Node graphNode ;

	public CGraph(final Storage storage, final ReificationStyle reificationStyle) {
		this(null, storage, reificationStyle);
	}

	public CGraph(final Node graphNode, final Storage storage, final ReificationStyle reificationStyle) {
		this.storage = storage;
		this.reificationStyle = reificationStyle;
		this.graphNode = graphNode;
	}

	@Override
	public QueryHandler queryHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected PrefixMapping createPrefixMapping() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void performAdd(final Triple triple) {
	}
	
	@Override
	public void performDelete(final Triple triple) {
		super.performDelete(triple);
	}
}