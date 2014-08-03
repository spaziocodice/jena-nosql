package org.gazzax.labs.jena.nosql.fwk;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;

/**
 * A Data Access Object towards a concrete storage.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Storage 
{
    void close() ;

    boolean isClosed();
    
    //?? 
	void addGraphInfoToStore(Node graphNode, Graph graph);

	Iterator<Node> listGraphNodes();

	boolean containsGraph(Node graphNode);

	Iterator<Graph> listGraphs();

	void removeGraph(Node graphNode);
}