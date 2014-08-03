package org.gazzax.labs.jena.nosql.fwk.dictionary;

import java.util.Iterator;

import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * Front / Top level dictionary interface.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface TopLevelDictionary extends Dictionary<Node> {	
	/**
	 * Returns the identifiers of the given resources.
	 * 
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @return the identifiers of the given resources.
	 * @throws StorageLayerException in case of data access failure.
	 */
	byte[][] asIdentifiers(Node s, Node p, Node o) throws StorageLayerException;

	/**
	 * Returns the identifiers of the given resources.
	 * 
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * @return the identifiers of the given resources.
	 * @throws StorageLayerException in case of data access failure.
	 */
	byte[][] asIdentifiers(Node s, Node p, Node o, Node c) throws StorageLayerException;
	
	/**
	 * Converts the given identifiers in a Triple.
	 * 
	 * @param s the subject identifier.
	 * @param p the predicate identifier.
	 * @param o the object identifier.
	 * @return a {@link Triple} with Nodes associated with input identifiers.
	 * @throws StorageLayerException in case of data access failure.
	 */
	Triple asTriple(byte[] s, byte[] p, byte[] o) throws StorageLayerException;

	/**
	 * Converts the given identifiers in a Triple.
	 * 
	 * @param s the subject identifier.
	 * @param p the predicate identifier.
	 * @param o the object identifier.
	 * @param c the context identifier.
	 * @return a {@link Triple} with Nodes associated with input identifiers.
	 * @throws StorageLayerException in case of data access failure.
	 */
	Quad asQuad(byte[] s, byte[] p, byte[] o, byte[] c) throws StorageLayerException;

	/**
	 * Returns true if a given identifiers maps to a BNode.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a BNode.
	 */
	boolean isBNode(byte[] id);

	/**
	 * Returns true if a given identifiers maps to a Literal.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a Literal..
	 */
	boolean isLiteral(byte[] id);

	/**
	 * Returns true if a given identifiers maps to a Resource.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a Resource.
	 */
	boolean isResource(byte[] id);

	/**
	 * Converts a given iterator of Triples in an iterator of identifiers.
	 * 
	 * @param Triples the iterator of Triples.
	 * @return the corresponding iterator of identifiers.
	 */
	Iterator<byte[][]> asTripleIdentifiersIterator(Iterator<Triple> Triples);

	/**
	 * Converts the incoming Triples iterator in a ids (byte[][]) iterator.
	 * Each Triple in the incoming iterator is converted into the corresponding byte[][].
	 * 
	 * @param quads the iterator containing quads Triples.
	 * @return an iterator containing corresponding ids.
	 */
	Iterator<byte[][]> asQuadIdentifiersIterator(final Iterator<Quad> quads);
	
	/**
	 * Converts a given iterator of identifiers in an iterator of Triples.
	 * 
	 * @param identifiers the iterator of identifiers.
	 * @return the corresponding iterator of Triples.
	 */
	Iterator<Triple> asTripleIterator(Iterator<byte[][]> identifiers);

	/**
	 * Converts the incoming ids iterator in a Triple iterator.
	 * Each id in the incoming iterator is converted into the corresponding Triple.
	 * 
	 * @param quads the iterator containing quads ids.
	 * @return an iterator containing corresponding Triples.
	 */
	Iterator<Quad> asQuadIterator(final Iterator<byte[][]> quads);
	
	/**
	 * Given a set of identifiers this method returns the corresponding composite identifier.
	 * This is actually needed for generating composite primary keys because all CF row keys are byte [] but
	 * some of them are composed by just one term (e.g. s, p, o) while some others are actually composites (e.g. so, po, sp, spc).
	 * 
	 * When the identifier is a composite, its internal encoding depends on how the dictionary generates each identifier. 
	 * For example
	 * 
	 * - A dictionary (e.g. ReadOptimizeDictionary) could generate fixed length identifiers;
	 * - A dictionary (e.g. WriteOptimizeDictionary) could generate variable lenght identifiers;
	 * 
	 * While in the first case the deserialization is trivial, in the second scenario there should be some mechanism for correctly 
	 * get each sub-identifier.
	 * At the end, instead of putting that codec logic somewhere, the natural and cohesive place is the dictionary itself.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second identifier.
	 * @return a composite identifier containing sub-identifiers associated with input Nodes. 
	 */
	byte[] compose(byte[] id1, final byte [] id2);

	/**
	 * Given a set of identifiers this method returns the corresponding composite identifier.
	 * This is actually needed for generating composite primary keys because all CF row keys are byte [] but
	 * some of them are composed by just one term (e.g. s, p, o) while some others are actually composites (e.g. so, po, sp, spc).
	 * 
	 * When the identifier is a composite, its internal encoding depends on how the dictionary generates each identifier. 
	 * For example
	 * 
	 * - A dictionary (e.g. ReadOptimizeDictionary) could generate fixed length identifiers;
	 * - A dictionary (e.g. WriteOptimizeDictionary) could generate variable lenght identifiers;
	 * 
	 * While in the first case the deserialization is trivial, in the second scenario there should be some mechanism for correctly 
	 * get each sub-identifier.
	 * At the end, instead of putting that codec logic somewhere, the natural and cohesive place is the dictionary itself.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second identifier.
	 * @param id3 the third identifier.
	 * @return a composite identifier containing sub-identifiers associated with input Nodes. 
	 */
	byte[] compose(byte[] id1, final byte [] id2, byte[] id3);

	/**
	 * Decomposes a composite id returning an array of all compounding identifiers.
	 * 
	 * @param compositeId the composite identifiers.
	 * @return an array containing all compounding identifiers.
	 */
	byte[][] decompose(byte[] compositeId);
}