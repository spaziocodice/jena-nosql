package org.gazzax.labs.jena.nosql.solr.dao;

import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNtURI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.util.NTriples;
import org.gazzax.labs.jena.nosql.solr.Field;

import com.google.common.collect.AbstractIterator;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;

public class SolrTripleIndexDAO implements TripleIndexDAO<Triple, TripleMatch> {

	private final static Iterator<Triple> EMPTY_TRIPLES_ITERATOR = new ArrayList<Triple>(0).iterator();
	
	private final SolrServer solr;
	
	/**
	 * Builds a new {@link TripleIndexDAO} with the given SOLR client.
	 * 
	 * @param solr the SOLR client.
	 */
	public SolrTripleIndexDAO(final SolrServer solr) {
		this.solr = solr;
	}

	@Override
	public void insertTriple(final Triple triple) throws StorageLayerException {
		final SolrInputDocument document = new SolrInputDocument();
		document.setField(Field.S, asNt(triple.getSubject()));
		document.setField(Field.P, asNtURI(triple.getPredicate()));
		document.setField(Field.O, asNt(triple.getSubject()));
		
		try {
			solr.add(document);
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}

	@Override
	public void deleteTriple(final Triple triple) throws StorageLayerException {
		try {
			solr.deleteByQuery(deleteQuery(triple));
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}
	
	/**
	 * Builds a delete query starting from a given triple.
	 * 
	 * @param triple the triple.
	 * @return a delete query starting from a given triple.
	 */
	private String deleteQuery(final Triple triple) {
		return new StringBuilder()
			.append(Field.S).append(":\"").append(asNt(triple.getSubject())).append("\" AND ")
			.append(Field.P).append(":\"").append(asNt(triple.getPredicate())).append("\" AND ")
			.append(Field.O).append(":\"").append(asNt(triple.getObject())).append("\"")
			.toString();
	} 
	
	// TODO: To be optimized...with this implementation wildcard queries are not supported
	// so if I need to delete 5 triples then 5 commands should be issued.
	@Override
	public List<Triple> deleteTriples(
			final Iterator<Triple> triples,
			final int batchSize) throws StorageLayerException {
		final List<Triple> result = new ArrayList<Triple>();
		while (triples.hasNext()) {
			final Triple triple = triples.next();
			try {
				final UpdateResponse response = solr.deleteByQuery(deleteQuery(triple));
				if (response.getStatus() == 0) {
					result.add(triple);
				}
			} catch (final Exception exception) {
				throw new StorageLayerException(exception);
			}
		}
		
		return null;
	}

	@Override
	public void executePendingMutations() throws StorageLayerException {
		try {
			solr.commit();
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}
	
	@Override
	public void clear() {
		try {
			solr.deleteByQuery("*;*");
		} catch (final Exception exception) {
			// TODO: log
			exception.printStackTrace();
		}
	}

	@Override
	public Iterator<Triple> query(final TripleMatch query) throws StorageLayerException {
		final SolrQuery q = new SolrQuery("*:*");
		q.setStart(0);
		final Node s = query.getMatchSubject();
		final Node p = query.getMatchPredicate();
		final Node o = query.getMatchObject();
		
		if (s != null) {
			q.addFilterQuery(
					new StringBuilder()
						.append(Field.S)
						.append(":\"")
						.append(asNt(s))
						.append("\"")
						.toString());
		}
		
		if (p != null) {
			q.addFilterQuery(
					new StringBuilder()
						.append(Field.P)
						.append(":\"")
						.append(asNtURI(p))
						.append("\"")
						.toString());
		}
		
		if (o != null) {
			q.addFilterQuery(
					new StringBuilder()
						.append(Field.O)
						.append(":\"")
						.append(asNt(o))
						.append("\"")
						.toString());
		}
		
		try {
			final QueryResponse response = solr.query(q);
		
			if (response.getResults().getNumFound() == 0) {
				return EMPTY_TRIPLES_ITERATOR;
			}

			return new AbstractIterator<Triple>() {
				
				int rowId;
				SolrDocumentList page = response.getResults();
				
				@Override
				protected Triple computeNext() {
					
					if (page.getStart() + page.size() == page.getNumFound()) {
						return endOfData();
					}					
					
					if (rowId == page.size() - 1) {
						rowId = 0;
						q.setStart(q.getStart() + page.size());
						try {
							page = solr.query(q).getResults();
						} catch (final SolrServerException exception) {
							throw new RuntimeException(exception);
						}
					}
					
					final SolrDocument document = page.get(rowId);
					return Triple.create(
							NTriples.asURIorBlankNode((String) document.getFieldValue(Field.S)), 
							NTriples.asURI((String) document.getFieldValue(Field.P)),
							NTriples.asNode((String) document.getFieldValue(Field.P)));
				}
			};
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}
}