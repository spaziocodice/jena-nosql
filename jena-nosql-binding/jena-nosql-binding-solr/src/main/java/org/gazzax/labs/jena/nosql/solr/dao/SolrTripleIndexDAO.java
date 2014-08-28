package org.gazzax.labs.jena.nosql.solr.dao;

import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNtURI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.solr.Field;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;

public class SolrTripleIndexDAO implements TripleIndexDAO<Triple, TripleMatch> {
	protected final Log logger = new Log(LoggerFactory.getLogger(SolrTripleIndexDAO.class));
	
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
		document.setField(Field.O, asNt(triple.getObject()));
		
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
			.append(Field.S).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getSubject()))).append("\" AND ")
			.append(Field.P).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getPredicate()))).append("\" AND ")
			.append(Field.O).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getObject()))).append("\"")
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
			solr.deleteByQuery("*:*");
		} catch (final Exception exception) {
			logger.error(MessageCatalog._00170_UNABLE_TO_CLEAR, exception);
		}
	}

	@Override
	public Iterator<Triple> query(final TripleMatch query) throws StorageLayerException {
		final SolrQuery q = new SolrQuery();
		q.addSort(Field.ID, ORDER.asc);	
		q.setRows(10);
		final Node s = query.getMatchSubject();
		final Node p = query.getMatchPredicate();
		final Node o = query.getMatchObject();
		
		if (s != null) {
			q.addFilterQuery(newFilterQuery(Field.S, ClientUtils.escapeQueryChars(asNt(s))));
		}
		
		if (p != null) {
			q.addFilterQuery(newFilterQuery(Field.P, ClientUtils.escapeQueryChars(asNtURI(p))));
		}
		
		if (o != null) {
			q.addFilterQuery(newFilterQuery(Field.O, ClientUtils.escapeQueryChars(asNt(o))));
		}
		
		return new SolrDeepPagingIterator(solr, q);
	}
	
	/**
	 * Builds a filter query with the given data.
	 * 
	 * @param fieldName the field name.
	 * @param value the field value.
	 * @return a filter query with the given data.
	 */
	String newFilterQuery(final String fieldName, final String value) {
		return new StringBuilder()
			.append(fieldName)
			.append(":\"")
			.append(ClientUtils.escapeQueryChars(value))
			.append("\"")
			.toString();
	}
	
}