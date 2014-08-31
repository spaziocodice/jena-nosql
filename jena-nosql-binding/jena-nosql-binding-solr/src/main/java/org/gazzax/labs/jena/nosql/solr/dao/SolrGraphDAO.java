package org.gazzax.labs.jena.nosql.solr.dao;

import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNtURI;

import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.solr.Field;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;

/**
 * {@link GraphDAO} implementation for Apache SOLR.
 * 
 * @see http://lucene.apache.org/solr
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrGraphDAO implements GraphDAO<Triple, TripleMatch> {
	protected final Log logger = new Log(LoggerFactory.getLogger(SolrGraphDAO.class));
	
	private final SolrServer solr;
	private final int addCommitWithinMsecs;
	private final int deleteCommitWithinMsecs;
	
	private final Node name;
	
	/**
	 * Builds a new {@link GraphDAO} with the given SOLR client.
	 * 
	 * @param addCommitWithin max time (in ms) before a commit will happen when inserting triples.
	 * @param deleteCommitWithin max time (in ms) before a commit will happen when deleting triples.
	 * @param solr the SOLR client.
	 */
	public SolrGraphDAO(final SolrServer solr, final int addCommitWithin, final int deleteCommitWithin) {
		this(solr, null, addCommitWithin, deleteCommitWithin);
	}
	
	/**
	 * Builds a new {@link GraphDAO} with the given SOLR client.
	 * 
	 * @param addCommitWithin max time (in ms) before a commit will happen when inserting triples.
	 * @param deleteCommitWithin max time (in ms) before a commit will happen when deleting triples.
	 * @param solr the SOLR client.
	 * @param name the name of the graph associated with this DAO.
	 */
	public SolrGraphDAO(final SolrServer solr, final Node name, final int addCommitWithin, final int deleteCommitWithin) {
		this.solr = solr;
		this.name = name;
		this.addCommitWithinMsecs = addCommitWithin;
		this.deleteCommitWithinMsecs = deleteCommitWithin;
	}

	@Override
	public void insertTriple(final Triple triple) throws StorageLayerException {
		final SolrInputDocument document = new SolrInputDocument();
		document.setField(Field.S, asNt(triple.getSubject()));
		document.setField(Field.P, asNtURI(triple.getPredicate()));
		document.setField(Field.O, asNt(triple.getObject()));
		document.setField(Field.C, name != null ? asNtURI(name) : null);
		
		try {
			solr.add(document, addCommitWithinMsecs);
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}

	@Override
	public void deleteTriple(final Triple triple) throws StorageLayerException {
		try {
			solr.deleteByQuery(deleteQuery(triple), deleteCommitWithinMsecs);
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}
	}
	
	@Override
	public List<Triple> deleteTriples(final Iterator<Triple> triples) {
		throw new UnsupportedOperationException("Not implemented when using SOLR.");
	}

	@Override
	public void executePendingMutations() throws StorageLayerException {
		try {
			solr.commit();
		} catch (Exception exception) {
			throw new StorageLayerException(exception);
		}
	}
	
	@Override
	public void clear() {
		try {
			solr.deleteByQuery(name == null ? "*:*" : (Field.C + ":\"" + ClientUtils.escapeQueryChars(asNtURI(name)) + "\""), deleteCommitWithinMsecs);
		} catch (final Exception exception) {
			logger.error(MessageCatalog._00170_UNABLE_TO_CLEAR, exception);
		}
	}

	@Override
	public Iterator<Triple> query(final TripleMatch query) throws StorageLayerException {
		final SolrQuery q = new SolrQuery();
		q.setRows(10);
		final Node s = query.getMatchSubject();
		final Node p = query.getMatchPredicate();
		final Node o = query.getMatchObject();
		
		if (s != null) {
			q.addFilterQuery(newFilterQuery(Field.S, asNt(s)));
		}
		
		if (p != null) {
			q.addFilterQuery(newFilterQuery(Field.P, asNtURI(p)));
		}
		
		if (o != null) {
			q.addFilterQuery(newFilterQuery(Field.O, asNt(o)));
		}
		
		if (name != null) {
			q.addFilterQuery(newFilterQuery(Field.C, asNtURI(name)));			
		}
		
		return new SolrDeepPagingIterator(solr, q);
	}
	

	@Override
	public long countTriples() throws StorageLayerException {
		final SolrQuery query = new SolrQuery();
		try {
			return solr.query(query).getResults().getNumFound();
		} catch (final Exception exception) {
			throw new StorageLayerException(exception);
		}		
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
	
	/**
	 * Builds a delete query starting from a given triple.
	 * 
	 * @param triple the triple.
	 * @return a delete query starting from a given triple.
	 */
	String deleteQuery(final Triple triple) {
		
		final StringBuilder builder = new StringBuilder();
		if (triple.getSubject().isConcrete()) {
			builder.append(Field.S).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getSubject()))).append("\"");
		}
		
		if (triple.getPredicate().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(Field.P).append(":\"").append(ClientUtils.escapeQueryChars(asNtURI(triple.getPredicate()))).append("\"");
		}
			
		if (triple.getObject().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(Field.O).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getObject()))).append("\"");
		}
			
		
		if (name != null) {
			builder.append(" AND ").append(Field.C).append(":\"").append(ClientUtils.escapeQueryChars(asNtURI(name))).append("\"");
		}
		
		return builder.toString();
	}
}