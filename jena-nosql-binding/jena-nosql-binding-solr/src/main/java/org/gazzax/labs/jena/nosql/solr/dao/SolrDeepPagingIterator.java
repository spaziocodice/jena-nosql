package org.gazzax.labs.jena.nosql.solr.dao;

import java.util.Iterator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.gazzax.labs.jena.nosql.fwk.util.NTriples;
import org.gazzax.labs.jena.nosql.solr.Field;

import com.google.common.collect.UnmodifiableIterator;
import com.hp.hpl.jena.graph.Triple;

/**
 * An iterator over SOLR results that uses the built-in Deep Paging strategy.
 * Internally it uses other iterators to represents each iteration state. 
 * 
 * @see http://solr.pl/en/2014/03/10/solr-4-7-efficient-deep-paging
 * @see http://heliosearch.org/solr/paging-and-deep-paging
 * @see <a href="http://en.wikipedia.org/wiki/Finite-state_machine">http://en.wikipedia.org/wiki/Finite-state_machine</a>
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrDeepPagingIterator extends UnmodifiableIterator<Triple> {

	private final SolrServer solr;
	private final SolrQuery query;
	private SolrDocumentList page;
	
	private String nextCursorMark;
	private String sentCursorMark;
	
	/**
	 * Iteration state: we need to (re)execute a query. 
	 * This could be needed the very first time we start iteration and each time the current result
	 * page has been consumed.
	 */
	private final Iterator<Triple> executeQuery = new UnmodifiableIterator<Triple>() {
		@Override
		public boolean hasNext() {
			try {
				final QueryResponse response = solr.query(query);
				nextCursorMark = response.getNextCursorMark();
				page = response.getResults();
				return page.getNumFound() > 0;
			} catch (final Exception exception) {
				throw new RuntimeException(exception);
			}
		}

		@Override
		public Triple next() {
			currentState = iterateOverCurrentPage;
			return currentState.next();
		}
	};
			
	/**
	 * Iteration state: query has been executed and now it's time to iterate over results. 
	 */
	private final Iterator<Triple> iterateOverCurrentPage = new UnmodifiableIterator<Triple>() {
		Iterator<SolrDocument> iterator;
		
		@Override
		public boolean hasNext() {
			if (iterator().hasNext()) {
				return true;
			} else {
				currentState = checkForIterationCompleteness;
				return currentState.hasNext();
			}
		}
		
		@Override
		public Triple next() {
			final SolrDocument document = iterator().next();
			return Triple.create(
					NTriples.asURIorBlankNode((String) document.getFieldValue(Field.S)), 
					NTriples.asURI((String) document.getFieldValue(Field.P)),
					NTriples.asNode((String) document.getFieldValue(Field.O)));
		}
		
		Iterator<SolrDocument> iterator() {
			if (iterator == null) {
				iterator = page.iterator();	
			}
			return iterator;
			 
		}
	};

	/**
	 * Iteration state: once a page has been consumed we need to determine if another query should be issued or not. 
	 */
	private final Iterator<Triple> checkForIterationCompleteness = new UnmodifiableIterator<Triple>() {
		@Override
		public boolean hasNext() {
			return !(page.size() < query.getRows() || sentCursorMark.equals(nextCursorMark));
		}

		@Override
		public Triple next() {
			query.set(CursorMarkParams.CURSOR_MARK_PARAM, nextCursorMark);
			currentState = executeQuery;
			return currentState.next();
		}
	};
	
	private Iterator<Triple> currentState = executeQuery;
	
	/**
	 * Builds a new iterator with the given data.
	 * 
	 * @param solr the SOLR facade.
	 * @param query the query that will be submitted.
	 */
	public SolrDeepPagingIterator(final SolrServer solr, final SolrQuery query) {
		this.solr = solr;
		this.query = query;
		this.sentCursorMark = CursorMarkParams.CURSOR_MARK_START;
		this.query.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
	}

	@Override
	public boolean hasNext() {
		return currentState.hasNext();
	}

	@Override
	public Triple next() {
		return currentState.next();
	}
}