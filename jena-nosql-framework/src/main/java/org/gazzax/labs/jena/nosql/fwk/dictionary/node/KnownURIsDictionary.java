package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.Constants.CHARSET_UTF8;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNtURI;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asURI;
import static org.gazzax.labs.jena.nosql.fwk.util.Utility.murmurHash3;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.mx.ManageableKnownURIsDictionary;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.sparql.vocabulary.DOAP;
import com.hp.hpl.jena.sparql.vocabulary.EARL;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.OWL;
/**
 * A dictionary that manages a fixed set of vocabularies.
 * This is useful when you want to separate the management of triples coming 
 * from well-known vocabularies such dc, dcterms, foaf.
 * Enabling this dictionary, which is not supposed to be used standalone, and 
 * decorating it with a {@link CacheValueDictionary} having a size moreless equal to 
 * the expected number of triples in managed vocabularies, allows for fast (in memory)
 * lookup of the corresponding entries.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class KnownURIsDictionary extends SingleIndexValueDictionary implements ManageableKnownURIsDictionary {

	static final byte KNOWN_URI_MARKER = 31;
	static final int ID_LENGTH = 19;
	static final String [] DEFAULT_DOMAINS = {
		DC.NS,
		DCTerms.NS,
		DOAP.NS,
		EARL.NS,
		FOAF.NS,
		OWL.NS};
//		RDF.NS,
//		RDFS.NS};
	
	private TopLevelDictionary decoratee;
	final String[] domains;

	private final AtomicLong idKnownURIsHitsCount = new AtomicLong();
	private final AtomicLong valueknownURIsHitsCount = new AtomicLong();
	
	/**
	 * Builds a new known uris dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 */
	public KnownURIsDictionary(final String id, final TopLevelDictionary decoratee) {
		this(id, decoratee, (String[])null);
	}

	/**
	 * Builds a new known uris dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 * @param domains the domains that will be managed by this dictionary.
	 */
	public KnownURIsDictionary(final String id, final TopLevelDictionary decoratee, final String ... domains) {
		super(id, "DICT_WELL_KNOWN_URIS");
		
		if (decoratee == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}
		
		this.decoratee = decoratee;
		this.domains = (domains != null && domains.length > 0) ? domains : DEFAULT_DOMAINS;
	}

	@Override
	public void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {		
		super.initialiseInternal(factory);
		decoratee.initialise(factory);
	}
	
	@Override
	protected byte[] getIdInternal(final Node value, final boolean p) throws StorageLayerException {
		if (value.isURI() && contains(value.getNameSpace())) {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			idKnownURIsHitsCount.incrementAndGet();
			final String nt = asNtURI(value);
			byte[] id = null;
			
			synchronized (this) {
				id = getID(nt, p);
				if (id[0] == NOT_SET[0]) {
					id = newId(nt, index);
					index.putQuick(nt, id);
				}
			}
			return id;
		} else {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return decoratee.getID(value, p);
		}
	}

	@Override
	protected Node getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		if (id[0] == KNOWN_URI_MARKER && id.length == ID_LENGTH) {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			valueknownURIsHitsCount.incrementAndGet();
			
			return asURI(getN3(id, p));
		} else {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return decoratee.getValue(id, p);
		}
	}

	@Override
	public void removeValue(final Node value, final boolean p) throws StorageLayerException {
		if (value.isURI() && contains(((Node_URI) value).getNameSpace())) {
			final String n3 = asNtURI(value);
			index.remove(n3);
		} else {
			decoratee.removeValue(value, p);
		}
	}

	/**
	 * Checks if the given prefix is managed by this dictionary.
	 * 
	 * @param prefix the prefix to check.
	 * @return true if the given prefix is managed by this dictionary.
	 */
	boolean contains(final String prefix) {
		for (final String domain : domains) {
			if (domain.equals(prefix)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates a new (hash) identifier for the given resource.
	 * 
	 * @param n3 the N3 representation of the resource.
	 * @return a new (hash) identifier for the given resource.
	 */
	protected byte[] makeNewHashID(final String n3) {
		final byte[] hash = murmurHash3(n3.getBytes(CHARSET_UTF8)).asBytes();
		final ByteBuffer buffer = ByteBuffer.allocate(ID_LENGTH);
		buffer.put(KNOWN_URI_MARKER);
		buffer.put(RESOURCE_BYTE_FLAG);

		buffer.put(hash);
		buffer.flip();
		return buffer.array();
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] != KNOWN_URI_MARKER && decoratee.isBNode(id);
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id[0] != KNOWN_URI_MARKER && decoratee.isLiteral(id);
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && ((id[0] == KNOWN_URI_MARKER && id[1] == RESOURCE_BYTE_FLAG && id.length == ID_LENGTH)
				|| (decoratee.isResource(id)));
	}

	@Override
	public long getIdKnownURIsHitsCount() {
		return idKnownURIsHitsCount.get();
	}

	@Override
	public long getValueKnownURIsHitsCount() {
		return valueknownURIsHitsCount.get();
	}
	
	@Override
	protected void closeInternal() {
		decoratee.close();
	}	
}