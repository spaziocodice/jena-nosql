package org.gazzax.labs.jena.nosql.fwk.dictionary.node;

import static org.gazzax.labs.jena.nosql.fwk.Constants.CHARSET_UTF8;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.fillIn;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.subarray;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asBlankNode;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asLiteral;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asURI;

import java.util.UUID;

import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.impl.LiteralLabel;

/**
 * Dictionary that uses strings encoding for generating variable-length identifiers.
 * 
 * Identifiers can be simple or compound. In case of a simple id then its value is the byte array
 * representation of the N3 value (String.getBytes(UTF-8)).
 * In case of compound id, the resulting byte array is composed in the following way
 * 
 * <ul>
 * 	<li>2 bytes indicating how many components we have in the identifier;</li>
 *  <li>2 bytes indicating the length of the sub-identifier;</li>
 *  <li>sub-identifier;</li>
 * </ul>
 * 
 * Note that 2nd and 3rd point are repeated for each sub-identifier.
 * 
 * In addition, this dictionary decorates and delegates identifier management to another dictionary in 
 * case the input value is a literal with a size exceeding a predefined length.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TransientValueDictionary extends ValueDictionaryBase {

	static final int DEFAULT_THRESHOLD = 1000; // 1K
	
	static final byte THRESHOLD_EXCEEDED = 1;
	static final byte THRESHOLD_NOT_EXCEEDED = 2;
			
	final int threshold; 
	TopLevelDictionary longLiteralsDictionary;

	/**
	 * Builds a new write optimized dictionary with the given data.
	 * 
	 * @param id the dictionary identifier.
	 * @param longLiteralsDictionary the dictionary that will be used for long literals.
	 * @param literalLengthThresold 
	 * 				the minimum literal length that will trigger involvment of the wrapped dictionary.
	 * 				0 disables the wrapped dictionary, -1 uses default {@link #DEFAULT_THRESHOLD}.
	 */
	public TransientValueDictionary(
			final String id, 
			final TopLevelDictionary longLiteralsDictionary, 
			final int literalLengthThresold) {
		super(id);
		
		if (longLiteralsDictionary == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}
		
		this.longLiteralsDictionary = longLiteralsDictionary;
		if (literalLengthThresold < 0) {
			threshold = DEFAULT_THRESHOLD;
		} else if (literalLengthThresold == 0) {
			threshold = Integer.MAX_VALUE;
		} else {
			threshold = literalLengthThresold;
		}
	}
	
	/**
	 * Builds a new write optimized dictionary with default values.
	 * Specifically 
	 * 
	 * <ul>
	 * 	<li>Long literals threshold length is 1K;</li>
	 * 	<li>Long literals dictionary is {@link PersistentValueDictionary}</li>
	 * </ul>
	 * 
	 * @param id the dictionary identifier.
	 */
	public TransientValueDictionary(final String id) {
		this(id, new PersistentValueDictionary(UUID.randomUUID().toString()), DEFAULT_THRESHOLD);
	}
	
	@Override
	protected void initialiseInternal(final StorageLayerFactory factory) throws InitialisationException {
		longLiteralsDictionary.initialise(factory);
	}
	
	@Override
	protected void closeInternal() {
		longLiteralsDictionary.close();
	}

	@Override
	public Node getValueInternal(final byte[] id, final boolean p) throws StorageLayerException {
		switch (id[0]) {
		case THRESHOLD_EXCEEDED:
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return longLiteralsDictionary.getValue(subarray(id, 1, id.length - 1), p);
		default:
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			final String n3 = new String(id, 2, id.length - 2, CHARSET_UTF8);
			switch(id[1]){
			case RESOURCE_BYTE_FLAG:
				return asURI(n3);
			case LITERAL_BYTE_FLAG:
				return asLiteral(n3);
			default:
				return asBlankNode(n3);
			}
		}
	}

	@Override
	public void removeValue(final Node value, final boolean p) throws StorageLayerException {
		final byte[] id = getID(value, p);
		if (id[0] == THRESHOLD_EXCEEDED) {
			longLiteralsDictionary.removeValue(value, p);
		}
	}

	@Override
	protected byte[] getIdInternal(final Node value, final boolean p) throws StorageLayerException {		
		if (value.isLiteral()) {
			final LiteralLabel literal = value.getLiteral();
			final String literalValue = String.valueOf(literal.getValue());
			if (literalValue.length() > threshold) {
				final byte[] idFromEmbeddedDictionary = longLiteralsDictionary.getID(value, p);
				final byte [] result = new byte[idFromEmbeddedDictionary.length + 1];
				result[0] = THRESHOLD_EXCEEDED;
				fillIn(result, 1, idFromEmbeddedDictionary);
				RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
				return result;
			}
		} 
		 
		final String n3 = asNt(value);

		final byte[] n3b = n3.getBytes(CHARSET_UTF8);
		final byte[] id = new byte[n3b.length + 2];
		
		if (value.isLiteral()) {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = LITERAL_BYTE_FLAG;
		} else if (value.isBlank()) {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = BNODE_BYTE_FLAG;
		} else {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = RESOURCE_BYTE_FLAG;
		}		
		
		fillIn(id, 2, n3b);
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return id;			
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == BNODE_BYTE_FLAG;
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && (id[0] == THRESHOLD_EXCEEDED || (id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == LITERAL_BYTE_FLAG));
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == RESOURCE_BYTE_FLAG;
	}
}