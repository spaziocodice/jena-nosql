package org.gazzax.labs.jena.nosql.cassandra.serializer;

import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.decodeLong;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.encode;

/**
 * A long serializer.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LongSerializer extends Serializer<Long> {
	@Override
	protected byte[] serializeInternal(final Long object) {
		byte[] result = new byte[8];
		encode(object, result, 0);
		return result;
	}

	@Override
	protected Long deserializeInternal(final byte[] array) {
		return decodeLong(array, 0);
	}

	@Override
	protected boolean isEqualInternal(final Long a, final Long b) {
		return a == b;
	}
}