package org.gazzax.labs.jena.nosql.cassandra.serializer;

import java.io.UnsupportedEncodingException;

import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.slf4j.LoggerFactory;

/**
 * A string serializer.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class StringSerializer extends Serializer<String> {
	@Override
	public byte[] serializeInternal(final String object) {
		try {
			return object.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			new Log(LoggerFactory.getLogger(getClass())).error(MessageCatalog._00011_UTF8_NOT_SUPPORTED, e);
			return null;
		}
	}

	@Override
	public String deserializeInternal(final byte[] serialized) {
		try {
			return new String(serialized, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			new Log(LoggerFactory.getLogger(getClass())).error(MessageCatalog._00011_UTF8_NOT_SUPPORTED, e);
			return null;
		}
	}

	@Override
	protected boolean isEqualInternal(final String a, final String b) {
		return a.equals(b);
	}
}