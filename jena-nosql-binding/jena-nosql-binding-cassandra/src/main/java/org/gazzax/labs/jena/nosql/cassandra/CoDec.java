package org.gazzax.labs.jena.nosql.cassandra;

import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.decodeLong;
import static org.gazzax.labs.jena.nosql.fwk.util.Bytes.encode;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.gazzax.labs.jena.nosql.fwk.Constants;

import com.datastax.driver.core.utils.Bytes;

/**
 * Converts {@code <T>} to {@code ByteBuffer} and back.
 * 
 * This class has been derived from CumulusRDF code, with many thanks to CumulusRDF team for allowing this.
 * 
 * @see https://code.google.com/p/cumulusrdf
 * @param <T> The type to convert to and from.
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class CoDec<T> {
	public static final CoDec<String> STRING_SERIALIZER = new CoDec<String>() {
		@Override
		public byte[] doSerialize(final String object) {
			return object.getBytes(Constants.CHARSET_UTF8);
		}

		@Override
		public String deserializeInternal(final byte[] serialized) {
			return new String(serialized, Constants.CHARSET_UTF8);
		}

		@Override
		protected boolean doIsEqual(final String v1, final String v2) {
			return v1.equals(v2);
		}	
	};
	
	public static final CoDec<byte[]> BYTE_ARRAY_SERIALIZER = new CoDec<byte[]>() {
		@Override
		public byte[] doSerialize(final byte[] object) {
			return object;
		}

		@Override
		public byte[] deserializeInternal(final byte[] serialized) {
			return serialized;
		}

		@Override
		protected boolean doIsEqual(final byte[] v1, final byte[] v2) {
			return Arrays.equals(v1, v2);
		}			
	};
	
	public static final CoDec<Long> LONG_SERIALIZER = new CoDec<Long>() {
		@Override
		protected byte[] doSerialize(final Long object) {
			byte[] result = new byte[8];
			encode(object, result, 0);
			return result;
		}

		@Override
		protected Long deserializeInternal(final byte[] array) {
			return decodeLong(array, 0);
		}

		@Override
		protected boolean doIsEqual(final Long v1, final Long v2) {
			return (v1 == null && v2 == null) || (v1 == v2);
		}
	};

	/**
	 * Serializes the given object.
	 * 
	 * @param object The object to serialize.
	 * @return The serialized form of the object, as ByteBuffer.
	 */
	public final ByteBuffer serialize(final T object) {
		return ByteBuffer.wrap(doSerialize(object));
	}

	/**
	 * Serializes the given object.
	 * 
	 * @param object The object to serialize.
	 * @return The serialized form of the object, as byte[].
	 */
	public byte[] serializeDirect(final T object) {
		return doSerialize(object);
	}

	/**
	 * Serializes the given object into a byte[].
	 * 
	 * @param object The object.
	 * @return A byte[] containing the serialized object.
	 */
	protected abstract byte[] doSerialize(T object);

	/**
	 * Deserializes the object from the given ByteBuffer.
	 * 
	 * @param serialized The serialized object.
	 * @return The deserialized object.
	 */
	public T deserialize(final ByteBuffer serialized) {
		return deserializeInternal(Bytes.getArray(serialized));
	}

	/**
	 * Deserializes the given byte[].<br />
	 * It is possible that the given array backs a ByteBuffer, so changing it's
	 * content may result in undefined behavior.
	 * 
	 * @param array The byte[]. DO NOT MODIFY THE CONTENT!
	 * @return The deserialized object.
	 * @see ByteBuffer#array()
	 */
	protected abstract T deserializeInternal(byte[] array);

	/**
	 * Returns true if the two values are equal.
	 * 
	 * @param a The first value.
	 * @param b The second value.
	 * @return True if the two values are equal.
	 */
	public final boolean isEqual(final T a, final T b) {
		return (a == b) || ( (a != null && b != null) && doIsEqual(a, b) );
	}
	
	/**
	 * Returns true if the two values are equal.
	 * The given values are never null.
	 * 
	 * @param a The first value.
	 * @param b The second value.
	 * @return True if the two values are equal.
	 */
	protected abstract boolean doIsEqual(T a, T b);
	
	/**
	 * Returns the serializer associated with the given class.
	 * Note: differently from its sibling implementation coming from Hector, 
	 * this class could return null results, if no serializer is found for a given class.
	 * 
	 * @param clazz the class.
	 * @param <T> the type managed by the returned serializer.
	 * @return the serializer associated with the given class, or null if no suitable serializer is found.
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoDec<T> get(final Class<?> clazz) {
		if (byte[].class.equals(clazz)) {
			return (CoDec<T>) BYTE_ARRAY_SERIALIZER;
		} else if (Long.class.equals(clazz) || long.class.equals(clazz)) {
			return (CoDec<T>) LONG_SERIALIZER;
		} else if (String.class.equals(clazz)) {
			return (CoDec<T>) STRING_SERIALIZER;
		}
		throw new IllegalArgumentException("Cannot find Serializer for " + clazz);
	}
}