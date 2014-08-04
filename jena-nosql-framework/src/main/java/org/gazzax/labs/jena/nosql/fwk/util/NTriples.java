package org.gazzax.labs.jena.nosql.fwk.util;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

/**
 * Booch utility for parsing RDF resources.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class NTriples {
	private final static String START_URI_CHAR = "<";
	private final static String END_URI_CHAR = ">";
	private final static String START_BNODE_CHARS = "_:";
	private final static String START_LITERAL_CHAR = "\"";
	private final static String LANGUAGE_MARKER = "@";
	private final static String DATATYPE_MARKER = "^^";
	
	/**
	 * Returns true if the given NT value denotes a URI.
	 * 
	 * @param nt the value to be checked.
	 * @return true if the given NT value denotes a URI.
	 */
	static boolean isURI(final String nt) {
		return nt != null && nt.startsWith(START_URI_CHAR) && nt.endsWith(END_URI_CHAR);
	}
	
	/**
	 * Returns true if the given NT value denotes a blank node.
	 * 
	 * @param nt the value to be checked.
	 * @return true if the given NT value denotes a blank node.
	 */
	static boolean isBlankNode(final String nt) {
		return nt != null && nt.startsWith(START_BNODE_CHARS);
	}
	
	/**
	 * Tries to parse the given input as a (nt) resource.
	 * 
	 * @param nt
	 * @return
	 * @throws IllegalArgumentException
	 */
	// FIXME: bad sequential logic
	public static Node asNode(final String nt) throws IllegalArgumentException {
		if (isURI(nt)) {
			return internalAsURI(nt);
		} else if (isBlankNode(nt)) {
			return internalAsBlankNode(nt);
		} 
		return asLiteral(nt);
	}

	public static Node asURIorBlankNode(final String nt) throws IllegalArgumentException {
		return (isURI(nt)) ? internalAsURI(nt) : internalAsBlankNode(nt);
	}

	public static Node asURI(final String nt) throws IllegalArgumentException {
		if (isURI(nt)) {
			return internalAsURI(nt);
		} 
		throw new IllegalArgumentException(nt);
	}

	/**
	 * Parses the given input as URI.
	 * 
	 * @param nt the URI string value.
	 * @return the {@link Node} URI representation of the given value.
	 */
	private static Node internalAsURI(final String nt) {
		final String uri = unescapeString(nt.substring(1, nt.length() - 1));
		return NodeFactory.createURI(uri);		
	}
	
	/**
	 * Parses the given input as a blank node.
	 * 
	 * @param nt the blank node string value.
	 * @return the {@link Node} Blank node representation of the given value.
	 */
	private static Node internalAsBlankNode(final String nt) {
		return NodeFactory.createAnon(AnonId.create(nt.substring(2)));	
	}
	
	
	public static Node asBlankNode(String nt) throws IllegalArgumentException {
		if (isBlankNode(nt)) {
			return internalAsBlankNode(nt);
		}
		throw new IllegalArgumentException(nt);
	}
	
	public static Node asLiteral(final String nt) throws IllegalArgumentException
	{
		if (nt.startsWith(START_LITERAL_CHAR)) {
			int endIndexOfValue = endIndexOfValue(nt);

			if (endIndexOfValue != -1) {
				final String literalValue = unescapeString(nt.substring(1, endIndexOfValue));

				final int startIndexOfLanguage = nt.indexOf(LANGUAGE_MARKER, endIndexOfValue);
				final int startIndexOfDatatype = nt.indexOf(DATATYPE_MARKER, endIndexOfValue);

				if (startIndexOfLanguage != -1) {
					return NodeFactory.createLiteral(
							literalValue, 
							nt.substring(startIndexOfLanguage + LANGUAGE_MARKER.length()), 
							null);
				} else if (startIndexOfDatatype != -1) {
					return NodeFactory.createLiteral(
							literalValue, 
							null, 
							NodeFactory.getType(nt.substring(startIndexOfDatatype + DATATYPE_MARKER.length())));
				} else {
					return NodeFactory.createLiteral(literalValue);
				}
			}
		}

		throw new IllegalArgumentException(nt);
	}

	private static int endIndexOfValue(String nTriplesLiteral) {
		boolean previousWasBackslash = false;

		for (int i = 1; i < nTriplesLiteral.length(); i++) {
			char c = nTriplesLiteral.charAt(i);

			if (c == '"' && !previousWasBackslash) {
				return i;
			}
			else if (c == '\\' && !previousWasBackslash) {
				// start of escape
				previousWasBackslash = true;
			}
			else if (previousWasBackslash) {
				// c was escaped
				previousWasBackslash = false;
			}
		}

		return -1;
	}

	/**
	 * Creates an N-Triples string for the supplied value.
	 */
	public static String asNt(Node value) {
		if (value.isURI()) {
			return asNtURI(value);
		} else if (value.isBlank()) {
			return asNtBlankNode(value);		
		} else if (value.isLiteral()) {
			return asNtLiteral(value);
		}
		throw new IllegalArgumentException("Unknown value type: " + value.getClass());
	}

	public static String asNtURI(Node uri) {
		final StringBuilder buffer = new StringBuilder("<");
		escapeAndAppend(uri.getURI(), buffer);
		return buffer.append(">").toString();
	}
	
	public static String asNtBlankNode(final Node blankNode) {
		return new StringBuilder("_:")
			.append(blankNode.getBlankNodeLabel())
			.toString();
	}

	public static String asNtLiteral(final Node literal) {
		final StringBuilder buffer = new StringBuilder("\"");
		escapeAndAppend(String.valueOf(literal.getLiteralValue()), buffer);
		buffer.append("\"");
		final String language = literal.getLiteralLanguage();
		if (language != null) {
			buffer.append("@").append(language);
		}
		
		final String datatypeURI = literal.getLiteralDatatypeURI();
		if (datatypeURI != null) {
			buffer.append("^^<");
			escapeAndAppend(datatypeURI, buffer);
			buffer.append(">");
		}
		return buffer.toString();
	}

	/**
	 * Checks whether the supplied character is a letter or number according to
	 * the N-Triples specification.
	 * 
	 * @see #isLetter
	 * @see #isNumber
	 */
	public static boolean isLetterOrNumber(int c) {
		return isLetter(c) || isNumber(c);
	}

	/**
	 * Checks whether the supplied character is a letter according to the
	 * N-Triples specification. N-Triples letters are A - Z and a - z.
	 */
	public static boolean isLetter(int c) {
		return (c >= 65 && c <= 90) || // A - Z
				(c >= 97 && c <= 122); // a - z
	}

	/**
	 * Checks whether the supplied character is a number according to the
	 * N-Triples specification. N-Triples numbers are 0 - 9.
	 */
	public static boolean isNumber(int c) {
		return (c >= 48 && c <= 57); // 0 - 9
	}

	public static void escapeAndAppend(String value, final StringBuilder buffer)
	{
		int labelLength = value.length();

		for (int i = 0; i < labelLength; i++) {
			char c = value.charAt(i);
			int cInt = c;
			switch(c) {
			case '\\':
				buffer.append("\\\\");
				break;
			case '\"' : 
				buffer.append("\\\"");
				break;
			case '\n':
				buffer.append("\\n");
				break;
			case '\r':
				buffer.append("\\r");
				break;
			case '\t':
				buffer.append("\\t");
				break;
			default:
				if (cInt >= 0x0 && cInt <= 0x8 || cInt == 0xB || cInt == 0xC || cInt >= 0xE && cInt <= 0x1F
				|| cInt >= 0x7F && cInt <= 0xFFFF) {
					buffer.append("\\u");
					buffer.append(toHexString(cInt, 4));
				} else if (cInt >= 0x10000 && cInt <= 0x10FFFF) {
					buffer.append("\\U");
					buffer.append(toHexString(cInt, 8));
				}
				else {
					buffer.append(c);
				}
			} 
		}
	}

	/**
	 * Unescapes an escaped Unicode string. Any Unicode sequences (
	 * <tt>&#x5C;uxxxx</tt> and <tt>&#x5C;Uxxxxxxxx</tt>) are restored to the
	 * value indicated by the hexadecimal argument and any backslash-escapes (
	 * <tt>\"</tt>, <tt>\\</tt>, etc.) are decoded to their original form.
	 * 
	 * @param s
	 *        An escaped Unicode string.
	 * @return The unescaped string.
	 * @throws IllegalArgumentException
	 *         If the supplied string is not a correctly escaped N-Triples
	 *         string.
	 */
	public static String unescapeString(String s) {
		int backSlashIdx = s.indexOf('\\');

		if (backSlashIdx == -1) {
			// No escaped characters found
			return s;
		}

		int startIdx = 0;
		int sLength = s.length();
		StringBuilder sb = new StringBuilder(sLength);

		while (backSlashIdx != -1) {
			sb.append(s.substring(startIdx, backSlashIdx));

			if (backSlashIdx + 1 >= sLength) {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			char c = s.charAt(backSlashIdx + 1);

			if (c == 't') {
				sb.append('\t');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'b') {
				sb.append('\b');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'n') {
				sb.append('\n');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'r') {
				sb.append('\r');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'f') {
				sb.append('\f');
				startIdx = backSlashIdx + 2;
			}
			else if (c == '"') {
				sb.append('"');
				startIdx = backSlashIdx + 2;
			}
			else if (c == '\'') {
				sb.append('\'');
				startIdx = backSlashIdx + 2;
			}
			else if (c == '\\') {
				sb.append('\\');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'u') {
				// \\uxxxx
				if (backSlashIdx + 5 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 6);

				try {
					c = (char)Integer.parseInt(xx, 16);
					sb.append(c);

					startIdx = backSlashIdx + 6;
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\u" + xx + "' in: " + s);
				}
			}
			else if (c == 'U') {
				// \\Uxxxxxxxx
				if (backSlashIdx + 9 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 10);

				try {
					c = (char)Integer.parseInt(xx, 16);
					sb.append(c);

					startIdx = backSlashIdx + 10;
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\U" + xx + "' in: " + s);
				}
			}
			else {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			backSlashIdx = s.indexOf('\\', startIdx);
		}

		sb.append(s.substring(startIdx));

		return sb.toString();
	}

	/**
	 * Converts a decimal value to a hexadecimal string represention of the
	 * specified length.
	 * 
	 * @param decimal
	 *        A decimal value.
	 * @param stringLength
	 *        The length of the resulting string.
	 */
	public static String toHexString(int decimal, int stringLength) {
		StringBuilder sb = new StringBuilder(stringLength);

		String hexVal = Integer.toHexString(decimal).toUpperCase();

		// insert zeros if hexVal has less than stringLength characters:
		int nofZeros = stringLength - hexVal.length();
		for (int i = 0; i < nofZeros; i++) {
			sb.append('0');
		}

		sb.append(hexVal);

		return sb.toString();
	}
}
