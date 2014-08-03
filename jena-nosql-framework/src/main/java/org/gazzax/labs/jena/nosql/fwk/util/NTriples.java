package org.gazzax.labs.jena.nosql.fwk.util;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;

public class NTriples {

	public static Node asNode(final String nt) throws IllegalArgumentException {
		if (nt != null) {
			if (nt.startsWith("<")) {
				return asURI(nt);
			}
			else if (nt.startsWith("_:")) {
				return asBlankNode(nt);
			}
			else if (nt.startsWith("\"")) {
				return asLiteral(nt);
			}
		}
		throw new IllegalArgumentException(nt);
	}

	public static Node asURIorBlankNode(final String nt) throws IllegalArgumentException {
		if (nt.startsWith("<") && nt.endsWith(">")) {
			return internalAsURI(nt);
		} else if (nt.startsWith("_:")) {
			return asBlankNode(nt);
		}
		throw new IllegalArgumentException(nt);
	}

	public static Node asURI(final String nt) throws IllegalArgumentException {
		if (nt.startsWith("<") && nt.endsWith(">")) {
			internalAsURI(nt);
		} 
		throw new IllegalArgumentException(nt);
	}

	private static Node internalAsURI(final String nt) {
		final String uri = unescapeString(nt.substring(1, nt.length() - 1));
		return Node.createURI(uri);		
	}
	
	public static Node asBlankNode(String nt) throws IllegalArgumentException {
		if (nt.startsWith("_:")) {
			return Node.createAnon(AnonId.create(nt.substring(2)));
		}
		throw new IllegalArgumentException(nt);
	}

	public static Node asLiteral(String nt) throws IllegalArgumentException
	{
		if (nt.startsWith("\"")) {
			// Find string separation points
			int endLabelIdx = findEndOfLabel(nt);

			if (endLabelIdx != -1) {
				int startLangIdx = nt.indexOf("@", endLabelIdx);
				int startDtIdx = nt.indexOf("^^", endLabelIdx);

				if (startLangIdx != -1 && startDtIdx != -1) {
					throw new IllegalArgumentException("Literals can not have both a language and a datatype");
				}

				// Get label
				String label = nt.substring(1, endLabelIdx);
				label = unescapeString(label);

				if (startLangIdx != -1) {
					// Get language
					String language = nt.substring(startLangIdx + 1);
					return Node.createLiteral(label, language, null);
				} else if (startDtIdx != -1) {
					// Get datatype
					String datatype = nt.substring(startDtIdx + 2);
					return Node.createLiteral(label, null, Node.getType(datatype));
				} else {
					return Node.createLiteral(label);
				}
			}
		}

		throw new IllegalArgumentException(nt);
	}

	private static int findEndOfLabel(String nTriplesLiteral) {
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
