package data.tools;

import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class for "Writable" classes.
 * 
 * @author Nicomak
 *
 */
public class StringHelper {

	public static final Log LOG = LogFactory.getLog(StringHelper.class);
	
	private static Pattern quotes = Pattern.compile("\"");
	
	/**
	 * Removes double-quote characters from a string.
	 * 
	 * @param csvField
	 * @return
	 */
	public static String removeDoubleQuotes(String text) {
		return quotes.matcher(text).replaceAll("");
	}
	
	/**
	 * Parses a string into a float.
	 * Returns 0 if the value cannot be parsed.
	 * 
	 * @param csvField
	 * @return
	 */
	public static float parseFloat(String csvField) {
		try {
			return Float.parseFloat(csvField);
		} catch (Exception e) {
			return 0f;
		}
	}
	
	/**
	 * Parses a string into an int.
	 * Returns 0 if the value cannot be parsed.
	 * 
	 * @param csvField
	 * @return
	 */
	public static int parseInt(String csvField) {
		try {
			return Integer.parseInt(csvField);
		} catch (Exception e) {
			return 0;
		}
	}
	
	/**	
	 * Parses a "f" (false) or "t" (true) valued string into a boolean.
	 * Returns false if value is different than "t".
	 * 
	 * @param csvField
	 * @return
	 */
	public static boolean parseBoolean(String csvField) {
		if ("t".equals(csvField)) {
			return true;
		}
		return false;
	}
}
