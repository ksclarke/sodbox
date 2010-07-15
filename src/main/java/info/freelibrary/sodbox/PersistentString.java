package info.freelibrary.sodbox;

/**
 * Class encapsulating native Java string. java.lang.String is not persistent
 * object so it can not be stored in Sodbox as independent persistent object.
 * But sometimes it is needed. This class sole this problem providing implicit
 * conversion operator from java.lang.String to PerisstentString. Also
 * PersistentString class is mutable, allowing to change it's values.
 */
public class PersistentString extends PersistentResource {


	private String myString;
	
	/**
	 * Constructor of persistent string
	 * 
	 * @param aString A Java string
	 */
	public PersistentString(String aString) {
		myString = aString;
	}

	@SuppressWarnings("unused")
	private PersistentString() {
	}

	/**
	 * Get Java string
	 * 
	 * @return Java string
	 */
	public String toString() {
		return myString;
	}

	/**
	 * Append string to the current string value of PersistentString
	 * 
	 * @param tail appended string
	 */
	public void append(String tail) {
		modify();
		myString = myString + tail;
	}

	/**
	 * Assign new string value to the PersistentString
	 * 
	 * @param aString new string value
	 */
	public void set(String aString) {
		modify();
		myString = aString;
	}

	/**
	 * Get current string value
	 * 
	 * @return Java string
	 */
	public String get() {
		return myString;
	}
}
