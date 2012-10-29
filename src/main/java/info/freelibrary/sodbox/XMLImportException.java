package info.freelibrary.sodbox;

/**
 * Exception thrown during import of data from XML file in database.
 */
public class XMLImportException extends Exception {

	private static final long serialVersionUID = -1747040844641874359L;

	private int myLine;
	private int myColumn;
	private String myMessage;

	public XMLImportException(int line, int column, String message) {
		super("In line " + line + " column " + column + ": " + message);

		myLine = line;
		myColumn = column;
		myMessage = message;
	}

	public String getMessageText() {
		return myMessage;
	}

	public int getLine() {
		return myLine;
	}

	public int getColumn() {
		return myColumn;
	}

}
