
package info.freelibrary.sodbox;

/**
 * Exception thrown during import of data from XML file in database.
 */
public class XMLImportException extends Exception {

    private static final long serialVersionUID = -1747040844641874359L;

    private final int myLine;

    private final int myColumn;

    private final String myMessage;

    public XMLImportException(final int line, final int column, final String message) {
        super("In line " + line + " column " + column + ": " + message);

        myLine = line;
        myColumn = column;
        myMessage = message;
    }

    public int getColumn() {
        return myColumn;
    }

    public int getLine() {
        return myLine;
    }

    public String getMessageText() {
        return myMessage;
    }

}
