
package info.freelibrary.sodbox;

import info.freelibrary.util.I18nException;

/**
 * Exception thrown during import of data from XML file in database.
 */
public class XMLImportException extends I18nException {

    private static final long serialVersionUID = -1747040844641874359L;

    private final int myLine;

    private final int myColumn;

    /**
     * Creates an XML import exception.
     *
     * @param aLine A line
     * @param aColumn A column
     */
    public XMLImportException(final int aLine, final int aColumn, final String aMessage) {
        super(Constants.MESSAGES, aMessage, aLine, aColumn);

        myLine = aLine;
        myColumn = aColumn;
    }

    /**
     * Creates an XML import exception.
     *
     * @param aLine A line
     * @param aColumn A column
     */
    public XMLImportException(final int aLine, final int aColumn, final String aMessage, final String aDetail) {
        super(Constants.MESSAGES, aMessage, aDetail, aLine, aColumn);

        myLine = aLine;
        myColumn = aColumn;
    }

    /**
     * Creates an XML import exception.
     *
     * @param aLine A line
     * @param aColumn A column
     */
    public XMLImportException(final Exception aCause, final int aLine, final int aColumn, final String aMessage,
            final String aDetail) {
        super(aCause, Constants.MESSAGES, aMessage, aDetail, aLine, aColumn);

        myLine = aLine;
        myColumn = aColumn;
    }

    /**
     * Gets the line number from the exception.
     *
     * @return The exception's line number
     */
    public int getLine() {
        return myLine;
    }

    /**
     * Gets the column number from the exception.
     *
     * @return The exception's column number.
     */
    public int getColumn() {
        return myColumn;
    }

}
