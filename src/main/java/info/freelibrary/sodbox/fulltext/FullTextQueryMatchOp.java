
package info.freelibrary.sodbox.fulltext;

/**
 * Match node of full text query
 */
public class FullTextQueryMatchOp extends FullTextQuery {

    /**
     * Matched word (shown be lower-case and in normal form, unless used in quotes)
     */
    public String myWord;

    /**
     * Position of word in the query (zero based)
     */
    public int myPosition;

    /**
     * Index of the word in query (set and used internally, should not be accessed by application)
     */
    public int myWordInQueryIndex;

    /**
     * Match node constructor
     *
     * @param aOp operation code (should ne MATCH or STICT_MATCH)
     * @param aWord searched word
     * @param aPosition position of word in the query
     */
    public FullTextQueryMatchOp(final int aOp, final String aWord, final int aPosition) {
        super(aOp);

        myWord = aWord;
        myPosition = aPosition;
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor aVisitor) {
        aVisitor.visit(this);
    }

    /**
     * Match node provides query constraint
     */
    @Override
    public boolean isConstrained() {
        return true;
    }

    @Override
    public String toString() {
        return myOp == MATCH ? myWord : '"' + myWord + '"';
    }

}
