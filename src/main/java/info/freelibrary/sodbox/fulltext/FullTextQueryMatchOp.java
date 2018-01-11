
package info.freelibrary.sodbox.fulltext;

/**
 * Match node of full text query
 */
public class FullTextQueryMatchOp extends FullTextQuery {

    /**
     * Matched word (shown be lowercvases and in normal form, unless used in quotes)
     */
    public String word;

    /**
     * Position of word in the query (zero based)
     */
    public int pos;

    /**
     * Index of the word in query (set and used internally, should not be accessed by application)
     */
    public int wno;

    /**
     * Match node constructor
     * 
     * @param op operation code (should ne MATCH or STICT_MATCH)
     * @param word searched word
     * @param pos position of word in the query
     */
    public FullTextQueryMatchOp(final int op, final String word, final int pos) {
        super(op);
        this.word = word;
        this.pos = pos;
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
        return op == MATCH ? word : '"' + word + '"';
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor visitor) {
        visitor.visit(this);
    }
}
