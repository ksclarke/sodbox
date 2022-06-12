
package info.freelibrary.sodbox.fulltext;

/**
 * Base class for full text query visitor
 */
public class FullTextQueryVisitor {

    /**
     * Visit a query.
     *
     * @param aQuery A query to visit
     */
    public void visit(final FullTextQuery aQuery) {
    }

    /**
     * Visit a query.
     *
     * @param aQuery A query to visit
     */
    public void visit(final FullTextQueryBinaryOp aQuery) {
        visit((FullTextQuery) aQuery);
    }

    /**
     * Visit a query.
     *
     * @param aQuery A query to visit
     */
    public void visit(final FullTextQueryUnaryOp aQuery) {
        visit((FullTextQuery) aQuery);
    }

    /**
     * Visit a query.
     *
     * @param aQuery A query to visit
     */
    public void visit(final FullTextQueryMatchOp aQuery) {
        visit((FullTextQuery) aQuery);
    }

}
