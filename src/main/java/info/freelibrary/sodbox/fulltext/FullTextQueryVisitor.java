
package info.freelibrary.sodbox.fulltext;

/**
 * Base class for full text query visitor
 */
public class FullTextQueryVisitor {

    public void visit(final FullTextQuery q) {
    }

    public void visit(final FullTextQueryBinaryOp q) {
        visit((FullTextQuery) q);
    }

    public void visit(final FullTextQueryMatchOp q) {
        visit((FullTextQuery) q);
    }

    public void visit(final FullTextQueryUnaryOp q) {
        visit((FullTextQuery) q);
    }
}
