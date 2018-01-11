
package info.freelibrary.sodbox.fulltext;

/**
 * Unary node of full text query
 */
public class FullTextQueryUnaryOp extends FullTextQuery {

    public FullTextQuery opd;

    /**
     * Unary node constructor
     * 
     * @param op operation code
     * @param opd operand
     */
    public FullTextQueryUnaryOp(final int op, final FullTextQuery opd) {
        super(op);
        this.opd = opd;
    }

    /**
     * This method checks that query can be executed by interection of keyword occurrences lists
     * 
     * @return true if quuery can be executed by FullTextIndex, false otherwise
     */
    @Override
    public boolean isConstrained() {
        return op == NOT ? false : opd.isConstrained();
    }

    @Override
    public String toString() {
        return operatorName[op] + '(' + opd.toString() + ')';
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor visitor) {
        visitor.visit(this);
        opd.visit(visitor);
    }
}
