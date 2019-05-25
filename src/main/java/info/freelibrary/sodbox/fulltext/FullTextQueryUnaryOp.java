
package info.freelibrary.sodbox.fulltext;

/**
 * Unary node of full text query
 */
public class FullTextQueryUnaryOp extends FullTextQuery {

    public FullTextQuery myQuery;

    /**
     * Unary node constructor
     *
     * @param aOp operation code
     * @param aOperand operand
     */
    public FullTextQueryUnaryOp(final int aOp, final FullTextQuery aOperand) {
        super(aOp);
        myQuery = aOperand;
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor aVisitor) {
        aVisitor.visit(this);
        myQuery.visit(aVisitor);
    }

    /**
     * This method checks that query can be executed by intersection of keyword occurrences lists
     *
     * @return true if query can be executed by FullTextIndex, false otherwise
     */
    @Override
    public boolean isConstrained() {
        return myOp == NOT ? false : myQuery.isConstrained();
    }

    @Override
    public String toString() {
        return OPERATOR_NAME[myOp] + '(' + myQuery.toString() + ')';
    }

}
