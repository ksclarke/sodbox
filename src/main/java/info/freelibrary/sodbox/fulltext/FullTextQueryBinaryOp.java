
package info.freelibrary.sodbox.fulltext;

/**
 * Binary node of full text query
 */
public class FullTextQueryBinaryOp extends FullTextQuery {

    public FullTextQuery myLeft;

    public FullTextQuery myRight;

    /**
     * Binary node constructor
     *
     * @param aOp operation code
     * @param aLeft left operand
     * @param aRight right operand
     */
    public FullTextQueryBinaryOp(final int aOp, final FullTextQuery aLeft, final FullTextQuery aRight) {
        super(aOp);

        myLeft = aLeft;
        myRight = aRight;
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor aVisitor) {
        aVisitor.visit(this);

        myLeft.visit(aVisitor);
        myRight.visit(aVisitor);
    }

    /**
     * This method checks that query can be executed by intersection of keyword occurrences lists
     *
     * @return true if query can be executed by FullTextIndex, false otherwise
     */
    @Override
    public boolean isConstrained() {
        return myOp == OR ? myLeft.isConstrained() && myRight.isConstrained() : myLeft.isConstrained() || myRight
                .isConstrained();
    }

    @Override
    public String toString() {
        return myOp == OR ? '(' + myLeft.toString() + ") OR (" + myRight.toString() + ')' : myLeft.toString() + ' ' +
                OPERATOR_NAME[myOp] + ' ' + myRight.toString();
    }

}
