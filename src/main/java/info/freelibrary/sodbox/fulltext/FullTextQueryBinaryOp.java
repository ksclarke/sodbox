
package info.freelibrary.sodbox.fulltext;

/**
 * Binary node of full text query
 */
public class FullTextQueryBinaryOp extends FullTextQuery {

    public FullTextQuery left;

    public FullTextQuery right;

    /**
     * Binary node constructor
     * 
     * @param op operation code
     * @param left left operand
     * @param right right operand
     */
    public FullTextQueryBinaryOp(final int op, final FullTextQuery left, final FullTextQuery right) {
        super(op);
        this.left = left;
        this.right = right;
    }

    /**
     * This method checks that query can be executed by interection of keyword occurrences lists
     * 
     * @return true if quuery can be executed by FullTextIndex, false otherwise
     */
    @Override
    public boolean isConstrained() {
        return op == OR ? left.isConstrained() && right.isConstrained() : left.isConstrained() || right
                .isConstrained();
    }

    @Override
    public String toString() {
        return op == OR ? '(' + left.toString() + ") OR (" + right.toString() + ')' : left.toString() + ' ' +
                operatorName[op] + ' ' + right.toString();
    }

    /**
     * Query node visitor.
     */
    @Override
    public void visit(final FullTextQueryVisitor visitor) {
        visitor.visit(this);
        left.visit(visitor);
        right.visit(visitor);
    }
}
