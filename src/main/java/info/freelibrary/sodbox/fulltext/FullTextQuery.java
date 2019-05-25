
package info.freelibrary.sodbox.fulltext;

/**
 * Base class for full test search query nodes. Query can be parsed by FullTextSearchHelper class or explicitly
 * created by user.
 */
public class FullTextQuery {

    public static final int MATCH = 0;

    public static final int STRICT_MATCH = 1;

    public static final int AND = 2;

    public static final int NEAR = 3;

    public static final int OR = 4;

    public static final int NOT = 5;

    public static final String[] OPERATOR_NAME = { "MATCH", "STRICT_MATCH", "AND", "NEAR", "OR", "NOT" };

    public int myOp;

    /**
     * Query node constructor
     *
     * @param aOp operation code
     */
    public FullTextQuery(final int aOp) {
        myOp = aOp;
    }

    /**
     * Query node visitor. It provides convenient way of iterating through query nodes.
     */
    public void visit(final FullTextQueryVisitor aVisitor) {
        aVisitor.visit(this);
    }

    /**
     * This method checks that query can be executed by intersection of keyword occurrences lists
     *
     * @return true if query can be executed by FullTextIndex; else, false
     */
    public boolean isConstrained() {
        return false;
    }

}
