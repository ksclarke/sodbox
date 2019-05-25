
package info.freelibrary.sodbox;

/**
 * Exception raised by <code>Assert</code> class when assertion was failed.
 */
public class AssertionFailed extends Error {

    private static final long serialVersionUID = 1826376129352570814L;

    /**
     * Creates a failed assertion error.
     */
    public AssertionFailed() {
        super("Assertion failed");
    }

    /**
     * Creates a failed assertion error with a detail message.
     *
     * @param aDescription
     */
    public AssertionFailed(final String aDescription) {
        super("Assertion '" + aDescription + "' failed");
    }
}
