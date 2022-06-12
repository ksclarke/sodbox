
package info.freelibrary.sodbox;

/**
 * Class for checking program invariants. Analog of C <code>assert()</code> macro. The Java compiler doesn't provide
 * information about compiled file and line number, so the place of assertion failure can be located only by analyzing
 * the stack trace of the thrown AssertionFailed exception.
 *
 * @see info.freelibrary.sodbox.AssertionFailed
 */
public final class Assert {

    private Assert() {
        super();
    }

    /**
     * Check specified condition and raise <code>AssertionFailed</code> exception if it is not true.
     *
     * @param aCondition result of checked condition
     */
    public static void that(final boolean aCondition) {
        if (!aCondition) {
            throw new AssertionFailed();
        }
    }

    /**
     * Check specified condition and raise <code>AssertionFailed</code> exception if it is not true.
     *
     * @param aDescription string describing checked condition
     * @param aCondition result of checked condition
     */
    public static void that(final String aDescription, final boolean aCondition) {
        if (!aCondition) {
            throw new AssertionFailed(aDescription);
        }
    }

    /**
     * Throw assertion failed exception.
     */
    public static void failed() {
        throw new AssertionFailed();
    }

    /**
     * Throw assertion failed exception with given description.
     */
    public static void failed(final String aDescription) {
        throw new AssertionFailed(aDescription);
    }

}
