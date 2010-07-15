package info.freelibrary.sodbox;

/**
 * Exception raised by <code>Assert</code> class when assertion was failed.
 */
public class AssertionFailed extends Error {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1826376129352570814L;

	public AssertionFailed() { 
        super("Assertion failed");
    }

    public AssertionFailed(String description) { 
        super("Assertion '" + description + "' failed");
    }
}
