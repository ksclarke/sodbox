package info.freelibrary.sodbox;

import java.io.OutputStream;
import java.io.IOException;
import java.io.DataOutputStream;

/**
 * Output stream for SelfSerializable and CustumSerializer.
 */
public abstract class SodboxOutputStream extends DataOutputStream {

	public SodboxOutputStream(OutputStream aStream) {
		super(aStream);
	}

	/**
	 * Write reference to the object or content of embedded object.
	 * 
	 * @param aObject swizzled object
	 */
	public abstract void writeObject(Object aObject) throws IOException;

	/**
	 * Write string according to the Sodbox string encoding.
	 * 
	 * @param aString string to be packed (may be be null)
	 */
	public abstract void writeString(String aString) throws IOException;

}
