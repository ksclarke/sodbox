
package info.freelibrary.sodbox;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream for SelfSerializable and CustumSerializer.
 */
public abstract class SodboxInputStream extends DataInputStream {

    public SodboxInputStream(final InputStream aStream) {
        super(aStream);
    }

    /**
     * Read reference to the object or content of the embedded object.
     *
     * @return unswizzled object
     */
    public abstract Object readObject() throws IOException;

    /**
     * Read string according to the Sodbox string encoding.
     *
     * @return extracted string or null
     */
    public abstract String readString() throws IOException;

}
