
package info.freelibrary.sodbox;

/**
 * This interface extends normal Java streams with seek capability. This interface is implemented by Sodbox BLOB
 * streams.
 */
public interface RandomAccessStream {

    /**
     * Set position in the stream.
     *
     * @param aPosition new absolute position in the stream
     * @return The actual position in the stream; it can be less than specified if end of stream is reached
     */
    long setPosition(long aPosition);

    /**
     * Get current position in the stream.
     *
     * @return Current position in the stream
     */
    long getPosition();

    /**
     * Get size of the stream.
     *
     * @return Number of bytes available in the stream
     */
    long size();

}
