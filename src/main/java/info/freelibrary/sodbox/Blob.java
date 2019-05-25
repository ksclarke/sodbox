
package info.freelibrary.sodbox;

/**
 * Interface to store/fetch large binary objects.
 */
public interface Blob extends IPersistent, IResource {

    int ENABLE_SEGMENT_CACHING = 1;

    int DOUBLE_SEGMENT_SIZE = 2;

    int TRUNCATE_LAST_SEGMENT = 4;

    int APPEND = 8;

    /**
     * Get input stream. InputStream.availabe method can be used to get BLOB size.
     *
     * @return input stream with BLOB data
     */
    RandomAccessInputStream getInputStream();

    /**
     * Get output stream to append data to the BLOB.
     *
     * @return output stream
     */
    RandomAccessOutputStream getOutputStream();

    /**
     * Get output stream to append data to the BLOB.
     *
     * @param aMultiSession whether BLOB allows further appends of data or closing this output stream means that BLOB
     *        will not be changed any more
     * @return output stream
     */
    RandomAccessOutputStream getOutputStream(boolean aMultiSession);

    /**
     * Get output stream with specified current position in BLOB.
     *
     * @param aPosition current position in BLOB, if less than zero, than data will be appended to the BLOB
     * @param aMultiSession whether BLOB allows further appends of data or closing this output stream means that BLOB
     *        will not be changed any more
     * @return output stream
     */
    RandomAccessOutputStream getOutputStream(long aPosition, boolean aMultiSession);

    /**
     * Get input stream. InputStream.availabe method can be used to get BLOB size.
     *
     * @param aFlags bit mask of BLOB flags: ENABLE_SEGMENT_CACHING
     * @return input stream with BLOB data
     */
    RandomAccessInputStream getInputStream(int aFlags);

    /**
     * Get output stream to append data to the BLOB.
     *
     * @param aFlags bit mask of BLOB flags: ENABLE_SEGMENT_CACHING, DOUBLE_SEGMENT_SIZE, TRUNCATE_LAST_SEGMENT,
     *        APPEND
     * @return output stream
     */
    RandomAccessOutputStream getOutputStream(int aFlags);

};
