
package info.freelibrary.sodbox;

/**
 * Interface of file. Programmer can provide its own implementation of this interface, adding such features as support
 * of flash cards, encrypted files.... Implementation of this interface should throw StorageError exception in case of
 * failure.
 */
public interface IFile {

    /**
     * Write data to the file.
     * 
     * @param aPosition offset in the file
     * @param aBuffer array with data to be writer (size is always equal to database page size)
     */
    void write(long aPosition, byte[] aBuffer);

    /**
     * Read data from the file.
     * 
     * @param aPosition offset in the file
     * @param aBuffer array to receive read data (size is always equal to database page size)
     * @return number of bytes actually read
     */
    int read(long aPosition, byte[] aBuffer);

    /**
     * Flush all fields changes to the disk.
     */
    void sync();

    /**
     * Try lock file.
     * 
     * @param aShared if lock is shared
     * @return <code>true</code> if file was successfully locked or locking in not implemented, <code>false</code> if
     *         file is locked by some other application
     */
    boolean tryLock(boolean aShared);

    /**
     * Lock file.
     * 
     * @param aShared if lock is shared
     */
    void lock(boolean aShared);

    /**
     * Unlock file.
     */
    void unlock();

    /**
     * Close file.
     */
    void close();

    /**
     * Length of the file.
     */
    long length();

}
