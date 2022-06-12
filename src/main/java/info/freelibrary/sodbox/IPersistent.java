
package info.freelibrary.sodbox;

/**
 * Interface of all persistent capable objects.
 */
public interface IPersistent extends ILoadable, IStoreable, java.io.Externalizable {

    /**
     * Load object from the database (if needed).
     */
    void load();

    /**
     * Check if object is stub and has to be loaded from the database.
     *
     * @return <code>true</code> if object has to be loaded from the database
     */
    boolean isRaw();

    /**
     * Check if object was modified within current transaction.
     *
     * @return <code>true</code> if object is persistent and was modified within current transaction
     */
    boolean isModified();

    /**
     * Check if object is deleted by Java GC from process memory.
     *
     * @return <code>true</code> if object is deleted by GC
     */
    boolean isDeleted();

    /**
     * Check if object is persistent.
     *
     * @return <code>true</code> if object has assigned OID
     */
    boolean isPersistent();

    /**
     * Explicitly make object persistent. Usually objects are made persistent implicitly using "persistence through
     * reachability approach", but this method allows to do it explicitly.
     *
     * @param aStorage storage in which object should be stored
     */
    void makePersistent(Storage aStorage);

    /**
     * Save object in the database.
     */
    void store();

    /**
     * Mark object as modified. Object will be saved to the database during transaction commit.
     */
    void modify();

    /**
     * Load object from the database (if needed) and mark it as modified.
     */
    void loadAndModify();

    /**
     * Get object identifier (OID).
     *
     * @return OID (0 if object is not persistent yet)
     */
    int getOid();

    /**
     * Deallocate persistent object from the database.
     */
    void deallocate();

    /**
     * Specified whether object should be automatically loaded when it is referenced by other loaded persistent
     * object. Default implementation of this method returns <code>true</code> making all cluster of referenced
     * objects loaded together. To avoid main memory overflow you should stop recursive loading of all objects from
     * the database to main memory by redefining this method in some classes and returning <code>false</code> in it.
     * In this case object has to be loaded explicitly using Persistent.load method.
     *
     * @return <code>true</code> if object is automatically loaded
     */
    boolean recursiveLoading();

    /**
     * Get storage in which this object is stored.
     *
     * @return storage containing this object (null if object is not persistent yet)
     */
    Storage getStorage();

    /**
     * Invalidate object. Invalidated object has to be explicitly reloaded usin3g load() method. Attempt to store
     * invalidated object will cause StoraegError exception.
     */
    void invalidate();

    /**
     * Assign OID to the object. This method is used by storage class and you should not invoke it directly.
     *
     * @param aStorage associated storage
     * @param aOID object identifier
     */
    void assignOid(Storage aStorage, int aOID, boolean aRaw);

    /**
     * Method used to remove association of object with storage.
     */
    void unassignOid();

}
