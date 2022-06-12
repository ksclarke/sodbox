
package info.freelibrary.sodbox;

/**
 * Listener of database events. Programmer should derive his own subclass and register it using Storage.setListener
 * method.
 */
public abstract class StorageListener {

    /**
     * This method is called during database open when database was not close normally and has to be recovered.
     */
    public void databaseCorrupted() {
    }

    /**
     * This method is called after completion of recovery.
     */
    public void recoveryCompleted() {
    }

    /**
     * Method invoked by Sodbox after object is loaded from the database.
     *
     * @param aObject loaded object
     */
    public void onObjectLoad(final Object aObject) {
    }

    /**
     * Method invoked by Sodbox before object is written to the database.
     *
     * @param aObject stored object
     */
    public void onObjectStore(final Object aObject) {
    }

    /**
     * Method invoked by Sodbox before object is deallocated.
     *
     * @param aObject deallocated object
     */
    public void onObjectDelete(final Object aObject) {
    }

    /**
     * Method invoked by Sodbox after object is assigned OID (becomes persistent).
     *
     * @param aObject object which is made persistent
     */
    public void onObjectAssignOid(final Object aObject) {
    }

    /**
     * Method invoked by Sodbox when slave node receive updates from master.
     */
    public void onMasterDatabaseUpdate() {
    }

    /**
     * Method invoked by Sodbox when transaction is committed.
     */
    public void onTransactionCommit() {
    }

    /**
     * Method invoked by Sodbox when transaction is aborted.
     */
    public void onTransactionRollback() {
    }

    /**
     * This method is called when garbage collection is started (either explicitly by invocation of Storage.gc()
     * method, either implicitly after allocation of some amount of memory)).
     */
    public void gcStarted() {
    }

    /**
     * This method is called when unreferenced object is deallocated from database during garbage collection. It is
     * possible to get instance of the object using <code>Storage.getObjectByOid()</code> method.
     *
     * @param aClass class of deallocated object
     * @param aOID object identifier of deallocated object
     */
    public void deallocateObject(final Class aClass, final int aOID) {
    }

    /**
     * This method is called when garbage collection is completed.
     *
     * @param aNumOfDeallocatedObjects number of deallocated objects
     */
    public void gcCompleted(final int aNumOfDeallocatedObjects) {
    }

    /**
     * Handle replication error.
     *
     * @param aHost address of host replication to which is failed (null if error happens at slave node)
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be repeated,
     *         <code>false</code> if no more attempts to communicate with this host should be performed
     */
    public boolean replicationError(final String aHost) {
        return false;
    }

    /**
     * This method is called by XML exporter if database corruption or some other reasons makes export of the object
     * not possible.
     *
     * @param aOID object identified
     * @param aX caught exception
     */
    public void objectNotExported(final int aOID, final StorageError aX) {
    }

}
