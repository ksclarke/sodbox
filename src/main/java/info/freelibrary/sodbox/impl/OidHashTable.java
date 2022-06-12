
package info.freelibrary.sodbox.impl;

public interface OidHashTable {

    /**
     * Remove an object by OID.
     *
     * @param aOID An OID of the object to remove
     * @return True if removed; else, false
     */
    boolean remove(int aOID);

    /**
     * Put an object and associate the supplied OID with it.
     *
     * @param aOID An object ID
     * @param aObject An object
     */
    void put(int aOID, Object aObject);

    /**
     * Get an object associated with the supplied OID.
     *
     * @param aOID A object ID
     * @return An object
     */
    Object get(int aOID);

    /**
     * Flush the OID HashTable.
     */
    void flush();

    /**
     * Invalidate the OID HashTable.
     */
    void invalidate();

    /**
     * Reload the OID HashTable.
     */
    void reload();

    /**
     * Clear the OID HashTable.
     */
    void clear();

    /**
     * Get the size of the OID HashTable.
     *
     * @return The size of the OID HashTable
     */
    int size();

    /**
     * Set the supplied object as dirty.
     *
     * @param aObject An object to set
     */
    void setDirty(Object aObject);

    /**
     * Clear the dirty setting on the supplied object.
     *
     * @param aObject An object to clear dirty setting
     */
    void clearDirty(Object aObject);

}
