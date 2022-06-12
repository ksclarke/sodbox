
package info.freelibrary.sodbox;

import java.lang.reflect.Field;

/**
 * Interface of indexed field. Index is used to provide fast access to the object by the value of indexed field.
 * Objects in the index are stored ordered by the value of indexed field. It is possible to select object using exact
 * value of the key or select set of objects which key belongs to the specified interval (each boundary can be
 * specified or unspecified and can be inclusive or exclusive) Key should be of scalar, String, java.util.Date or
 * persistent object type.
 */
public interface FieldIndex<T> extends GenericIndex<T> {

    /**
     * Put new object in the index.
     *
     * @param aObject object to be inserted in index. Object should contain indexed field. Object can be not yet
     *        persistent, in this case its forced to become persistent by assigning OID to it.
     * @return <code>true</code> if object is successfully inserted in the index, <code>false</code> if index was
     *         declared as unique and there is already object with such value of the key in the index.
     */
    boolean put(T aObject);

    /**
     * Associate new object with the key specified by object field value. If there is already object with such key in
     * the index, then it will be removed from the index and new value associated with this key.
     *
     * @param aObject object to be inserted in index. Object should contain indexed field. Object can be not yet
     *        persistent, in this case its forced to become persistent by assigning OID to it.
     * @return object previously associated with this key, <code>null</code> if there was no such object
     */
    T set(T aObject);

    /**
     * Assign to the integer indexed field unique auto-incremented value and insert object in the index.
     *
     * @param aObject object to be inserted in index. Object should contain indexed field of integer (<code>int</code>
     *        or <code>long</code>) type. This field is assigned unique value (which will not be reused while this
     *        index exists) and object is marked as modified. Object can be not yet persistent, in this case its
     *        forced to become persistent by assigning OID to it.
     * @throws StorageError INCOMPATIBLE_KEY_TYPE when indexed field has type other than <code>int</code> or
     *         <code>long</code>
     */
    void append(T aObject);

    /**
     * Remove object with specified key from the unique index.
     *
     * @param aKey value of removed key
     * @return removed object
     * @throws StorageError KEY_NOT_FOUND exception if there is no such key in the index, or StorageError
     *         KEY_NOT_UNIQUE if index is not unique.
     */
    T remove(Key aKey);

    /**
     * Remove object with specified key from the unique index.
     *
     * @param aKey value of removed key
     * @return removed object
     * @throws StorageError KEY_NOT_FOUND exception if there is no such key in the index, or
     *            StorageError KEY_NOT_UNIQUE if index is not unique.
     */
    T removeKey(Object aKey);

    /**
     * Check if index contains specified object instance.
     *
     * @param aObject object to be searched in the index. Object should contain indexed field.
     * @return <code>true</code> if object is present in the index, <code>false</code> otherwise
     */
    boolean containsObject(T aObject);

    /**
     * Get class object objects which can be inserted in this index.
     *
     * @return class specified in Storage.createFielIndex method
     */
    Class getIndexedClass();

    /**
     * Get fields used as a key.
     *
     * @return array of index key fields
     */
    Field[] getKeyFields();

    /**
     * Check if field index is case insensitive.
     *
     * @return true if index ignore case of string keys
     */
    boolean isCaseInsensitive();

}
