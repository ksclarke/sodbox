
package info.freelibrary.sodbox;

/**
 * Interface of bit index. Bit index allows to efficiently search object with specified set of properties. Each object
 * has associated mask of 32 bites. Meaning of bits is application dependent. Usually each bit stands for some binary or
 * boolean property, for example "sex", but it is possible to use group of bits to represent enumerations with more
 * possible values.
 */
public interface BitIndex<T> extends IPersistent, IResource {

    /**
     * Get properties of specified object
     *
     * @param aObject object which properties are requested
     * @return bit mask associated with this objects
     * @throws StorageError StorageError.KEY_NOT_FOUND exception if there is no object in the index
     */
    int getMask(T aObject);

    /**
     * Get iterator for selecting objects with specified properties. To select all record this method should be invoked
     * with (0, 0) parameters This iterator doesn't support remove() method. It is not possible to modify bit index
     * during iteration.
     *
     * @param aSet bitmask specifying bits which should be set (1)
     * @param aClear bitmask specifying bits which should be cleared (0)
     * @return Selection iterator
     */
    IterableIterator<T> iterator(int aSet, int aClear);

    /**
     * Put new object in the index. If such object already exists in index, then its mask will be rewritten
     *
     * @param aObject object placed in the index. Object can be not yet persistent, in this case its forced to become
     *        persistent by assigning OID to it.
     * @param aMask bit mask associated with this objects
     */
    void put(T aObject, int aMask);

}
