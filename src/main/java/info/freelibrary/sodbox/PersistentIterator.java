
package info.freelibrary.sodbox;

/**
 * Interface implemented by all Sodbox iterators allowing to get OID of the current object.
 */
public interface PersistentIterator {

    /**
     * Get OID of the next object.
     *
     * @return OID of the the next element in the iteration or 0 if iteration has no more objects.
     */
    public int nextOid();

}
