
package info.freelibrary.sodbox;

/**
 * Base class for persistent comparator used in SortedCollection class.
 */
public abstract class PersistentComparator<T> extends Persistent {

    /**
     * Compare two members of collection.
     *
     * @param aMember1 A first member
     * @param aMember2 A second member
     * @return Negative number if aMember1 &lt; aMember2, zero if aMember1 == aMember2 and positive number if aMember1
     *         &gt; aMember2
     */
    public abstract int compareMembers(T aMember1, T aMember2);

    /**
     * Compare member with specified search key.
     *
     * @param aMember collection member
     * @param key search key
     * @return negative number if <code>aMember</code> &lt; key, zero if <code>aMember</code> == key and positive
     *         number if <code>aMember</code> &gt; key
     */
    public abstract int compareMemberWithKey(T aMember, Object key);

}
