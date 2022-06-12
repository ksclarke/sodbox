
package info.freelibrary.sodbox;

/**
 * Base class for persistent comparator used in SortedCollection class.
 */
public abstract class PersistentComparator<T> extends Persistent {

    /**
     * Compare two members of collection.
     *
     * @param a1stMember A first member
     * @param a2ndMember A second member
     * @return Negative number if the first member &lt; the second member, zero if the first member == the second
     *         member and positive number if the first member &gt; the second member
     */
    public abstract int compareMembers(T a1stMember, T a2ndMember);

    /**
     * Compare member with specified search key.
     *
     * @param aMember collection member
     * @param aKey search key
     * @return negative number if <code>aMember</code> &lt; key, zero if <code>aMember</code> == key and positive
     *         number if <code>aMember</code> &gt; key
     */
    public abstract int compareMemberWithKey(T aMember, Object aKey);

}
