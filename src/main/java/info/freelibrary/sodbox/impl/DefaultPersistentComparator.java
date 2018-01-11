
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.PersistentComparator;

public class DefaultPersistentComparator<T extends Comparable> extends PersistentComparator<T> {

    @Override
    public int compareMembers(final T m1, final T m2) {
        return m1.compareTo(m2);
    }

    @Override
    public int compareMemberWithKey(final T mbr, final Object key) {
        return mbr.compareTo(key);
    }
}