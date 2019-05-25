
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.PersistentComparator;

public class DefaultPersistentComparator<T extends Comparable> extends PersistentComparator<T> {

    @SuppressWarnings("unchecked")
    @Override
    public int compareMembers(final T aFirstMember, final T aSecondMember) {
        return aFirstMember.compareTo(aSecondMember);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareMemberWithKey(final T aMember, final Object aKey) {
        return aMember.compareTo(aKey);
    }
}
