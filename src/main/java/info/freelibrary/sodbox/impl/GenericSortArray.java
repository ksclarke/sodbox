
package info.freelibrary.sodbox.impl;

/**
 * A generic sort array.
 */
public interface GenericSortArray {

    /**
     * Gets size.
     */
    int size();

    /**
     * Compares two array members
     *
     * @param aFirstMember
     * @param aSecondMember
     * @return
     */
    int compare(int aFirstMember, int aSecondMember);

    /**
     * Swaps two array members.
     *
     * @param aFirstMember
     * @param aSecondMember
     */
    void swap(int aFirstMember, int aSecondMember);

}
