
package info.freelibrary.sodbox;

/**
 * Double linked list element.
 */
public class L2ListElem extends PersistentResource {

    protected L2ListElem myNext;

    protected L2ListElem myPrev;

    /**
     * Creates a L2 list element.
     */
    public L2ListElem() {
        myNext = myPrev = this;
    }

    /**
     * Get next list element. Been call for the last list element, this method will return first element of the list
     * or list header.
     */
    public L2ListElem getNext() {
        return myNext;
    }

    /**
     * Get previous list element. Been call for the first list element, this method will return last element of the
     * list or list header.
     */
    public L2ListElem getPrev() {
        return myPrev;
    }

    /**
     * Make list empty. This method should be applied to list header.
     */
    public void prune() {
        modify();
        myNext = myPrev = this;
    }

    /**
     * Link specified element in the list after this element.
     *
     * @param aElem element to be linked in the list after this element.
     */
    public void linkAfter(final L2ListElem aElem) {
        modify();
        myNext.modify();
        aElem.modify();
        aElem.myNext = myNext;
        aElem.myPrev = this;
        myNext.myPrev = aElem;
        myNext = aElem;
    }

    /**
     * Link specified element in the list before this element.
     *
     * @param aElem element to be linked in the list before this element
     */
    public void linkBefore(final L2ListElem aElem) {
        modify();
        myPrev.modify();
        aElem.modify();
        aElem.myNext = this;
        aElem.myPrev = myPrev;
        myPrev.myNext = aElem;
        myPrev = aElem;
    }

    /**
     * Remove element from the list.
     */
    public void unlink() {
        myNext.modify();
        myPrev.modify();
        myNext.myPrev = myPrev;
        myPrev.myNext = myNext;
    }

}
