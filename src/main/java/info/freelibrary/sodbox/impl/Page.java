
package info.freelibrary.sodbox.impl;

public class Page extends LRU implements Comparable {

    public static final int PAGE_SIZE_LOG = 12;

    public static final int PAGE_SIZE = 1 << PAGE_SIZE_LOG;

    static final int PS_DIRTY = 0x01;// page has been modified

    static final int PS_RAW = 0x02;// page is loaded from the disk

    static final int PS_WAIT = 0x04;// other thread(s) wait load operation completion

    Page myCollisionChain;

    int myAccessCount;

    int myWriteQueueIndex;

    int myState;

    long myOffset;

    byte myData[];

    @Override
    public int compareTo(final Object aObject) {
        final long po = ((Page) aObject).myOffset;

        return myOffset < po ? -1 : myOffset == po ? 0 : 1;
    }

}
