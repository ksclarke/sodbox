
package info.freelibrary.sodbox.impl;

import java.io.IOException;

import info.freelibrary.sodbox.IFile;

/**
 * File performing asynchronous replication of changed pages to specified slave nodes.
 */
public class AsyncReplicationMasterFile extends ReplicationMasterFile {

    private final int myAsyncBufSize;

    private int myBuffered;

    private boolean isClosed;

    private Object myGo;

    private Object myAsync;

    private Parcel myHead;

    private Parcel myTail;

    private Thread myThread;

    /**
     * Constructor of replication master file
     *
     * @param aStorage replication storage
     * @param aFile local file used to store data locally
     * @param aAsyncBufSize size of asynchronous buffer
     * @param aPageTsFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     */
    public AsyncReplicationMasterFile(final ReplicationMasterStorageImpl aStorage, final IFile aFile,
            final int aAsyncBufSize, final String aPageTsFile) {
        super(aStorage, aFile, aPageTsFile);

        this.myAsyncBufSize = aAsyncBufSize;
        start();
    }

    /**
     * Constructor of replication master file
     *
     * @param aFile local file used to store data locally
     * @param aHosts slave node hosts to which replication will be performed
     * @param aAsyncBufSize size of asynchronous buffer
     * @param aAck whether master should wait acknowledgment from slave node during transaction commit
     */
    public AsyncReplicationMasterFile(final IFile aFile, final String[] aHosts, final int aAsyncBufSize,
            final boolean aAck) {
        this(aFile, aHosts, aAsyncBufSize, aAck, null);
    }

    /**
     * Constructor of replication master file
     *
     * @param aFile local file used to store data locally
     * @param aHosts slave node hosts to which replication will be performed
     * @param aAsyncBufSize size of asynchronous buffer
     * @param aAck whether master should wait acknowledgment from slave node during trasanction commit
     * @param aPageTsFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     */
    public AsyncReplicationMasterFile(final IFile aFile, final String[] aHosts, final int aAsyncBufSize,
            final boolean aAck, final String aPageTsFile) {
        super(aFile, aHosts, aAck, aPageTsFile);

        this.myAsyncBufSize = aAsyncBufSize;

        start();
    }

    private void start() {
        myGo = new Object();
        myAsync = new Object();
        myThread = new WriteThread();
        myThread.start();
    }

    @Override
    public void write(final long aPosition, final byte[] aBytes) {
        myFile.write(aPosition, aBytes);

        synchronized (myMutex) {
            if (myPageTs != null) {
                final int pageNo = (int) (aPosition >> Page.PAGE_SIZE_LOG);
                if (pageNo >= myPageTs.length) {
                    final int newLength = pageNo >= myPageTs.length * 2 ? pageNo + 1 : myPageTs.length *
                            2;

                    final int[] newPageTs = new int[newLength];

                    System.arraycopy(myPageTs, 0, newPageTs, 0, myPageTs.length);

                    myPageTs = newPageTs;

                    final int[] newDirtyPageTimestampMap = new int[(newLength * 4 + Page.PAGE_SIZE -
                            1 >> Page.PAGE_SIZE_LOG) + 31 >> 5];

                    System.arraycopy(myDirtyPageTsMap, 0, newDirtyPageTimestampMap, 0,
                            myDirtyPageTsMap.length);

                    myDirtyPageTsMap = newDirtyPageTimestampMap;
                }

                myPageTs[pageNo] = ++myTimestamp;
                myDirtyPageTsMap[pageNo >> Page.PAGE_SIZE_LOG - 2 + 5] |= 1 << (pageNo >> Page.PAGE_SIZE_LOG - 2 &
                        31);
            }
        }
        for (int i = 0; i < myOutputStream.length; i++) {
            if (myOutputStream[i] != null) {
                final byte[] data = new byte[8 + aBytes.length];

                Bytes.pack8(data, 0, aPosition);
                System.arraycopy(aBytes, 0, data, 8, aBytes.length);

                final Parcel p = new Parcel();

                p.myData = data;
                p.myPosition = aPosition;
                p.myHost = i;

                try {
                    synchronized (myAsync) {
                        myBuffered += data.length;

                        while (myBuffered > myAsyncBufSize && myBuffered != data.length) {
                            myAsync.wait();
                        }
                    }
                } catch (final InterruptedException details) {
                    // FIXME
                }

                synchronized (myGo) {
                    if (myHead == null) {
                        myHead = myTail = p;
                    } else {
                        myTail = myTail.myNext = p;
                    }

                    myGo.notify();
                }
            }
        }
    }

    /**
     * Asynchronous write.
     */
    public void asyncWrite() {
        try {
            while (true) {
                final Parcel parcel;

                synchronized (myGo) {
                    while (myHead == null) {
                        if (isClosed) {
                            return;
                        }

                        myGo.wait();
                    }

                    parcel = myHead;
                    myHead = parcel.myNext;
                }

                synchronized (myAsync) {
                    if (myBuffered > myAsyncBufSize) {
                        myAsync.notifyAll();
                    }

                    myBuffered -= parcel.myData.length;
                }

                final int i = parcel.myHost;

                while (myOutputStream[i] != null) {
                    try {
                        myOutputStream[i].write(parcel.myData);

                        if (!myAck || parcel.myPosition != 0 || myInputStream[i].read(myRcBuf) == 1) {
                            break;
                        }
                    } catch (final IOException details) {
                        // FIXME
                    }

                    myOutputStream[i] = null;
                    mySockets[i] = null;
                    myNumOfHosts -= 1;
                    if (handleError(myHosts[i])) {
                        connect(i);
                    } else {
                        break;
                    }
                }
            }
        } catch (final InterruptedException details) {
            // FIXME
        }
    }

    @Override
    public void close() {
        try {
            synchronized (myGo) {
                isClosed = true;
                myGo.notify();
            }
            myThread.join();
        } catch (final InterruptedException details) {
            // FIXME
        }

        super.close();
    }

    class WriteThread extends Thread {

        @Override
        public void run() {
            asyncWrite();
        }

    }

    static class Parcel {

        byte[] myData;

        long myPosition;

        int myHost;

        Parcel myNext;

    }

}
