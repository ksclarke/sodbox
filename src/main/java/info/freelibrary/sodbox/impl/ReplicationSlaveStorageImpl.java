
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.IResource;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.ReplicationSlaveStorage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

public abstract class ReplicationSlaveStorageImpl extends StorageImpl implements ReplicationSlaveStorage, Runnable {

    public static int LINGER_TIME = 10; // linger parameter for the socket

    protected static final int DB_HDR_CURR_INDEX_OFFSET = 0;

    protected static final int DB_HDR_DIRTY_OFFSET = 1;

    protected static final int DB_HDR_INITIALIZED_OFFSET = 2;

    protected static final int PAGE_DATA_OFFSET = 8;

    static final int REPL_CLOSE = -1;

    static final int REPL_SYNC = -2;

    static final int INIT_PAGE_TIMESTAMPS_LENGTH = 64 * 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationSlaveStorageImpl.class,
            Constants.MESSAGES);

    protected InputStream myInputStream;

    protected OutputStream myOutputStream;

    protected Socket mySocket;

    protected boolean isOutOfSync;

    protected boolean isInitialized;

    protected boolean isListening;

    protected Object mySync;

    protected Object myInit;

    protected Object myDone;

    protected Object myCommit;

    protected int myPreviousIndex;

    protected IResource myLock;

    protected Thread myThread;

    protected int[] myPageTimestamps;

    protected int[] myDirtyPageTimestampMap;

    protected OSFile myPageTimestampFile;

    protected ReplicationSlaveStorageImpl(final String aPageTimestampFilePath) {
        if (aPageTimestampFilePath != null) {
            myPageTimestampFile = new OSFile(aPageTimestampFilePath, false, myNoFlush);

            final long fileLength = myPageTimestampFile.length();

            if (fileLength == 0) {
                myPageTimestamps = new int[INIT_PAGE_TIMESTAMPS_LENGTH];
            } else {
                final byte[] page = new byte[Page.PAGE_SIZE];

                int index = 0;

                myPageTimestamps = new int[(int) (fileLength / 4)];

                for (long position = 0; position < fileLength; position += Page.PAGE_SIZE) {
                    final int readCount = myPageTimestampFile.read(position, page);

                    for (int offset = 0; offset < readCount; offset += 4) {
                        myPageTimestamps[index++] = Bytes.unpack4(page, offset);
                    }
                }

                if (index != myPageTimestamps.length) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
            }

            myDirtyPageTimestampMap = new int[(myPageTimestamps.length * 4 + Page.PAGE_SIZE -
                    1 >> Page.PAGE_SIZE_LOG) + 31 >> 5];
        }
    }

    @Override
    public void open(final IFile aFile, final long aPagePoolSize) {
        if (myOpened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }

        initialize(aFile, aPagePoolSize);
        myLock = new PersistentResource();
        myInit = new Object();
        mySync = new Object();
        myDone = new Object();
        myCommit = new Object();
        isListening = true;
        connect();
        myThread = new Thread(this);
        myThread.start();
        waitForSynchronizationCompletion();
        waitForInitializationCompletion();
        myOpened = true;
        beginReplicationSlaveTransaction();
        reloadScheme();
        endReplicationSlaveTransaction();
    }

    /**
     * Check if socket is connected to the master host
     *
     * @return <code>true</code> if connection between slave and master is successfully established
     */
    @Override
    public boolean isConnected() {
        return mySocket != null;
    }

    @Override
    public void beginReplicationSlaveTransaction() {
        beginThreadTransaction(REPLICATION_SLAVE_TRANSACTION);
    }

    @Override
    public void endReplicationSlaveTransaction() {
        myLock.unlock();
    }

    protected void waitForSynchronizationCompletion() {
        try {
            synchronized (mySync) {
                while (isOutOfSync) {
                    mySync.wait();
                }
            }
        } catch (final InterruptedException details) {
            // FIXME
        }
    }

    protected void waitForInitializationCompletion() {
        try {
            synchronized (myInit) {
                while (!isInitialized) {
                    myInit.wait();
                }
            }
        } catch (final InterruptedException details) {
            // FIXME
        }
    }

    /**
     * Wait until database is modified by master. This method blocks current thread until master node commits
     * transaction and this transaction is completely delivered to this slave node.
     */
    @Override
    public void waitForModification() {
        try {
            synchronized (myCommit) {
                if (mySocket != null) {
                    myCommit.wait();
                }
            }
        } catch (final InterruptedException details) {
            // FIXME
        }
    }

    /**
     * When overriden by base class this method performs socket error handling
     *
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be repeated,
     *         <code>false</code> if no more attempts to communicate with this host should be performed
     */
    public boolean handleError() {
        return myListener != null ? myListener.replicationError(null) : false;
    }

    void connect() {
        try {
            mySocket = getSocket();

            if (mySocket != null) {
                try {
                    mySocket.setSoLinger(true, LINGER_TIME);
                } catch (final NoSuchMethodError details) {
                    // FIXME
                }

                try {
                    mySocket.setTcpNoDelay(true);
                } catch (final Exception details) {
                    // FIXME
                }

                myInputStream = mySocket.getInputStream();

                if (myReplicationAck || myPageTimestamps != null) {
                    myOutputStream = mySocket.getOutputStream();
                }

                if (myPageTimestamps != null) {
                    final int size = myPageTimestamps.length;
                    final byte[] psBuf = new byte[4 + size * 4];

                    Bytes.pack4(psBuf, 0, size);

                    for (int i = 0; i < size; i++) {
                        Bytes.pack4(psBuf, (i + 1) * 4, myPageTimestamps[i]);
                    }

                    myOutputStream.write(psBuf, 0, psBuf.length);
                }
            }
        } catch (final IOException details) {
            details.printStackTrace();

            mySocket = null;
            myInputStream = null;
        }
    }

    abstract Socket getSocket() throws IOException;

    void cancelIO() {
    }

    @Override
    public void run() {
        final byte[] buffer = new byte[Page.PAGE_SIZE + PAGE_DATA_OFFSET + (myPageTimestamps != null ? 4 : 0)];

        while (isListening) {
            int offset = 0;

            do {
                int readCount = -1;

                if (myInputStream != null) {
                    try {
                        readCount = myInputStream.read(buffer, offset, buffer.length - offset);
                    } catch (final IOException details) {
                        details.printStackTrace();
                    }
                }

                synchronized (myDone) {
                    if (!isListening) {
                        return;
                    }
                }

                if (readCount < 0) {
                    if (handleError()) {
                        connect();
                    } else {
                        return;
                    }
                } else {
                    offset += readCount;
                }
            } while (offset < buffer.length);

            final long position = Bytes.unpack8(buffer, 0);

            boolean transactionCommit = false;

            if (position == 0) {
                if (myReplicationAck) {
                    try {
                        myOutputStream.write(buffer, 0, 1);
                    } catch (final IOException details) {
                        handleError();
                    }
                }

                if (buffer[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != myPreviousIndex) {
                    myPreviousIndex = buffer[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                    myLock.exclusiveLock();
                    transactionCommit = true;
                }
            } else if (position == REPL_SYNC) {
                synchronized (mySync) {
                    isOutOfSync = false;
                    mySync.notify();
                }

                continue;
            } else if (position == REPL_CLOSE) {
                synchronized (myCommit) {
                    hangup();
                    myCommit.notifyAll();
                }

                return;
            }

            if (myPageTimestamps != null) {
                final int pageNo = (int) (position >> Page.PAGE_SIZE_LOG);

                if (pageNo >= myPageTimestamps.length) {
                    final int newLength = pageNo >= myPageTimestamps.length * 2 ? pageNo + 1
                            : myPageTimestamps.length * 2;

                    final int[] newPageTimestamps = new int[newLength];

                    System.arraycopy(myPageTimestamps, 0, newPageTimestamps, 0, myPageTimestamps.length);
                    myPageTimestamps = newPageTimestamps;

                    final int[] newDirtyPageTimestampMap = new int[(newLength * 4 + Page.PAGE_SIZE -
                            1 >> Page.PAGE_SIZE_LOG) + 31 >> 5];

                    System.arraycopy(myDirtyPageTimestampMap, 0, newDirtyPageTimestampMap, 0,
                            myDirtyPageTimestampMap.length);
                    myDirtyPageTimestampMap = newDirtyPageTimestampMap;
                }

                final int timestamp = Bytes.unpack4(buffer, Page.PAGE_SIZE + PAGE_DATA_OFFSET);

                myPageTimestamps[pageNo] = timestamp;
                myDirtyPageTimestampMap[pageNo >> Page.PAGE_SIZE_LOG - 2 + 5] |= 1 << (pageNo >> Page.PAGE_SIZE_LOG -
                        2 & 31);
            }

            final Page pg = myPool.putPage(position);

            System.arraycopy(buffer, PAGE_DATA_OFFSET, pg.myData, 0, Page.PAGE_SIZE);
            myPool.unfix(pg);

            if (position == 0) {
                if (!isInitialized && buffer[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0) {
                    synchronized (myInit) {
                        isInitialized = true;
                        myInit.notify();
                    }
                }

                if (transactionCommit) {
                    myLock.unlock();

                    synchronized (myCommit) {
                        myCommit.notifyAll();
                    }

                    if (myListener != null) {
                        myListener.onMasterDatabaseUpdate();
                    }

                    myPool.flush();

                    if (myPageTimestamps != null) {
                        final byte[] page = new byte[Page.PAGE_SIZE];

                        for (int index = 0; index < myDirtyPageTimestampMap.length; index++) {
                            if (myDirtyPageTimestampMap[index] != 0) {
                                for (int jndex = 0; jndex < 32; jndex++) {
                                    if ((myDirtyPageTimestampMap[index] & 1 << jndex) != 0) {
                                        final int pageNo = (index << 5) + jndex;

                                        int start = pageNo << Page.PAGE_SIZE_LOG - 2;
                                        int end = start + Page.PAGE_SIZE / 4;

                                        if (end > myPageTimestamps.length) {
                                            end = myPageTimestamps.length;
                                        }

                                        offset = 0;

                                        while (start < end) {
                                            Bytes.pack4(page, offset, myPageTimestamps[start]);

                                            start += 1;
                                            offset += 4;
                                        }

                                        myPageTimestampFile.write(pageNo << Page.PAGE_SIZE_LOG, page);
                                    }
                                }
                            }

                            myDirtyPageTimestampMap[index] = 0;
                        }

                        myPageTimestampFile.sync();
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (myDone) {
            isListening = false;
        }

        cancelIO();

        try {
            myThread.interrupt();
            myThread.join();
        } catch (final InterruptedException details) {
            // FIXME
        }

        hangup();

        myPool.flush();

        super.close();

        if (myPageTimestampFile != null) {
            myPageTimestampFile.close();
        }
    }

    protected void hangup() {
        if (mySocket != null) {
            try {
                myInputStream.close();

                if (myOutputStream != null) {
                    myOutputStream.close();
                }

                mySocket.close();
            } catch (final IOException details) {
                // FIXME
            }

            myInputStream = null;
            mySocket = null;
        }
    }

    @Override
    protected boolean isDirty() {
        return false;
    }

    private void beginThreadTransaction(final int aMode) {
        final Page page;

        if (aMode != REPLICATION_SLAVE_TRANSACTION) {
            throw new IllegalArgumentException("Illegal transaction mode");
        }

        myLock.sharedLock();
        page = myPool.getPage(0);
        myHeader.unpack(page.myData);
        myPool.unfix(page);
        myCurrentIndex = 1 - myHeader.myCurrentRoot;
        myCurrentIndexSize = myHeader.myRoot[1 - myCurrentIndex].myIndexUsed;
        myCommittedIndexSize = myCurrentIndexSize;
        myUsedSize = myHeader.myRoot[myCurrentIndex].mySize;
        myObjectCache.clear();
    }

}
