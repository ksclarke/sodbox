
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.IResource;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.ReplicationSlaveStorage;
import info.freelibrary.sodbox.StorageError;

public abstract class ReplicationSlaveStorageImpl extends StorageImpl implements ReplicationSlaveStorage, Runnable {

    static final int REPL_CLOSE = -1;

    static final int REPL_SYNC = -2;

    static final int INIT_PAGE_TIMESTAMPS_LENGTH = 64 * 1024;

    protected static final int DB_HDR_CURR_INDEX_OFFSET = 0;

    protected static final int DB_HDR_DIRTY_OFFSET = 1;

    protected static final int DB_HDR_INITIALIZED_OFFSET = 2;

    protected static final int PAGE_DATA_OFFSET = 8;

    public static int LINGER_TIME = 10; // linger parameter for the socket

    protected InputStream in;

    protected OutputStream out;

    protected Socket socket;

    protected boolean outOfSync;

    protected boolean initialized;

    protected boolean listening;

    protected Object sync;

    protected Object init;

    protected Object done;

    protected Object commit;

    protected int prevIndex;

    protected IResource lock;

    protected Thread thread;

    protected int[] pageTimestamps;

    protected int[] dirtyPageTimestampMap;

    protected OSFile pageTimestampFile;

    protected ReplicationSlaveStorageImpl(final String pageTimestampFilePath) {
        if (pageTimestampFilePath != null) {
            pageTimestampFile = new OSFile(pageTimestampFilePath, false, myNoFlush);
            final long fileLength = pageTimestampFile.length();
            if (fileLength == 0) {
                pageTimestamps = new int[INIT_PAGE_TIMESTAMPS_LENGTH];
            } else {
                pageTimestamps = new int[(int) (fileLength / 4)];
                final byte[] page = new byte[Page.pageSize];
                int i = 0;
                for (long pos = 0; pos < fileLength; pos += Page.pageSize) {
                    final int rc = pageTimestampFile.read(pos, page);
                    for (int offs = 0; offs < rc; offs += 4) {
                        pageTimestamps[i++] = Bytes.unpack4(page, offs);
                    }
                }
                if (i != pageTimestamps.length) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
            }
            dirtyPageTimestampMap = new int[(pageTimestamps.length * 4 + Page.pageSize - 1 >> Page.pageSizeLog) +
                    31 >> 5];
        }
    }

    @Override
    public void beginThreadTransaction(final int mode) {
        if (mode != REPLICATION_SLAVE_TRANSACTION) {
            throw new IllegalArgumentException("Illegal transaction mode");
        }
        lock.sharedLock();
        final Page pg = myPagePool.getPage(0);
        myHeader.unpack(pg.data);
        myPagePool.unfix(pg);
        myCurrentIndex = 1 - myHeader.myCurrentRoot;
        myCurrentIndexSize = myHeader.myRootPage[1 - myCurrentIndex].myIndexUsed;
        myCommittedIndexSize = myCurrentIndexSize;
        usedSize = myHeader.myRootPage[myCurrentIndex].mySize;
        myObjectCache.clear();
    }

    void cancelIO() {
    }

    @Override
    public void close() {
        synchronized (done) {
            listening = false;
        }
        cancelIO();
        try {
            thread.interrupt();
            thread.join();
        } catch (final InterruptedException x) {
        }

        hangup();

        myPagePool.flush();
        super.close();
        if (pageTimestampFile != null) {
            pageTimestampFile.close();
        }
    }

    void connect() {
        try {
            socket = getSocket();
            if (socket != null) {
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (final NoSuchMethodError er) {
                }
                try {
                    socket.setTcpNoDelay(true);
                } catch (final Exception x) {
                }
                in = socket.getInputStream();
                if (myReplicationAck || pageTimestamps != null) {
                    out = socket.getOutputStream();
                }
                if (pageTimestamps != null) {
                    final int size = pageTimestamps.length;
                    final byte[] psBuf = new byte[4 + size * 4];
                    Bytes.pack4(psBuf, 0, size);
                    for (int i = 0; i < size; i++) {
                        Bytes.pack4(psBuf, (i + 1) * 4, pageTimestamps[i]);
                    }
                    out.write(psBuf, 0, psBuf.length);
                }
            }
        } catch (final IOException x) {
            x.printStackTrace();
            socket = null;
            in = null;
        }
    }

    @Override
    public void endThreadTransaction(final int maxDelay) {
        lock.unlock();
    }

    abstract Socket getSocket() throws IOException;

    /**
     * When overriden by base class this method perfroms socket error handling
     * 
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be repeated,
     *         <code>false</code> if no more attmpts to communicate with this host should be performed
     */
    public boolean handleError() {
        return myListener != null ? myListener.replicationError(null) : false;
    }

    protected void hangup() {
        if (socket != null) {
            try {
                in.close();
                if (out != null) {
                    out.close();
                }
                socket.close();
            } catch (final IOException x) {
            }
            in = null;
            socket = null;
        }
    }

    /**
     * Check if socket is connected to the master host
     * 
     * @return <code>true</code> if connection between slave and master is sucessfully established
     */
    @Override
    public boolean isConnected() {
        return socket != null;
    }

    @Override
    protected boolean isDirty() {
        return false;
    }

    @Override
    public void open(final IFile file, final long pagePoolSize) {
        if (myOpened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }
        initialize(file, pagePoolSize);
        lock = new PersistentResource();
        init = new Object();
        sync = new Object();
        done = new Object();
        commit = new Object();
        listening = true;
        connect();
        thread = new Thread(this);
        thread.start();
        waitSynchronizationCompletion();
        waitInitializationCompletion();
        myOpened = true;
        beginThreadTransaction(REPLICATION_SLAVE_TRANSACTION);
        reloadScheme();
        endThreadTransaction();
    }

    @Override
    public void run() {
        final byte[] buf = new byte[Page.pageSize + PAGE_DATA_OFFSET + (pageTimestamps != null ? 4 : 0)];

        while (listening) {
            int offs = 0;
            do {
                int rc = -1;
                if (in != null) {
                    try {
                        rc = in.read(buf, offs, buf.length - offs);
                    } catch (final IOException x) {
                        x.printStackTrace();
                    }
                }
                synchronized (done) {
                    if (!listening) {
                        return;
                    }
                }
                if (rc < 0) {
                    if (handleError()) {
                        connect();
                    } else {
                        return;
                    }
                } else {
                    offs += rc;
                }
            } while (offs < buf.length);

            final long pos = Bytes.unpack8(buf, 0);
            boolean transactionCommit = false;
            if (pos == 0) {
                if (myReplicationAck) {
                    try {
                        out.write(buf, 0, 1);
                    } catch (final IOException x) {
                        handleError();
                    }
                }
                if (buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != prevIndex) {
                    prevIndex = buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                    lock.exclusiveLock();
                    transactionCommit = true;
                }
            } else if (pos == REPL_SYNC) {
                synchronized (sync) {
                    outOfSync = false;
                    sync.notify();
                }
                continue;
            } else if (pos == REPL_CLOSE) {
                synchronized (commit) {
                    hangup();
                    commit.notifyAll();
                }
                return;
            }
            if (pageTimestamps != null) {
                final int pageNo = (int) (pos >> Page.pageSizeLog);
                if (pageNo >= pageTimestamps.length) {
                    final int newLength = pageNo >= pageTimestamps.length * 2 ? pageNo + 1 : pageTimestamps.length *
                            2;

                    final int[] newPageTimestamps = new int[newLength];
                    System.arraycopy(pageTimestamps, 0, newPageTimestamps, 0, pageTimestamps.length);
                    pageTimestamps = newPageTimestamps;

                    final int[] newDirtyPageTimestampMap = new int[(newLength * 4 + Page.pageSize -
                            1 >> Page.pageSizeLog) + 31 >> 5];
                    System.arraycopy(dirtyPageTimestampMap, 0, newDirtyPageTimestampMap, 0,
                            dirtyPageTimestampMap.length);
                    dirtyPageTimestampMap = newDirtyPageTimestampMap;
                }
                final int timestamp = Bytes.unpack4(buf, Page.pageSize + PAGE_DATA_OFFSET);
                pageTimestamps[pageNo] = timestamp;
                dirtyPageTimestampMap[pageNo >> Page.pageSizeLog - 2 + 5] |= 1 << (pageNo >> Page.pageSizeLog - 2 &
                        31);
            }
            final Page pg = myPagePool.putPage(pos);
            System.arraycopy(buf, PAGE_DATA_OFFSET, pg.data, 0, Page.pageSize);
            myPagePool.unfix(pg);

            if (pos == 0) {
                if (!initialized && buf[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0) {
                    synchronized (init) {
                        initialized = true;
                        init.notify();
                    }
                }
                if (transactionCommit) {
                    lock.unlock();
                    synchronized (commit) {
                        commit.notifyAll();
                    }
                    if (myListener != null) {
                        myListener.onMasterDatabaseUpdate();
                    }
                    myPagePool.flush();
                    if (pageTimestamps != null) {
                        final byte[] page = new byte[Page.pageSize];
                        for (int i = 0; i < dirtyPageTimestampMap.length; i++) {
                            if (dirtyPageTimestampMap[i] != 0) {
                                for (int j = 0; j < 32; j++) {
                                    if ((dirtyPageTimestampMap[i] & 1 << j) != 0) {
                                        final int pageNo = (i << 5) + j;
                                        int beg = pageNo << Page.pageSizeLog - 2;
                                        int end = beg + Page.pageSize / 4;
                                        if (end > pageTimestamps.length) {
                                            end = pageTimestamps.length;
                                        }
                                        offs = 0;
                                        while (beg < end) {
                                            Bytes.pack4(page, offs, pageTimestamps[beg]);
                                            beg += 1;
                                            offs += 4;
                                        }
                                        pageTimestampFile.write(pageNo << Page.pageSizeLog, page);
                                    }
                                }
                            }
                            dirtyPageTimestampMap[i] = 0;
                        }
                        pageTimestampFile.sync();
                    }
                }
            }
        }
    }

    /**
     * Wait until database is modified by master This method blocks current thread until master node commits
     * trasanction and this transanction is completely delivered to this slave node
     */
    @Override
    public void waitForModification() {
        try {
            synchronized (commit) {
                if (socket != null) {
                    commit.wait();
                }
            }
        } catch (final InterruptedException x) {
        }
    }

    protected void waitInitializationCompletion() {
        try {
            synchronized (init) {
                while (!initialized) {
                    init.wait();
                }
            }
        } catch (final InterruptedException x) {
        }
    }

    protected void waitSynchronizationCompletion() {
        try {
            synchronized (sync) {
                while (outOfSync) {
                    sync.wait();
                }
            }
        } catch (final InterruptedException x) {
        }
    }
}
