
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

/**
 * File performing replication of changed pages to specified slave nodes.
 */
public class ReplicationMasterFile implements IFile, Runnable {

    public static int LINGER_TIME = 10; // linger parameter for the socket

    public static int MAX_CONNECT_ATTEMPTS = 10; // attempts to establish connection with slave node

    public static int CONNECTION_TIMEOUT = 1000; // timeout between attempts to connect to the slave

    public static int INIT_PAGE_TIMESTAMPS_LENGTH = 64 * 1024;

    Object myMutex;

    OutputStream[] myOutputStream;

    InputStream[] myInputStream;

    Socket[] mySockets;

    byte[] myTxBuf;

    byte[] myRcBuf;

    IFile myFile;

    String[] myHosts;

    int myNumOfHosts;

    int myPort;

    boolean myAck;

    boolean myListening;

    Thread myListenThread;

    ServerSocket myListenSocket;

    int[] myPageTs;

    int[] myDirtyPageTsMap;

    OSFile myPageTsFile;

    int myTimestamp;

    Thread[] mySyncThreads;

    ReplicationMasterStorageImpl myStorage;

    /**
     * Constructor of replication master file
     *
     * @param aStorage replication storage
     * @param aFile local file used to store data locally
     * @param aPageTimestampFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     */
    public ReplicationMasterFile(final ReplicationMasterStorageImpl aStorage, final IFile aFile,
            final String aPageTimestampFile) {
        this(aStorage, aFile, aStorage.myPort, aStorage.myHosts, aStorage.myReplicationAck, aPageTimestampFile);
    }

    /**
     * Constructor of replication master file
     *
     * @param aFile local file used to store data locally
     * @param aHosts slave node hosts to which replication will be performed
     * @param aAck whether master should wait acknowledgment from slave node during transaction commit
     * @param aPageTimestampFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     */
    public ReplicationMasterFile(final IFile aFile, final String[] aHosts, final boolean aAck,
            final String aPageTimestampFile) {
        this(null, aFile, -1, aHosts, aAck, aPageTimestampFile);
    }

    /**
     * Constructor of replication master file
     *
     * @param aFile local file used to store data locally
     * @param aHosts slave node hosts to which replication will be performed
     * @param aAck whether master should wait acknowledgment from slave node during transaction commit
     */
    public ReplicationMasterFile(final IFile aFile, final String[] aHosts, final boolean aAck) {
        this(null, aFile, -1, aHosts, aAck, null);
    }

    private ReplicationMasterFile(final ReplicationMasterStorageImpl aStorage, final IFile aFile, final int aPort,
            final String[] aHosts, final boolean aAck, final String aPageTimestampFilePath) {
        myStorage = aStorage;
        myFile = aFile;
        myHosts = aHosts;
        myAck = aAck;
        myPort = aPort;
        myMutex = new Object();
        mySockets = new Socket[aHosts.length];
        mySyncThreads = new Thread[aHosts.length];
        myOutputStream = new OutputStream[aHosts.length];

        if (aAck) {
            myInputStream = new InputStream[aHosts.length];
            myRcBuf = new byte[1];
        }

        myNumOfHosts = 0;

        if (aPageTimestampFilePath != null) {
            myPageTsFile = new OSFile(aPageTimestampFilePath, false, aStorage != null && aStorage.myNoFlush);

            final long fileLength = myPageTsFile.length();

            if (fileLength == 0) {
                myPageTs = new int[INIT_PAGE_TIMESTAMPS_LENGTH];
            } else {
                final byte[] page = new byte[Page.PAGE_SIZE];

                int i = 0;

                myPageTs = new int[(int) (fileLength / 4)];

                for (long pos = 0; pos < fileLength; pos += Page.PAGE_SIZE) {
                    final int rc = myPageTsFile.read(pos, page);

                    for (int offs = 0; offs < rc; offs += 4, i++) {
                        myPageTs[i] = Bytes.unpack4(page, offs);

                        if (myPageTs[i] > myTimestamp) {
                            myTimestamp = myPageTs[i];
                        }
                    }
                }

                if (i != myPageTs.length) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
            }

            myDirtyPageTsMap = new int[(myPageTs.length * 4 + Page.PAGE_SIZE - 1 >> Page.PAGE_SIZE_LOG) +
                    31 >> 5];
            myTxBuf = new byte[12 + Page.PAGE_SIZE];
        } else {
            myTxBuf = new byte[8 + Page.PAGE_SIZE];
        }

        for (int i = 0; i < aHosts.length; i++) {
            connect(i);
        }

        if (aPort >= 0) {
            aStorage.setProperty("sodbox.alternative.btree", Boolean.TRUE); // prevent direct modification of pages

            try {
                myListenSocket = new ServerSocket(aPort);
            } catch (final IOException x) {
                throw new StorageError(StorageError.BAD_REPLICATION_PORT);
            }

            myListening = true;
            myListenThread = new Thread(this);
            myListenThread.start();
        }
    }

    @Override
    public void run() {
        while (true) {
            Socket socket = null;

            try {
                socket = myListenSocket.accept();
            } catch (final IOException details) {
                details.printStackTrace();
            }

            synchronized (myMutex) {
                if (!myListening) {
                    return;
                }
            }

            if (socket != null) {
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (final Exception details) {
                    //
                }

                try {
                    socket.setTcpNoDelay(true);
                } catch (final Exception details) {
                    //
                }

                addConnection(socket);
            }
        }
    }

    private void addConnection(final Socket aSocket) {
        OutputStream os = null;
        InputStream is = null;

        try {
            os = aSocket.getOutputStream();

            if (myAck || myPageTs != null) {
                is = aSocket.getInputStream();
            }
        } catch (final IOException details) {
            details.printStackTrace();
            return;
        }

        synchronized (myMutex) {
            final int n = myHosts.length;
            final String[] newHosts = new String[n + 1];

            System.arraycopy(myHosts, 0, newHosts, 0, n);

            newHosts[n] = aSocket.getRemoteSocketAddress().toString();
            myHosts = newHosts;

            final OutputStream[] newOut = new OutputStream[n + 1];

            System.arraycopy(myOutputStream, 0, newOut, 0, n);

            newOut[n] = os;
            myOutputStream = newOut;

            if (myAck || myPageTs != null) {
                final InputStream[] newIn = new InputStream[n + 1];

                if (myInputStream != null) {
                    System.arraycopy(myInputStream, 0, newIn, 0, n);
                }

                newIn[n] = is;
                myInputStream = newIn;
            }

            final Socket[] newSockets = new Socket[n + 1];

            System.arraycopy(mySockets, 0, newSockets, 0, n);

            newSockets[n] = aSocket;
            mySockets = newSockets;
            myNumOfHosts += 1;

            final Thread thread = new SynchronizeThread(n);
            final Thread[] newThreads = new Thread[n + 1];

            System.arraycopy(mySyncThreads, 0, newThreads, 0, n);

            newThreads[n] = thread;
            mySyncThreads = newThreads;

            thread.start();
        }
    }

    @SuppressWarnings("unused")
    private void synchronizeNode(final int aIndex) {
        final long size = myStorage.getDatabaseSize();
        final Socket socket;

        OutputStream os = null;
        InputStream is = null;

        synchronized (myMutex) {
            socket = mySockets[aIndex];

            if (socket == null) {
                mySyncThreads[aIndex] = null;
                return;
            }

            os = myOutputStream[aIndex];

            if (myAck || myPageTs != null) {
                is = myInputStream[aIndex];
            }
        }

        int[] syncNodeTimestamps = null;

        try {
            Sync:
            do {
                final byte[] txBuf;

                if (myPageTs != null) {
                    txBuf = new byte[12 + Page.PAGE_SIZE];
                    byte[] psBuf = new byte[4];

                    if (is.read(psBuf) != 4) {
                        System.err.println("Failed to receive page timestamps length from slave node");
                        break Sync;
                    }

                    final int psSize = Bytes.unpack4(psBuf, 0);

                    psBuf = new byte[psSize * 4];
                    int offs = 0;

                    while (offs < psBuf.length) {
                        final int rc = is.read(psBuf, offs, psBuf.length - offs);

                        if (rc <= 0) {
                            System.err.println("Failed to receive page timestamps from slave node");

                            break Sync;
                        }

                        offs += rc;
                    }

                    syncNodeTimestamps = new int[psSize];

                    for (int j = 0; j < psSize; j++) {
                        syncNodeTimestamps[j] = Bytes.unpack4(psBuf, j * 4);
                    }
                } else {
                    txBuf = new byte[8 + Page.PAGE_SIZE];
                }

                for (long pos = 0; pos < size; pos += Page.PAGE_SIZE) {
                    final int pageNo = (int) (pos >> Page.PAGE_SIZE_LOG);

                    if (syncNodeTimestamps != null) {
                        if (pageNo < syncNodeTimestamps.length && pageNo < myPageTs.length &&
                                syncNodeTimestamps[pageNo] == myPageTs[pageNo]) {
                            continue;
                        }
                    }

                    synchronized (myStorage) {
                        synchronized (myStorage.myObjectCache) {
                            final Page page = myStorage.myPool.getPage(pos);

                            Bytes.pack8(txBuf, 0, pos);

                            System.arraycopy(page.myData, 0, txBuf, 8, Page.PAGE_SIZE);

                            myStorage.myPool.unfix(page);

                            if (syncNodeTimestamps != null) {
                                Bytes.pack4(txBuf, Page.PAGE_SIZE + 8, pageNo < myPageTs.length
                                        ? myPageTs[pageNo] : 0);
                            }
                        }
                    }

                    final Page page = myStorage.myPool.getPage(pos);

                    synchronized (socket) {
                        os.write(txBuf);

                        if (myAck && pos == 0 && is.read(myRcBuf) != 1) {
                            System.err.println("Failed to receive ACK");
                            break Sync;
                        }
                    }
                }

                synchronized (socket) {
                    Bytes.pack8(txBuf, 0, ReplicationSlaveStorageImpl.REPL_SYNC);
                    os.write(txBuf); // end of synchronization
                }

                return;
            } while (false);
        } catch (final IOException details) {
            details.printStackTrace();
        }

        synchronized (myMutex) {
            if (mySockets[aIndex] != null) {
                handleError(myHosts[aIndex]);
                mySockets[aIndex] = null;
                myOutputStream[aIndex] = null;
                mySyncThreads[aIndex] = null;
                myNumOfHosts -= 1;
            }
        }
    }

    /**
     * Gets number of available hosts.
     *
     * @return Number of available hosts
     */
    public int getNumberOfAvailableHosts() {
        return myNumOfHosts;
    }

    @SuppressWarnings("unused")
    protected void connect(final int aIndex) {
        String host = myHosts[aIndex];

        final int colon = host.indexOf(':');
        final int port = Integer.parseInt(host.substring(colon + 1));

        host = host.substring(0, colon);
        Socket socket = null;

        try {
            final int maxAttempts = myStorage != null ? myStorage.mySlaveConnectionTimeout : MAX_CONNECT_ATTEMPTS;

            for (int j = 0; j < maxAttempts; j++) {
                try {
                    socket = new Socket(InetAddress.getByName(host), port);

                    if (socket != null) {
                        break;
                    }

                    Thread.sleep(CONNECTION_TIMEOUT);
                } catch (final IOException details) {
                    //
                }
            }
        } catch (final InterruptedException details) {
            //
        }

        if (socket != null) {
            try {
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (final NoSuchMethodError details) {
                    //
                }

                try {
                    socket.setTcpNoDelay(true);
                } catch (final Exception details) {
                    //
                }

                mySockets[aIndex] = socket;
                myOutputStream[aIndex] = socket.getOutputStream();

                if (myAck || myPageTs != null) {
                    myInputStream[aIndex] = socket.getInputStream();
                }

                myNumOfHosts += 1;

                if (myPageTs != null) {
                    synchronizeNode(aIndex);
                }
            } catch (final IOException x) {
                handleError(myHosts[aIndex]);
                mySockets[aIndex] = null;
                myOutputStream[aIndex] = null;
            }
        }
    }

    /**
     * When overriden by base class this method performs socket error handling
     *
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be repeated,
     *         <code>false</code> if no more attempts to communicate with this host should be performed
     */
    public boolean handleError(final String aHost) {
        System.err.println("Failed to establish connection with host " + aHost);
        return myStorage != null && myStorage.myListener != null ? myStorage.myListener.replicationError(aHost) : false;
    }

    @Override
    public void write(final long aPosition, final byte[] aBytes) {
        synchronized (myMutex) {
            if (myPageTs != null) {
                final int pageNo = (int) (aPosition >> Page.PAGE_SIZE_LOG);

                if (pageNo >= myPageTs.length) {
                    final int newLength = pageNo >= myPageTs.length * 2 ? pageNo + 1 : myPageTs.length *
                            2;
                    final int[] newPageTimestamps = new int[newLength];

                    System.arraycopy(myPageTs, 0, newPageTimestamps, 0, myPageTs.length);

                    myPageTs = newPageTimestamps;

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

            for (int i = 0; i < myOutputStream.length; i++) {
                while (myOutputStream[i] != null) {
                    try {
                        synchronized (mySockets[i]) {
                            Bytes.pack8(myTxBuf, 0, aPosition);
                            System.arraycopy(aBytes, 0, myTxBuf, 8, aBytes.length);

                            if (myPageTs != null) {
                                Bytes.pack4(myTxBuf, Page.PAGE_SIZE + 8, myTimestamp);
                            }

                            myOutputStream[i].write(myTxBuf);

                            if (!myAck || aPosition != 0 || myInputStream[i].read(myRcBuf) == 1) {
                                break;
                            }
                        }
                    } catch (final IOException details) {
                        //
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
        }

        myFile.write(aPosition, aBytes);
    }

    @Override
    public int read(final long aPosition, final byte[] aBytes) {
        return myFile.read(aPosition, aBytes);
    }

    @Override
    public void sync() {
        if (myPageTs != null) {
            synchronized (myMutex) {
                final byte[] page = new byte[Page.PAGE_SIZE];

                for (int i = 0; i < myDirtyPageTsMap.length; i++) {
                    if (myDirtyPageTsMap[i] != 0) {
                        for (int j = 0; j < 32; j++) {
                            if ((myDirtyPageTsMap[i] & 1 << j) != 0) {
                                final int pageNo = (i << 5) + j;

                                int beg = pageNo << Page.PAGE_SIZE_LOG - 2;
                                int end = beg + Page.PAGE_SIZE / 4;

                                if (end > myPageTs.length) {
                                    end = myPageTs.length;
                                }

                                int offs = 0;

                                while (beg < end) {
                                    Bytes.pack4(page, offs, myPageTs[beg]);
                                    beg += 1;
                                    offs += 4;
                                }

                                final long pos = pageNo << Page.PAGE_SIZE_LOG;

                                myPageTsFile.write(pos, page);
                            }
                        }

                        myDirtyPageTsMap[i] = 0;
                    }
                }
            }

            myPageTsFile.sync();
        }

        myFile.sync();
    }

    @Override
    public boolean tryLock(final boolean aShared) {
        return myFile.tryLock(aShared);
    }

    @Override
    public void lock(final boolean aSharedLock) {
        myFile.lock(aSharedLock);
    }

    @Override
    public void unlock() {
        myFile.unlock();
    }

    @Override
    public void close() {
        if (myListenThread != null) {
            synchronized (myMutex) {
                myListening = false;
            }

            try {
                final Socket socket = new Socket("localhost", myPort);

                socket.close();
            } catch (final IOException details) {
                //
            }

            try {
                myListenThread.join();
            } catch (final InterruptedException details) {
                //
            }

            try {
                myListenSocket.close();
            } catch (final IOException details) {
                //
            }
        }

        for (int i = 0; i < mySyncThreads.length; i++) {
            final Thread t = mySyncThreads[i];

            if (t != null) {
                try {
                    t.join();
                } catch (final InterruptedException details) {
                    //
                }
            }
        }

        myFile.close();
        Bytes.pack8(myTxBuf, 0, ReplicationSlaveStorageImpl.REPL_CLOSE);

        for (int i = 0; i < myOutputStream.length; i++) {
            if (mySockets[i] != null) {
                try {
                    myOutputStream[i].write(myTxBuf);
                    myOutputStream[i].close();

                    if (myInputStream != null) {
                        myInputStream[i].close();
                    }

                    mySockets[i].close();
                } catch (final IOException details) {
                    //
                }
            }
        }

        if (myPageTsFile != null) {
            myPageTsFile.close();
        }
    }

    @Override
    public long length() {
        return myFile.length();
    }

    class SynchronizeThread extends Thread {

        int myIndex;

        SynchronizeThread(final int aIndex) {
            myIndex = aIndex;
            // setPriority(Thread.NORM_PRIORITY-1);
        }

        @Override
        public void run() {
            synchronizeNode(myIndex);
        }
    }
}
