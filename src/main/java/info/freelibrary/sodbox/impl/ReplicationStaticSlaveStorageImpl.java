
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

public class ReplicationStaticSlaveStorageImpl extends ReplicationSlaveStorageImpl {

    protected ServerSocket myAcceptor;

    protected int myPort;

    /**
     * Creates a new replication static slave storage.
     * 
     * @param aPort
     * @param aPageTimestampFilePath
     */
    public ReplicationStaticSlaveStorageImpl(final int aPort, final String aPageTimestampFilePath) {
        super(aPageTimestampFilePath);

        myPort = aPort;
    }

    @Override
    public void open(final IFile aFile, final long aPagePoolSize) {
        try {
            myAcceptor = new ServerSocket(myPort);
        } catch (IOException x) {
            throw new StorageError(StorageError.BAD_REPLICATION_PORT);
        }

        final byte[] rootPage = new byte[Page.PAGE_SIZE];
        final int rc = aFile.read(0, rootPage);

        if (rc == Page.PAGE_SIZE) {
            myPreviousIndex = rootPage[DB_HDR_CURR_INDEX_OFFSET];
            isInitialized = rootPage[DB_HDR_INITIALIZED_OFFSET] != 0;
        } else {
            isInitialized = false;
            myPreviousIndex = -1;
        }

        isOutOfSync = false;

        super.open(aFile, aPagePoolSize);
    }

    @Override
    Socket getSocket() throws IOException {
        return myAcceptor.accept();
    }

    // Cancel accept
    @Override
    void cancelIO() {
        try {
            new Socket("localhost", myPort).close();
        } catch (IOException details) {
            // LOG THIS
        }
    }

}
