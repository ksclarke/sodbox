
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

public class ReplicationStaticSlaveStorageImpl extends ReplicationSlaveStorageImpl {

    protected ServerSocket acceptor;

    protected int port;

    public ReplicationStaticSlaveStorageImpl(final int port, final String pageTimestampFilePath) {
        super(pageTimestampFilePath);
        this.port = port;
    }

    // Cancel accept
    @Override
    void cancelIO() {
        try {
            final Socket s = new Socket("localhost", port);
            s.close();
        } catch (final IOException x) {
        }
    }

    @Override
    Socket getSocket() throws IOException {
        return acceptor.accept();
    }

    @Override
    public void open(final IFile file, final long pagePoolSize) {
        try {
            acceptor = new ServerSocket(port);
        } catch (final IOException x) {
            throw new StorageError(StorageError.BAD_REPLICATION_PORT);
        }
        final byte[] rootPage = new byte[Page.pageSize];
        final int rc = file.read(0, rootPage);
        if (rc == Page.pageSize) {
            prevIndex = rootPage[DB_HDR_CURR_INDEX_OFFSET];
            initialized = rootPage[DB_HDR_INITIALIZED_OFFSET] != 0;
        } else {
            initialized = false;
            prevIndex = -1;
        }
        outOfSync = false;
        super.open(file, pagePoolSize);
    }
}
