
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;

public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl {

    protected String host;

    protected int port;

    public ReplicationDynamicSlaveStorageImpl(final String host, final int port, final String pageTimestampFilePath) {
        super(pageTimestampFilePath);
        this.host = host;
        this.port = port;
    }

    @Override
    Socket getSocket() throws IOException {
        return new Socket(host, port);
    }

    @Override
    public void open(final IFile file, final long pagePoolSize) {
        initialized = false;
        prevIndex = -1;
        outOfSync = true;
        super.open(file, pagePoolSize);
    }
}
