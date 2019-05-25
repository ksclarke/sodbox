
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.net.Socket;

import info.freelibrary.sodbox.IFile;

public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl {

    protected String myHost;

    protected int myPort;

    /**
     * Creates a replication storage slave.
     *
     * @param aHost A host
     * @param aPort A port
     * @param aPageTimestampFilePath A page timestamp file path
     */
    public ReplicationDynamicSlaveStorageImpl(final String aHost, final int aPort,
            final String aPageTimestampFilePath) {
        super(aPageTimestampFilePath);

        myHost = aHost;
        myPort = aPort;
    }

    @Override
    public void open(final IFile aFile, final long aPagePoolSize) {
        isInitialized = false;
        myPreviousIndex = -1;
        isOutOfSync = true;
        super.open(aFile, aPagePoolSize);
    }

    @Override
    Socket getSocket() throws IOException {
        return new Socket(myHost, myPort);
    }

}
