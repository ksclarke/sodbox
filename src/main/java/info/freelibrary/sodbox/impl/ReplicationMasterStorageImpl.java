
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.ReplicationMasterStorage;

public class ReplicationMasterStorageImpl extends StorageImpl implements ReplicationMasterStorage {

    int myPort;

    String[] myHosts;

    int myAsyncBufSize;

    String myPageTimestampFilePath;

    /**
     * Creates a master storage.
     *
     * @param aPort A port
     * @param aHosts A host array
     * @param aAsyncBufferSize A asynchronous buffer size
     * @param aPageTimestampFilePath A page timestamp file path
     */
    public ReplicationMasterStorageImpl(final int aPort, final String[] aHosts, final int aAsyncBufferSize,
            final String aPageTimestampFilePath) {
        myPort = aPort;
        myHosts = aHosts;
        myAsyncBufSize = aAsyncBufferSize;
        myPageTimestampFilePath = aPageTimestampFilePath;
    }

    @Override
    public void open(final IFile aFile, final long aPagePoolSize) {
        super.open(myAsyncBufSize != 0 ? (ReplicationMasterFile) new AsyncReplicationMasterFile(this, aFile,
                myAsyncBufSize, myPageTimestampFilePath) : new ReplicationMasterFile(this, aFile,
                        myPageTimestampFilePath), aPagePoolSize);
    }

    @Override
    public int getNumberOfAvailableHosts() {
        return ((ReplicationMasterFile) myPool.myFile).getNumberOfAvailableHosts();
    }

}
