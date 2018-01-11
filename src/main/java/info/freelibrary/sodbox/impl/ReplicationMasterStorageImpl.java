
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.ReplicationMasterStorage;

public class ReplicationMasterStorageImpl extends StorageImpl implements ReplicationMasterStorage {

    int port;

    String[] hosts;

    int asyncBufSize;

    String pageTimestampFile;

    public ReplicationMasterStorageImpl(final int port, final String[] hosts, final int asyncBufSize,
            final String pageTimestampFile) {
        this.port = port;
        this.hosts = hosts;
        this.asyncBufSize = asyncBufSize;
        this.pageTimestampFile = pageTimestampFile;
    }

    @Override
    public int getNumberOfAvailableHosts() {
        return ((ReplicationMasterFile) myPagePool.file).getNumberOfAvailableHosts();
    }

    @Override
    public void open(final IFile file, final long pagePoolSize) {
        super.open(asyncBufSize != 0 ? (ReplicationMasterFile) new AsyncReplicationMasterFile(this, file,
                asyncBufSize, pageTimestampFile) : new ReplicationMasterFile(this, file, pageTimestampFile),
                pagePoolSize);
    }
}
