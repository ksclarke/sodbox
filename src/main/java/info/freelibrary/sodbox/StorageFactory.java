
package info.freelibrary.sodbox;

import info.freelibrary.sodbox.impl.ReplicationDynamicSlaveStorageImpl;
import info.freelibrary.sodbox.impl.ReplicationMasterStorageImpl;
import info.freelibrary.sodbox.impl.ReplicationStaticSlaveStorageImpl;
import info.freelibrary.sodbox.impl.StorageImpl;

/**
 * Storage factory.
 */
public class StorageFactory {

    protected static final StorageFactory INSTANCE = new StorageFactory();

    /**
     * Create new instance of the storage.
     *
     * @return new instance of the storage (unopened, you should explicitly invoke open method)
     */
    public Storage createStorage() {
        return new StorageImpl();
    }

    /**
     * Create new instance of the master node of replicated storage. There are two kinds of replication slave nodes:
     * statically defined and dynamically added. First one are specified by replicationSlaveNodes parameter. When
     * replication master is started it tries to establish connection with all of the specified nodes. It is expected
     * that state of each such node is synchronized with state of the master node. It is not possible to add or remove
     * static replication slave node without stopping master node. Dynamic slave nodes can be added at any moment of
     * time. Replication master will send to such node complete snapshot of the database.
     *
     * @param aPort socket port at which replication master will listen for dynamic slave nodes connections. If this
     *        parameter is -1, then no dynamic slave node connections are accepted.
     * @param aReplicationSlaveNodes addresses of static replication slave nodes, i.e. hosts to which replication will
     *        be performed. Address is specified as NAME:PORT
     * @param aAsyncBufSize if value of this parameter is greater than zero then replication will be asynchronous,
     *        done by separate thread and not blocking main application. Otherwise data is send to the slave nodes by
     *        the same thread which updates the database. If space asynchronous buffer is exhausted, then main thread
     *        will be also blocked until the data is send.
     * @return new instance of the master storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationMasterStorage createReplicationMasterStorage(final int aPort,
            final String[] aReplicationSlaveNodes, final int aAsyncBufSize) {
        return new ReplicationMasterStorageImpl(aPort, aReplicationSlaveNodes, aAsyncBufSize, null);
    }

    /**
     * Create new instance of the master node of replicated storage. There are two kinds of replication slave nodes:
     * statically defined and dynamically added. First one are specified by replicationSlaveNodes parameter. When
     * replication master is started it tries to establish connection with all of the specified nodes. It is expected
     * that state of each such node is synchronized with state of the master node. It is not possible to add or remove
     * static replication slave node without stopping master node. Dynamic slave nodes can be added at any moment of
     * time. Replication master will send to such node complete snapshot of the database.
     *
     * @param aPort socket port at which replication master will listen for dynamic slave nodes connections. If this
     *        parameter is -1, then no dynamic slave node connections are accepted.
     * @param aReplicationSlaveNodes addresses of static replication slave nodes, i.e. hosts to which replication will
     *        be performed. Address is specified as NAME:PORT
     * @param aAsyncBufSize if value of this parameter is greater than zero then replication will be asynchronous,
     *        done by separate thread and not blocking main application. Otherwise data is send to the slave nodes by
     *        the same thread which updates the database. If space asynchronous buffer is exhausted, then main thread
     *        will be also blocked until the data is send.
     * @param aPageTimestampFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     * @return new instance of the master storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationMasterStorage createReplicationMasterStorage(final int aPort,
            final String[] aReplicationSlaveNodes, final int aAsyncBufSize, final String aPageTimestampFile) {
        return new ReplicationMasterStorageImpl(aPort, aReplicationSlaveNodes, aAsyncBufSize, aPageTimestampFile);
    }

    /**
     * Create new instance of the static slave node of replicated storage. The address of this host should be
     * specified in the replicationSlaveNodes parameter of createReplicationMasterStorage method. When replication
     * master is started it tries to establish connection with all of the specified nodes.
     *
     * @param aSlavePort socket port at which connection from master will be established
     * @return new instance of the slave storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationSlaveStorage createReplicationSlaveStorage(final int aSlavePort) {
        return new ReplicationStaticSlaveStorageImpl(aSlavePort, null);
    }

    /**
     * Create new instance of the static slave node of replicated storage. The address of this host should be
     * specified in the replicationSlaveNodes parameter of createReplicationMasterStorage method. When replication
     * master is started it tries to establish connection with all of the specified nodes.
     *
     * @param aSlavePort socket port at which connection from master will be established
     * @param aPageTimestampFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     * @return new instance of the slave storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationSlaveStorage createReplicationSlaveStorage(final int aSlavePort,
            final String aPageTimestampFile) {
        return new ReplicationStaticSlaveStorageImpl(aSlavePort, aPageTimestampFile);
    }

    /**
     * Add new instance of the dynamic slave node of replicated storage.
     *
     * @param aReplicationMasterNode name of the host where replication master is running
     * @param aMasterPort replication master socket port to which connection should be established
     * @return new instance of the slave storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationSlaveStorage addReplicationSlaveStorage(final String aReplicationMasterNode,
            final int aMasterPort) {
        return new ReplicationDynamicSlaveStorageImpl(aReplicationMasterNode, aMasterPort, null);
    }

    /**
     * Add new instance of the dynamic slave node of replicated storage.
     *
     * @param aReplicationMasterNode name of the host where replication master is running
     * @param aMasterPort replication master socket port to which connection should be established
     * @param aPageTimestampFile path to the file with pages timestamps. This file is used for synchronizing with
     *        master content of newly attached node
     * @return new instance of the slave storage (unopened, you should explicitly invoke open method)
     */
    public ReplicationSlaveStorage addReplicationSlaveStorage(final String aReplicationMasterNode,
            final int aMasterPort, final String aPageTimestampFile) {
        return new ReplicationDynamicSlaveStorageImpl(aReplicationMasterNode, aMasterPort, aPageTimestampFile);
    }

    /**
     * Get instance of storage factory. So new storages should be create in application in the following way:
     * <code>StorageFactory.getInstance().createStorage()</code>
     *
     * @return instance of the storage factory
     */
    public static StorageFactory getInstance() {
        return INSTANCE;
    }

};
