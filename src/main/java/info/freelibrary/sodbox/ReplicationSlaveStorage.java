
package info.freelibrary.sodbox;

/**
 * Storage receiving modified pages from replication master and been able to run read-only transactions.
 */
public interface ReplicationSlaveStorage extends Storage {

    /**
     * Check if socket is connected to the master host.
     *
     * @return <code>true</code> if connection between slave and master is successfully established
     */
    boolean isConnected();

    /**
     * Wait until database is modified by master This method blocks current thread until master node commits
     * transaction and this transaction is completely delivered to this slave node.
     */
    void waitForModification();

}
