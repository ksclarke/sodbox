
package info.freelibrary.sodbox;

/**
 * Base class for persistent capable objects supporting locking.
 */
public class PersistentResource extends Persistent implements IResource {

    private transient Thread owner;

    private transient int nReaders;

    private transient int nWriters;

    public PersistentResource() {
    }

    public PersistentResource(final Storage storage) {
        super(storage);
    }

    @Override
    public synchronized void exclusiveLock() {
        final Thread currThread = Thread.currentThread();

        try {
            while (true) {
                if (owner == currThread) {
                    nWriters += 1;
                    break;
                } else if (nReaders == 0 && nWriters == 0) {
                    nWriters = 1;
                    owner = currThread;
                    if (myStorage != null) {
                        myStorage.lockObject(this);
                    }
                    break;
                } else {
                    wait();
                }
            }
        } catch (final InterruptedException x) {
            throw new StorageError(StorageError.LOCK_FAILED);
        }
    }

    @Override
    public boolean exclusiveLock(final long timeout) {
        final Thread currThread = Thread.currentThread();
        final long startTime = System.currentTimeMillis();

        synchronized (this) {
            try {
                while (true) {
                    if (owner == currThread) {
                        nWriters += 1;
                        return true;
                    } else if (nReaders == 0 && nWriters == 0) {
                        nWriters = 1;
                        owner = currThread;
                        if (myStorage != null) {
                            myStorage.lockObject(this);
                        }
                        return true;
                    } else {
                        long currTime = System.currentTimeMillis();
                        if (currTime < startTime) {
                            currTime = startTime;
                        }
                        if (startTime + timeout <= currTime) {
                            return false;
                        }
                        wait(startTime + timeout - currTime);
                    }
                }
            } catch (final InterruptedException x) {
                return false;
            }
        }
    }

    @Override
    public synchronized void reset() {
        if (nWriters > 0) {
            nWriters = 0;
            nReaders = 0;
            owner = null;
        } else if (nReaders > 0) {
            nReaders -= 1;
        }

        notifyAll();
    }

    @Override
    public synchronized void sharedLock() {
        final Thread currThread = Thread.currentThread();

        try {
            while (true) {
                if (owner == currThread) {
                    nWriters += 1;
                    break;
                } else if (nWriters == 0) {
                    if (myStorage == null || myStorage.lockObject(this)) {
                        nReaders += 1;
                    }
                    break;
                } else {
                    wait();
                }
            }
        } catch (final InterruptedException x) {
            throw new StorageError(StorageError.LOCK_FAILED);
        }
    }

    @Override
    public boolean sharedLock(final long timeout) {
        final Thread currThread = Thread.currentThread();
        final long startTime = System.currentTimeMillis();

        synchronized (this) {
            try {
                while (true) {
                    if (owner == currThread) {
                        nWriters += 1;
                        return true;
                    } else if (nWriters == 0) {
                        if (myStorage == null || myStorage.lockObject(this)) {
                            nReaders += 1;
                        }
                        return true;
                    } else {
                        long currTime = System.currentTimeMillis();
                        if (currTime < startTime) {
                            currTime = startTime;
                        }
                        if (startTime + timeout <= currTime) {
                            return false;
                        }
                        wait(startTime + timeout - currTime);
                    }
                }
            } catch (final InterruptedException x) {
                return false;
            }
        }
    }

    @Override
    public synchronized void unlock() {
        if (nWriters != 0) {
            if (--nWriters == 0) {
                owner = null;
                notifyAll();
            }
        } else if (nReaders != 0) {
            if (--nReaders == 0) {
                notifyAll();
            }
        }
    }

}
