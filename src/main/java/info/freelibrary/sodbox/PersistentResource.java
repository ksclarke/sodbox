
package info.freelibrary.sodbox;

/**
 * Base class for persistent capable objects supporting locking.
 */
public class PersistentResource extends Persistent implements IResource {

    private transient Thread myOwner;

    private transient int myNReaders;

    private transient int myNWriters;

    /**
     * Creates a persistent resource.
     */
    public PersistentResource() {
    }

    /**
     * Creates a persistent resource with the supplied storage.
     *
     * @param aStorage
     */
    public PersistentResource(final Storage aStorage) {
        super(aStorage);
    }

    @Override
    public synchronized void sharedLock() {
        final Thread currThread = Thread.currentThread();

        try {
            while (true) {
                if (myOwner == currThread) {
                    myNWriters += 1;
                    break;
                } else if (myNWriters == 0) {
                    if (myStorage == null || myStorage.lockObject(this)) {
                        myNReaders += 1;
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
    public boolean sharedLock(final long aTimeout) {
        final Thread currThread = Thread.currentThread();
        final long startTime = System.currentTimeMillis();

        synchronized (this) {
            try {
                while (true) {
                    if (myOwner == currThread) {
                        myNWriters += 1;
                        return true;
                    } else if (myNWriters == 0) {
                        if (myStorage == null || myStorage.lockObject(this)) {
                            myNReaders += 1;
                        }
                        return true;
                    } else {
                        long currTime = System.currentTimeMillis();
                        if (currTime < startTime) {
                            currTime = startTime;
                        }
                        if (startTime + aTimeout <= currTime) {
                            return false;
                        }
                        wait(startTime + aTimeout - currTime);
                    }
                }
            } catch (final InterruptedException x) {
                return false;
            }
        }
    }

    @Override
    public synchronized void exclusiveLock() {
        final Thread currThread = Thread.currentThread();

        try {
            while (true) {
                if (myOwner == currThread) {
                    myNWriters += 1;
                    break;
                } else if (myNReaders == 0 && myNWriters == 0) {
                    myNWriters = 1;
                    myOwner = currThread;
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
    public boolean exclusiveLock(final long aTimeout) {
        final Thread currThread = Thread.currentThread();
        final long startTime = System.currentTimeMillis();

        synchronized (this) {
            try {
                while (true) {
                    if (myOwner == currThread) {
                        myNWriters += 1;
                        return true;
                    } else if (myNReaders == 0 && myNWriters == 0) {
                        myNWriters = 1;
                        myOwner = currThread;
                        if (myStorage != null) {
                            myStorage.lockObject(this);
                        }
                        return true;
                    } else {
                        long currTime = System.currentTimeMillis();
                        if (currTime < startTime) {
                            currTime = startTime;
                        }
                        if (startTime + aTimeout <= currTime) {
                            return false;
                        }
                        wait(startTime + aTimeout - currTime);
                    }
                }
            } catch (final InterruptedException x) {
                return false;
            }
        }
    }

    @Override
    public synchronized void unlock() {
        if (myNWriters != 0) {
            if (--myNWriters == 0) {
                myOwner = null;
                notifyAll();
            }
        } else if (myNReaders != 0) {
            if (--myNReaders == 0) {
                notifyAll();
            }
        }
    }

    @Override
    public synchronized void reset() {
        if (myNWriters > 0) {
            myNWriters = 0;
            myNReaders = 0;
            myOwner = null;
        } else if (myNReaders > 0) {
            myNReaders -= 1;
        }

        notifyAll();
    }

}
