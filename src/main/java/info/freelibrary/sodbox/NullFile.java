
package info.freelibrary.sodbox;

/**
 * This implementation of <code>IFile</code> interface can be used to make Sodbox an main-memory database. It should
 * be used when pagePoolSize is set to <code>Storage.INFINITE_PAGE_POOL</code>. In this case all pages are cached in
 * memory and <code>NullFile</code> is used just as a stub. <code>NullFile</code> should be used only when data is
 * transient - i.e. it should not be saved between database sessions. If you need in-memory database but which provide
 * data persistence, you should use normal file and infinite page pool size.
 */
public class NullFile implements IFile {

    @Override
    public void write(final long aPosition, final byte[] aBuffer) {
    }

    @Override
    public int read(final long aPosition, final byte[] aBuffer) {
        return 0;
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean tryLock(final boolean aShared) {
        return true;
    }

    @Override
    public void lock(final boolean aShared) {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void close() {
    }

    @Override
    public long length() {
        return 0;
    }

}
