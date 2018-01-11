
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

public class OSFile implements IFile {

    protected RandomAccessFile file;

    protected boolean noFlush;

    private FileLock lck;

    public OSFile(final String filePath, final boolean readOnly, final boolean noFlush) {
        this.noFlush = noFlush;
        try {
            file = new RandomAccessFile(filePath, readOnly ? "r" : "rw");
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public void close() {
        try {
            file.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public long length() {
        try {
            return file.length();
        } catch (final IOException x) {
            return -1;
        }
    }

    @Override
    public void lock(final boolean shared) {
        try {
            lck = file.getChannel().lock(0, Long.MAX_VALUE, shared);
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    /*
     * JDK 1.3 and older public static boolean lockFile(RandomAccessFile file, boolean shared) { try { Class cls =
     * file.getClass(); Method getChannel = cls.getMethod("getChannel", new Class[0]); if (getChannel != null) {
     * Object channel = getChannel.invoke(file, new Object[0]); if (channel != null) { cls = channel.getClass();
     * Class[] paramType = new Class[3]; paramType[0] = Long.TYPE; paramType[1] = Long.TYPE; paramType[2] =
     * Boolean.TYPE; Method lock = cls.getMethod("lock", paramType); if (lock != null) { Object[] param = new
     * Object[3]; param[0] = new Long(0); param[1] = new Long(Long.MAX_VALUE); param[2] = new Boolean(shared); return
     * lock.invoke(channel, param) != null; } } } } catch (Exception x) {} return true; }
     */

    @Override
    public int read(final long pos, final byte[] buf) {
        try {
            file.seek(pos);
            return file.read(buf, 0, buf.length);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public void sync() {
        if (!noFlush) {
            try {
                file.getFD().sync();
            } catch (final IOException x) {
                throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
            }
        }
    }

    @Override
    public boolean tryLock(final boolean shared) {
        try {
            lck = file.getChannel().tryLock(0, Long.MAX_VALUE, shared);
            return lck != null;
        } catch (final IOException x) {
            return true;
        }
    }

    @Override
    public void unlock() {
        try {
            lck.release();
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    @Override
    public void write(final long pos, final byte[] buf) {
        try {
            file.seek(pos);
            file.write(buf, 0, buf.length);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }
}
