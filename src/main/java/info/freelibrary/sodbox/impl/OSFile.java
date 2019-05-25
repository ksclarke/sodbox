
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

public class OSFile implements IFile {

    protected RandomAccessFile myFile;

    protected boolean isNotFlushable;

    private FileLock myLock;

    /**
     * Creates an OSFile.
     *
     * @param aFilePath A file path
     * @param aReadOnly Whether the OSFile is read only
     * @param aNoFlush Whether the OSFile is flushable
     */
    public OSFile(final String aFilePath, final boolean aReadOnly, final boolean aNoFlush) {
        isNotFlushable = aNoFlush;

        try {
            myFile = new RandomAccessFile(aFilePath, aReadOnly ? "r" : "rw");
        } catch (final IOException details) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, details);
        }
    }

    @Override
    public void write(final long aPosition, final byte[] aBytes) {
        try {
            myFile.seek(aPosition);
            myFile.write(aBytes, 0, aBytes.length);
        } catch (final IOException details) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, details);
        }
    }

    @Override
    public int read(final long aPosition, final byte[] aBytes) {
        try {
            myFile.seek(aPosition);
            return myFile.read(aBytes, 0, aBytes.length);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public void sync() {
        if (!isNotFlushable) {
            try {
                myFile.getFD().sync();
            } catch (final IOException details) {
                throw new StorageError(StorageError.FILE_ACCESS_ERROR, details);
            }
        }
    }

    @Override
    public void close() {
        try {
            myFile.close();
        } catch (final IOException details) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, details);
        }
    }

    @Override
    public boolean tryLock(final boolean aSharedLock) {
        try {
            myLock = myFile.getChannel().tryLock(0, Long.MAX_VALUE, aSharedLock);
            return myLock != null;
        } catch (final IOException details) {
            return true;
        }
    }

    @Override
    public void lock(final boolean aSharedLock) {
        try {
            myLock = myFile.getChannel().lock(0, Long.MAX_VALUE, aSharedLock);
        } catch (final IOException details) {
            throw new StorageError(StorageError.LOCK_FAILED, details);
        }
    }

    @Override
    public void unlock() {
        try {
            myLock.release();
        } catch (final IOException details) {
            throw new StorageError(StorageError.LOCK_FAILED, details);
        }
    }

    @Override
    public long length() {
        try {
            return myFile.length();
        } catch (final IOException x) {
            return -1;
        }
    }

}
