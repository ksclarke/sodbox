
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Class using NIO mapping file on virtual mapping. Using this class instead standard OSFile can significantly
 * increase speed of application in some cases.
 */
public class MappedFile implements IFile {

    RandomAccessFile myFile;

    MappedByteBuffer myMap;

    FileChannel myChannel;

    long myMapSize;

    FileLock myLock;

    /**
     * Creates a mapped file.
     *
     * @param aFilePath A file to be mapped
     * @param aInitialSize An initial map size
     * @param aReadOnly Whether the map size
     */
    public MappedFile(final String aFilePath, final long aInitialSize, final boolean aReadOnly) {
        final FileChannel.MapMode mode;

        try {
            mode = aReadOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
            myFile = new RandomAccessFile(aFilePath, aReadOnly ? "r" : "rw");
            myChannel = myFile.getChannel();

            final long size = myChannel.size();

            myMapSize = aReadOnly || size > aInitialSize ? size : aInitialSize;
            myMap = myChannel.map(mode, 0, myMapSize);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    private void checkSize(final long aSize) throws IOException {
        if (aSize > myMapSize) {
            long newSize = myMapSize < Integer.MAX_VALUE / 2 ? myMapSize * 2 : Integer.MAX_VALUE;

            if (newSize < aSize) {
                newSize = aSize;
            }

            myMapSize = newSize;
            myMap = myChannel.map(FileChannel.MapMode.READ_WRITE, 0, // position
                    myMapSize);
        }
    }

    @Override
    public void write(final long aPosition, final byte[] aBuffer) {
        try {
            checkSize(aPosition + aBuffer.length);
            myMap.position((int) aPosition);
            myMap.put(aBuffer, 0, aBuffer.length);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public int read(final long aPosition, final byte[] aBuffer) {
        if (aPosition >= myMapSize) {
            return 0;
        }

        myMap.position((int) aPosition);
        myMap.get(aBuffer, 0, aBuffer.length);

        return aBuffer.length;
    }

    @Override
    public void sync() {
        myMap.force();
    }

    @Override
    public void close() {
        try {
            myChannel.close();
            myFile.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public boolean tryLock(final boolean aShared) {
        try {
            myLock = myChannel.tryLock(0, Long.MAX_VALUE, aShared);

            return myLock != null;
        } catch (final IOException x) {
            return true;
        }
    }

    @Override
    public void lock(final boolean aShared) {
        try {
            myLock = myChannel.lock(0, Long.MAX_VALUE, aShared);
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    @Override
    public void unlock() {
        try {
            myLock.release();
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
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
