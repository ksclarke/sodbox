
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

    RandomAccessFile f;

    MappedByteBuffer map;

    FileChannel chan;

    long mapSize;

    FileLock lck;

    public MappedFile(final String filePath, final long initialSize, final boolean readOnly) {
        try {
            f = new RandomAccessFile(filePath, readOnly ? "r" : "rw");
            chan = f.getChannel();

            final long size = chan.size();

            mapSize = readOnly || size > initialSize ? size : initialSize;
            map = chan.map(readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, // position
                    mapSize);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    private final void checkSize(final long size) throws IOException {
        if (size > mapSize) {
            long newSize = mapSize < Integer.MAX_VALUE / 2 ? mapSize * 2 : Integer.MAX_VALUE;

            if (newSize < size) {
                newSize = size;
            }

            mapSize = newSize;
            map = chan.map(FileChannel.MapMode.READ_WRITE, 0, // position
                    mapSize);
        }
    }

    @Override
    public void close() {
        try {
            chan.close();
            f.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public long length() {
        try {
            return f.length();
        } catch (final IOException x) {
            return -1;
        }
    }

    @Override
    public void lock(final boolean shared) {
        try {
            lck = chan.lock(0, Long.MAX_VALUE, shared);
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    @Override
    public int read(final long pos, final byte[] buf) {
        if (pos >= mapSize) {
            return 0;
        }

        map.position((int) pos);
        map.get(buf, 0, buf.length);

        return buf.length;
    }

    @Override
    public void sync() {
        map.force();
    }

    @Override
    public boolean tryLock(final boolean shared) {
        try {
            lck = chan.tryLock(0, Long.MAX_VALUE, shared);

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
            checkSize(pos + buf.length);
            map.position((int) pos);
            map.put(buf, 0, buf.length);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

}
