
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Compressed read-only database file. You should create database using normal file (OSFile). Then use
 * CompressDatabase utility to compress database file. To work with compressed database file you should pass instance
 * of this class in <code>Storage.open</code> method
 */
public class CompressedFile implements IFile {

    static final int SEGMENT_LENGTH = 128 * 1024;

    ZipFile file;

    ZipEntry[] entries;

    byte[] segment;

    ZipEntry currEntry;

    /**
     * Constructor of compressed file
     *
     * @param path path to the archive previously prepared by CompressDatabase utility
     */
    public CompressedFile(final String path) {
        try {
            file = new ZipFile(path);
            final int nEntries = file.size();
            entries = new ZipEntry[nEntries];
            final Enumeration<? extends ZipEntry> ee = file.entries();

            for (int i = 0; ee.hasMoreElements(); i++) {
                entries[i] = ee.nextElement();
            }

            segment = new byte[SEGMENT_LENGTH];
            currEntry = null;
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    @Override
    public void close() {
        try {
            file.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    @Override
    public long length() {
        return (long) (entries.length - 1) * SEGMENT_LENGTH + entries[entries.length - 1].getSize();
    }

    @Override
    public void lock(final boolean shared) {
    }

    @Override
    public int read(final long pos, final byte[] buf) {
        try {
            final int seg = (int) (pos / SEGMENT_LENGTH);
            final ZipEntry e = entries[seg];
            final int size = (int) e.getSize();

            if (e != currEntry) {
                final InputStream in = file.getInputStream(e);
                int rc, offs = 0;

                while (offs < size && (rc = in.read(segment, offs, size - offs)) >= 0) {
                    offs += rc;
                }

                if (offs != size) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }

                currEntry = e;
            }

            final int offs = (int) (pos - (long) seg * SEGMENT_LENGTH);
            if (offs < size) {
                final int len = buf.length < size - offs ? buf.length : size - offs;
                System.arraycopy(segment, offs, buf, 0, len);
                return len;
            }

            return 0;
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean tryLock(final boolean shared) {
        return true;
    }

    @Override
    public void unlock() {
    }

    @Override
    public void write(final long pos, final byte[] buf) {
        throw new UnsupportedOperationException("ZipFile.write");
    }

}
