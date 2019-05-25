
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

    /** A segment length */
    static final int SEGMENT_LENGTH = 128 * 1024;

    /** A Zip file */
    ZipFile myFile;

    /** Zip entries */
    ZipEntry[] myEntries;

    /** A segment */
    byte[] mySegment;

    /** The current Zip entry */
    ZipEntry myCurrentEntry;

    /**
     * Constructor of compressed file
     *
     * @param aPath path to the archive previously prepared by CompressDatabase utility
     */
    public CompressedFile(final String aPath) {
        try {
            myFile = new ZipFile(aPath);
            final int nEntries = myFile.size();
            myEntries = new ZipEntry[nEntries];
            final Enumeration<? extends ZipEntry> ee = myFile.entries();

            for (int i = 0; ee.hasMoreElements(); i++) {
                myEntries[i] = ee.nextElement();
            }

            mySegment = new byte[SEGMENT_LENGTH];
            myCurrentEntry = null;
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    @Override
    public void write(final long aPosition, final byte[] aBuffer) {
        throw new UnsupportedOperationException("ZipFile.write");
    }

    @Override
    public int read(final long aPosition, final byte[] aBuffer) {
        try {
            final int seg = (int) (aPosition / SEGMENT_LENGTH);
            final ZipEntry e = myEntries[seg];
            final int size = (int) e.getSize();

            if (e != myCurrentEntry) {
                final InputStream in = myFile.getInputStream(e);

                int offs = 0;
                int rc;

                while (offs < size && (rc = in.read(mySegment, offs, size - offs)) >= 0) {
                    offs += rc;
                }

                if (offs != size) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }

                myCurrentEntry = e;
            }

            final int offs = (int) (aPosition - (long) seg * SEGMENT_LENGTH);

            if (offs < size) {
                final int len = aBuffer.length < size - offs ? aBuffer.length : size - offs;
                System.arraycopy(mySegment, offs, aBuffer, 0, len);
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
        try {
            myFile.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    @Override
    public long length() {
        return (long) (myEntries.length - 1) * SEGMENT_LENGTH + myEntries[myEntries.length - 1].getSize();
    }

}
