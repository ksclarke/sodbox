
package info.freelibrary.sodbox.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StreamTokenizer;

import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.StorageError;

public class MultiFile implements IFile {

    MultiFileSegment mySegment[];

    long myFixedSize;

    int myCurrentSegment;

    boolean isNotFlushable;

    /**
     * Creates a multi-file.
     *
     * @param aSegmentPath A segment path
     * @param aSegmentSize A segment size
     * @param aReadOnly Whether file is read only
     * @param aNoFlush Whether a file is flushable
     */
    public MultiFile(final String[] aSegmentPath, final long[] aSegmentSize, final boolean aReadOnly,
            final boolean aNoFlush) {
        isNotFlushable = aNoFlush;
        mySegment = new MultiFileSegment[aSegmentPath.length];

        for (int index = 0; index < mySegment.length; index++) {
            final MultiFileSegment seg = new MultiFileSegment();

            seg.myFile = new OSFile(aSegmentPath[index], aReadOnly, aNoFlush);
            seg.mySize = aSegmentSize[index];
            myFixedSize += seg.mySize;
            mySegment[index] = seg;
        }

        myFixedSize -= mySegment[mySegment.length - 1].mySize;
        mySegment[mySegment.length - 1].mySize = Long.MAX_VALUE;
    }

    /**
     * Creates a MultiFile from the supplied segments.
     *
     * @param aSegments MultiFile segments
     */
    public MultiFile(final MultiFileSegment[] aSegments) {
        mySegment = aSegments;

        for (int index = 0; index < aSegments.length; index++) {
            myFixedSize += aSegments[index].mySize;
        }

        myFixedSize -= mySegment[mySegment.length - 1].mySize;
        mySegment[mySegment.length - 1].mySize = Long.MAX_VALUE;
    }

    /**
     * Creates a MultiFile.
     *
     * @param aFilePath A file path
     * @param aReadOnly Whether a MultiFile is read only
     * @param aNoFlush Whether a MultiFile is flushable
     */
    public MultiFile(final String aFilePath, final boolean aReadOnly, final boolean aNoFlush) {
        try {
            final StreamTokenizer in = new StreamTokenizer(new BufferedReader(new FileReader(aFilePath)));
            final File dir = new File(aFilePath).getParentFile();

            int token = in.nextToken();

            isNotFlushable = aNoFlush;
            mySegment = new MultiFileSegment[0];

            do {
                final MultiFileSegment segment = new MultiFileSegment();
                final MultiFileSegment[] newSegment;

                if (token != StreamTokenizer.TT_WORD && token != '"') {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Multifile segment name expected");
                }

                String path = in.sval;

                token = in.nextToken();

                if (token != StreamTokenizer.TT_EOF) {
                    if (token != StreamTokenizer.TT_NUMBER) {
                        throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Multifile segment size expected");
                    }

                    segment.mySize = (long) in.nval * 1024; // kilobytes
                    token = in.nextToken();
                }

                myFixedSize += segment.mySize;

                if (dir != null) {
                    File file = new File(path);

                    if (!file.isAbsolute()) {
                        file = new File(dir, path);
                        path = file.getPath();
                    }
                }

                segment.myFile = new OSFile(path, aReadOnly, aNoFlush);
                newSegment = new MultiFileSegment[mySegment.length + 1];

                System.arraycopy(mySegment, 0, newSegment, 0, mySegment.length);

                newSegment[mySegment.length] = segment;
                mySegment = newSegment;
            } while (token != StreamTokenizer.TT_EOF);

            myFixedSize -= mySegment[mySegment.length - 1].mySize;
            mySegment[mySegment.length - 1].mySize = Long.MAX_VALUE;
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    long seek(final long aPosition) {
        long position = aPosition;

        myCurrentSegment = 0;

        while (position >= mySegment[myCurrentSegment].mySize) {
            position -= mySegment[myCurrentSegment].mySize;
            myCurrentSegment += 1;
        }

        return position;
    }

    @Override
    public void write(final long aPosition, final byte[] aBytes) {
        final long position = seek(aPosition);

        mySegment[myCurrentSegment].myFile.write(position, aBytes);
    }

    @Override
    public int read(final long aPosition, final byte[] aBytes) {
        final long position = seek(aPosition);

        return mySegment[myCurrentSegment].myFile.read(position, aBytes);
    }

    @Override
    public void sync() {
        if (!isNotFlushable) {
            for (int index = mySegment.length; --index >= 0;) {
                mySegment[index].myFile.sync();
            }
        }
    }

    @Override
    public boolean tryLock(final boolean aSharedLock) {
        return mySegment[0].myFile.tryLock(aSharedLock);
    }

    @Override
    public void lock(final boolean aSharedLock) {
        mySegment[0].myFile.lock(aSharedLock);
    }

    @Override
    public void unlock() {
        mySegment[0].myFile.unlock();
    }

    @Override
    public void close() {
        for (int index = mySegment.length; --index >= 0;) {
            mySegment[index].myFile.close();
        }
    }

    @Override
    public long length() {
        return myFixedSize + mySegment[mySegment.length - 1].myFile.length();
    }

    public static class MultiFileSegment {

        IFile myFile;

        long mySize;

    }

}
