
package info.freelibrary.sodbox.impl;

import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.RandomAccessInputStream;
import info.freelibrary.sodbox.RandomAccessOutputStream;
import info.freelibrary.sodbox.Storage;

public class RandomAccessBlobImpl extends PersistentResource implements Blob {

    static final int CHUNK_SIZE = Page.PAGE_SIZE - ObjectHeader.SIZE_OF;

    long mySize;

    Index myChunks;

    RandomAccessBlobImpl(final Storage aStorage) {
        super(aStorage);
        myChunks = aStorage.createIndex(long.class, true);
    }

    RandomAccessBlobImpl() {
    }

    @Override
    public RandomAccessInputStream getInputStream() {
        return getInputStream(0);
    }

    @Override
    public RandomAccessInputStream getInputStream(final int aFlags) {
        return new BlobInputStream();
    }

    @Override
    public RandomAccessOutputStream getOutputStream() {
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final boolean aMultisession) {
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final long aPosition, final boolean aMultisession) {
        final RandomAccessOutputStream stream = getOutputStream(aMultisession);

        stream.setPosition(aPosition);

        return stream;
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final int aFlags) {
        return new BlobOutputStream(aFlags);
    }

    @Override
    public void deallocate() {
        final Iterator iterator = myChunks.iterator();

        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }

        myChunks.clear();

        super.deallocate();
    }

    static class Chunk extends Persistent {

        byte[] myBody;

        Chunk() {
        }

        Chunk(final Storage aStorage) {
            super(aStorage);

            myBody = new byte[CHUNK_SIZE];
        }
    }

    class BlobInputStream extends RandomAccessInputStream {

        protected Chunk myCurrentChunk;

        protected long myCurrentPosition;

        protected long myCurrentChunkPosition;

        protected Iterator myIterator;

        protected BlobInputStream() {
            setPosition(0);
        }

        @Override
        public int read() {
            final byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        @Override
        public int read(final byte[] aBytes, final int aOffset, final int aLength) {
            int length = aLength;
            int offset = aOffset;

            if (myCurrentPosition >= mySize) {
                return -1;
            }

            final long rest = mySize - myCurrentPosition;

            if (length > rest) {
                length = (int) rest;
            }

            final int read = length;

            while (length > 0) {
                if (myCurrentPosition >= myCurrentChunkPosition + CHUNK_SIZE) {
                    final Map.Entry entry = (Map.Entry) myIterator.next();

                    myCurrentChunkPosition = ((Long) entry.getKey()).longValue();
                    myCurrentChunk = (Chunk) entry.getValue();
                    Assert.that(myCurrentPosition < myCurrentChunkPosition + CHUNK_SIZE);
                }

                if (myCurrentPosition < myCurrentChunkPosition) {
                    int fill = length < myCurrentChunkPosition - myCurrentPosition ? length
                            : (int) (myCurrentChunkPosition - myCurrentPosition);

                    length -= fill;
                    myCurrentPosition += fill;

                    while (--fill >= 0) {
                        aBytes[offset++] = 0;
                    }
                }

                final int chunkOffs = (int) (myCurrentPosition - myCurrentChunkPosition);
                final int copy = length < CHUNK_SIZE - chunkOffs ? length : CHUNK_SIZE - chunkOffs;

                System.arraycopy(myCurrentChunk.myBody, chunkOffs, aBytes, offset, copy);

                length -= copy;
                offset += copy;
                myCurrentPosition += copy;
            }

            return read;
        }

        @Override
        public long setPosition(final long aNewPosition) {
            final Key key;

            if (aNewPosition < 0) {
                return -1;
            }

            myCurrentPosition = aNewPosition > mySize ? mySize : aNewPosition;
            key = new Key(myCurrentPosition / CHUNK_SIZE * CHUNK_SIZE);
            myIterator = myChunks.entryIterator(key, null, Index.ASCENT_ORDER);
            myCurrentChunkPosition = Long.MIN_VALUE;
            myCurrentChunk = null;

            return myCurrentPosition;
        }

        @Override
        public long getPosition() {
            return myCurrentPosition;
        }

        @Override
        public long size() {
            return mySize;
        }

        @Override
        public long skip(final long aOffset) {
            return setPosition(myCurrentPosition + aOffset);
        }

        @Override
        public int available() {
            return (int) (mySize - myCurrentPosition);
        }

        @Override
        public void close() {
            myCurrentChunk = null;
            myIterator = null;
        }

    }

    class BlobOutputStream extends RandomAccessOutputStream {

        protected Chunk myCurrentChunk;

        protected long myCurrentPosition;

        protected long myCurrentChunkPosition;

        protected Iterator myIterator;

        protected BlobOutputStream(final int aFlags) {
            setPosition((aFlags & APPEND) != 0 ? mySize : 0);
        }

        @Override
        public void write(final int aByte) {
            final byte[] buf = new byte[1];

            buf[0] = (byte) aByte;
            write(buf, 0, 1);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void write(final byte[] aBytes, final int aOffset, final int aLength) {
            int length = aLength;
            int offset = aOffset;

            while (length > 0) {
                boolean newChunk = false;

                if (myCurrentPosition >= myCurrentChunkPosition + CHUNK_SIZE) {
                    if (myIterator.hasNext()) {
                        final Map.Entry e = (Map.Entry) myIterator.next();

                        myCurrentChunkPosition = ((Long) e.getKey()).longValue();
                        myCurrentChunk = (Chunk) e.getValue();
                        Assert.that(myCurrentPosition < myCurrentChunkPosition + CHUNK_SIZE);
                    } else {
                        myCurrentChunk = new Chunk(getStorage());
                        myCurrentChunkPosition = myCurrentPosition / CHUNK_SIZE * CHUNK_SIZE;
                        newChunk = true;
                    }
                }

                if (myCurrentPosition < myCurrentChunkPosition) {
                    myCurrentChunk = new Chunk(getStorage());
                    myCurrentChunkPosition = myCurrentPosition / CHUNK_SIZE * CHUNK_SIZE;
                    newChunk = true;
                }

                final int chunkOffs = (int) (myCurrentPosition - myCurrentChunkPosition);
                final int copy = length < CHUNK_SIZE - chunkOffs ? length : CHUNK_SIZE - chunkOffs;

                System.arraycopy(aBytes, offset, myCurrentChunk.myBody, chunkOffs, copy);

                length -= copy;
                myCurrentPosition += copy;
                offset += copy;

                if (newChunk) {
                    myChunks.put(new Key(myCurrentChunkPosition), myCurrentChunk);
                    myIterator = myChunks.entryIterator(new Key(myCurrentChunkPosition + CHUNK_SIZE), null,
                            Index.ASCENT_ORDER);
                } else {
                    myCurrentChunk.modify();
                }
            }

            if (myCurrentPosition > mySize) {
                mySize = myCurrentPosition;
                modify();
            }
        }

        @Override
        public long setPosition(final long aNewPosition) {
            if (aNewPosition < 0) {
                return -1;
            }

            myCurrentPosition = aNewPosition;
            myIterator = myChunks.entryIterator(new Key(myCurrentPosition / CHUNK_SIZE * CHUNK_SIZE), null,
                    Index.ASCENT_ORDER);
            myCurrentChunkPosition = Long.MIN_VALUE;
            myCurrentChunk = null;

            return myCurrentPosition;
        }

        @Override
        public long getPosition() {
            return myCurrentPosition;
        }

        @Override
        public long size() {
            return mySize;
        }

        public long skip(final long aOffset) {
            return setPosition(myCurrentPosition + aOffset);
        }

        @Override
        public void close() {
            myCurrentChunk = null;
            myIterator = null;
        }
    }
}
