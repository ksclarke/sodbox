
package info.freelibrary.sodbox.impl;

import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.GenericIndex;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.RandomAccessInputStream;
import info.freelibrary.sodbox.RandomAccessOutputStream;
import info.freelibrary.sodbox.Storage;

public class RandomAccessBlobImpl extends PersistentResource implements Blob {

    class BlobInputStream extends RandomAccessInputStream {

        protected Chunk currChunk;

        protected long currPos;

        protected long currChunkPos;

        protected Iterator iterator;

        protected BlobInputStream() {
            setPosition(0);
        }

        @Override
        public int available() {
            return (int) (size - currPos);
        }

        @Override
        public void close() {
            currChunk = null;
            iterator = null;
        }

        @Override
        public long getPosition() {
            return currPos;
        }

        @Override
        public int read() {
            final byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        @Override
        public int read(final byte b[], int off, int len) {
            if (currPos >= size) {
                return -1;
            }
            final long rest = size - currPos;
            if (len > rest) {
                len = (int) rest;
            }
            final int rc = len;
            while (len > 0) {
                if (currPos >= currChunkPos + CHUNK_SIZE) {
                    final Map.Entry e = (Map.Entry) iterator.next();
                    currChunkPos = ((Long) e.getKey()).longValue();
                    currChunk = (Chunk) e.getValue();
                    Assert.that(currPos < currChunkPos + CHUNK_SIZE);
                }
                if (currPos < currChunkPos) {
                    int fill = len < currChunkPos - currPos ? len : (int) (currChunkPos - currPos);
                    len -= fill;
                    currPos += fill;
                    while (--fill >= 0) {
                        b[off++] = 0;
                    }
                }
                final int chunkOffs = (int) (currPos - currChunkPos);
                final int copy = len < CHUNK_SIZE - chunkOffs ? len : CHUNK_SIZE - chunkOffs;
                System.arraycopy(currChunk.body, chunkOffs, b, off, copy);
                len -= copy;
                off += copy;
                currPos += copy;
            }
            return rc;
        }

        @Override
        public long setPosition(final long newPos) {
            if (newPos < 0) {
                return -1;
            }
            currPos = newPos > size ? size : newPos;
            iterator = chunks.entryIterator(new Key(currPos / CHUNK_SIZE * CHUNK_SIZE), null,
                    GenericIndex.ASCENT_ORDER);
            currChunkPos = Long.MIN_VALUE;
            currChunk = null;
            return currPos;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long skip(final long offs) {
            return setPosition(currPos + offs);
        }
    }

    class BlobOutputStream extends RandomAccessOutputStream {

        protected Chunk currChunk;

        protected long currPos;

        protected long currChunkPos;

        protected Iterator iterator;

        protected BlobOutputStream(final int flags) {
            setPosition((flags & APPEND) != 0 ? size : 0);
        }

        @Override
        public void close() {
            currChunk = null;
            iterator = null;
        }

        @Override
        public long getPosition() {
            return currPos;
        }

        @Override
        public long setPosition(final long newPos) {
            if (newPos < 0) {
                return -1;
            }
            currPos = newPos;
            iterator = chunks.entryIterator(new Key(currPos / CHUNK_SIZE * CHUNK_SIZE), null,
                    GenericIndex.ASCENT_ORDER);
            currChunkPos = Long.MIN_VALUE;
            currChunk = null;
            return currPos;
        }

        @Override
        public long size() {
            return size;
        }

        public long skip(final long offs) {
            return setPosition(currPos + offs);
        }

        @Override
        public void write(final byte b[], int off, int len) {
            while (len > 0) {
                boolean newChunk = false;
                if (currPos >= currChunkPos + CHUNK_SIZE) {
                    if (iterator.hasNext()) {
                        final Map.Entry e = (Map.Entry) iterator.next();
                        currChunkPos = ((Long) e.getKey()).longValue();
                        currChunk = (Chunk) e.getValue();
                        Assert.that(currPos < currChunkPos + CHUNK_SIZE);
                    } else {
                        currChunk = new Chunk(getStorage());
                        currChunkPos = currPos / CHUNK_SIZE * CHUNK_SIZE;
                        newChunk = true;
                    }
                }
                if (currPos < currChunkPos) {
                    currChunk = new Chunk(getStorage());
                    currChunkPos = currPos / CHUNK_SIZE * CHUNK_SIZE;
                    newChunk = true;
                }
                final int chunkOffs = (int) (currPos - currChunkPos);
                final int copy = len < CHUNK_SIZE - chunkOffs ? len : CHUNK_SIZE - chunkOffs;
                System.arraycopy(b, off, currChunk.body, chunkOffs, copy);
                len -= copy;
                currPos += copy;
                off += copy;
                if (newChunk) {
                    chunks.put(new Key(currChunkPos), currChunk);
                    iterator = chunks.entryIterator(new Key(currChunkPos + CHUNK_SIZE), null,
                            GenericIndex.ASCENT_ORDER);
                } else {
                    currChunk.modify();
                }
            }
            if (currPos > size) {
                size = currPos;
                modify();
            }
        }

        @Override
        public void write(final int b) {
            final byte[] buf = new byte[1];
            buf[0] = (byte) b;
            write(buf, 0, 1);
        }
    }

    static class Chunk extends Persistent {

        byte[] body;

        Chunk() {
        }

        Chunk(final Storage db) {
            super(db);
            body = new byte[CHUNK_SIZE];
        }
    }

    static final int CHUNK_SIZE = Page.pageSize - ObjectHeader.sizeof;

    long size;

    Index chunks;

    RandomAccessBlobImpl() {
    }

    RandomAccessBlobImpl(final Storage storage) {
        super(storage);
        chunks = storage.createIndex(long.class, true);
    }

    @Override
    public void deallocate() {
        final Iterator iterator = chunks.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        chunks.clear();
        super.deallocate();
    }

    @Override
    public RandomAccessInputStream getInputStream() {
        return getInputStream(0);
    }

    @Override
    public RandomAccessInputStream getInputStream(final int flags) {
        return new BlobInputStream();
    }

    @Override
    public RandomAccessOutputStream getOutputStream() {
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final boolean multisession) {
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final int flags) {
        return new BlobOutputStream(flags);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final long position, final boolean multisession) {
        final RandomAccessOutputStream stream = getOutputStream(multisession);
        stream.setPosition(position);
        return stream;
    }
}