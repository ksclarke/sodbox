
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.RandomAccessInputStream;
import info.freelibrary.sodbox.RandomAccessOutputStream;
import info.freelibrary.sodbox.Storage;

public class BlobImpl extends PersistentResource implements Blob {

    static class BlobInputStream extends RandomAccessInputStream {

        protected BlobImpl curr;

        protected BlobImpl first;

        protected int pos;

        protected int blobOffs;

        protected int flags;

        protected BlobInputStream(final BlobImpl first, final int flags) {
            this.flags = flags;
            this.first = first;
            first.load();
            curr = first;
            first.used += 1;
        }

        @Override
        public int available() {
            return first.size - pos;
        }

        @Override
        public void close() {
            curr.discard(flags);
            if (first != curr) {
                first.discard(flags);
            }
            curr = first = null;
        }

        @Override
        public long getPosition() {
            return pos;
        }

        @Override
        public int read() {
            final byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        @Override
        public int read(final byte b[], int off, int len) {
            if (pos >= first.size) {
                return -1;
            }
            final int rest = first.size - pos;
            if (len > rest) {
                len = rest;
            }
            final int rc = len;
            while (len > 0) {
                if (blobOffs == curr.body.length) {
                    final BlobImpl prev = curr;
                    curr = prev.next;
                    curr.load();
                    curr.used += 1;
                    if (prev != first) {
                        prev.discard(flags);
                    }
                    blobOffs = 0;
                }
                final int n = len > curr.body.length - blobOffs ? curr.body.length - blobOffs : len;
                System.arraycopy(curr.body, blobOffs, b, off, n);
                blobOffs += n;
                off += n;
                len -= n;
                pos += n;
            }
            return rc;
        }

        @Override
        public long setPosition(final long newPos) {
            if (newPos < pos) {
                if (newPos >= pos - blobOffs) {
                    blobOffs -= pos - newPos;
                    return pos = (int) newPos;
                }
                if (first != curr) {
                    curr.discard(flags);
                    curr = first;
                }
                pos = 0;
                blobOffs = 0;
            }
            skip(newPos - pos);
            return pos;
        }

        @Override
        public long size() {
            return first.size;
        }

        @Override
        public long skip(long offs) {
            final int rest = first.size - pos;
            if (offs > rest) {
                offs = rest;
            }
            int len = (int) offs;
            while (len > 0) {
                if (blobOffs == curr.body.length) {
                    final BlobImpl prev = curr;
                    curr = prev.next;
                    curr.load();
                    curr.used += 1;
                    if (prev != first) {
                        prev.discard(flags);
                    }
                    blobOffs = 0;
                }
                final int n = len > curr.body.length - blobOffs ? curr.body.length - blobOffs : len;
                pos += n;
                len -= n;
                blobOffs += n;
            }
            return offs;
        }
    }

    static class BlobOutputStream extends RandomAccessOutputStream {

        protected BlobImpl first;

        protected BlobImpl curr;

        protected int pos;

        protected int blobOffs;

        protected int flags;

        protected boolean modified;

        BlobOutputStream(final BlobImpl first, final int flags) {
            this.flags = flags;
            this.first = first;
            first.load();
            first.used += 1;
            curr = first;
            if ((flags & APPEND) != 0) {
                skip(first.size);
            }
        }

        @Override
        public void close() {
            if ((flags & TRUNCATE_LAST_SEGMENT) != 0 && blobOffs < curr.body.length && curr.next == null) {
                final byte[] tmp = new byte[blobOffs];
                System.arraycopy(curr.body, 0, tmp, 0, blobOffs);
                curr.body = tmp;
            }
            curr.store();
            curr.discard(flags);
            if (curr != first) {
                first.store();
                first.discard(flags);
            }
            first = curr = null;
        }

        @Override
        public long getPosition() {
            return pos;
        }

        @Override
        public long setPosition(final long newPos) {
            if (newPos < pos) {
                if (newPos >= pos - blobOffs) {
                    blobOffs -= pos - newPos;
                    return pos = (int) newPos;
                }
                if (first != curr) {
                    if (modified) {
                        curr.store();
                        modified = false;
                    }
                    curr.discard(flags);
                    curr = first;
                }
                pos = 0;
                blobOffs = 0;
            }
            skip(newPos - pos);
            return pos;
        }

        @Override
        public long size() {
            return first.size;
        }

        public long skip(long offs) {
            final int rest = first.size - pos;
            if (offs > rest) {
                offs = rest;
            }
            int len = (int) offs;
            while (len > 0) {
                if (blobOffs == curr.body.length) {
                    final BlobImpl prev = curr;
                    curr = prev.next;
                    curr.load();
                    curr.used += 1;
                    if (prev != first) {
                        if (modified) {
                            prev.store();
                            modified = false;
                        }
                        prev.discard(flags);
                    }
                    blobOffs = 0;
                }
                final int n = len > curr.body.length - blobOffs ? curr.body.length - blobOffs : len;
                pos += n;
                len -= n;
                blobOffs += n;
            }
            return offs;
        }

        @Override
        public void write(final byte b[], int off, int len) {
            while (len > 0) {
                if (blobOffs == curr.body.length) {
                    final BlobImpl prev = curr;
                    if (prev.next == null) {
                        int length = curr.body.length;
                        if ((flags & DOUBLE_SEGMENT_SIZE) != 0 && length << 1 > length) {
                            length = (length + headerSize << 1) - headerSize;
                        }
                        final BlobImpl next = new BlobImpl(curr.getStorage(), length);
                        curr = prev.next = next;
                        modified = true;
                    } else {
                        curr = prev.next;
                        curr.load();
                    }
                    curr.used += 1;
                    if (prev != first) {
                        if (modified) {
                            prev.store();
                        }
                        prev.discard(flags);
                    }
                    blobOffs = 0;
                }
                final int n = len > curr.body.length - blobOffs ? curr.body.length - blobOffs : len;
                System.arraycopy(b, off, curr.body, blobOffs, n);
                modified = true;
                blobOffs += n;
                off += n;
                len -= n;
                pos += n;
            }
            if (pos > first.size) {
                first.size = pos;
            }
        }

        @Override
        public void write(final int b) {
            final byte[] buf = new byte[1];
            buf[0] = (byte) b;
            write(buf, 0, 1);
        }
    }

    static final int headerSize = ObjectHeader.sizeof + 3 * 4;

    int size;

    BlobImpl next;

    byte[] body;

    transient int used;

    BlobImpl() {
    }

    BlobImpl(final Storage storage, final int size) {
        super(storage);
        body = new byte[size];
    }

    @Override
    public void deallocate() {
        load();
        if (size > 0) {
            BlobImpl curr = next;
            while (curr != null) {
                curr.load();
                final BlobImpl tail = curr.next;
                curr.deallocate();
                curr = tail;
            }
        }
        super.deallocate();
    }

    void discard(final int flags) {
        if (--used == 0 && (flags & ENABLE_SEGMENT_CACHING) == 0) {
            invalidate();
            next = null;
        }
    }

    @Override
    public RandomAccessInputStream getInputStream() {
        return getInputStream(0);
    }

    @Override
    public RandomAccessInputStream getInputStream(final int flags) {
        return new BlobInputStream(this, flags);
    }

    @Override
    public RandomAccessOutputStream getOutputStream() {
        return getOutputStream(APPEND);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final boolean multisession) {
        return getOutputStream(multisession ? APPEND : TRUNCATE_LAST_SEGMENT | APPEND);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final int flags) {
        return new BlobOutputStream(this, flags);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final long position, final boolean multisession) {
        final RandomAccessOutputStream stream = getOutputStream(multisession);
        stream.setPosition(position);
        return stream;
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }
}