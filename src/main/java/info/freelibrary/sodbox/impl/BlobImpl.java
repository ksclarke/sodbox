
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.RandomAccessInputStream;
import info.freelibrary.sodbox.RandomAccessOutputStream;
import info.freelibrary.sodbox.Storage;

public class BlobImpl extends PersistentResource implements Blob {

    static final int HEADER_SIZE = ObjectHeader.SIZE_OF + 3 * 4;

    int mySize;

    BlobImpl myNext;

    byte[] myBody;

    transient int myUsed;

    BlobImpl(final Storage aStorage, final int aSize) {
        super(aStorage);
        myBody = new byte[aSize];
    }

    BlobImpl() {
    }

    void discard(final int aFlags) {
        if (--myUsed == 0 && (aFlags & ENABLE_SEGMENT_CACHING) == 0) {
            invalidate();
            myNext = null;
        }
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    @Override
    public RandomAccessInputStream getInputStream() {
        return getInputStream(0);
    }

    @Override
    public RandomAccessInputStream getInputStream(final int aFlags) {
        return new BlobInputStream(this, aFlags);
    }

    @Override
    public RandomAccessOutputStream getOutputStream() {
        return getOutputStream(APPEND);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final boolean aMultisession) {
        return getOutputStream(aMultisession ? APPEND : TRUNCATE_LAST_SEGMENT | APPEND);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final long aPosition, final boolean aMultisession) {
        final RandomAccessOutputStream stream = getOutputStream(aMultisession);

        stream.setPosition(aPosition);

        return stream;
    }

    @Override
    public RandomAccessOutputStream getOutputStream(final int aFlags) {
        return new BlobOutputStream(this, aFlags);
    }

    @Override
    public void deallocate() {
        load();

        if (mySize > 0) {
            BlobImpl current = myNext;

            while (current != null) {
                final BlobImpl tail;

                current.load();
                tail = current.myNext;
                current.deallocate();
                current = tail;
            }
        }

        super.deallocate();
    }

    static class BlobInputStream extends RandomAccessInputStream {

        protected BlobImpl myCurrent;

        protected BlobImpl myFirst;

        protected int myPosition;

        protected int myBlobOffs;

        protected int myFlags;

        protected BlobInputStream(final BlobImpl aFirst, final int aFlags) {
            myFlags = aFlags;
            myFirst = aFirst;
            aFirst.load();
            myCurrent = aFirst;
            aFirst.myUsed += 1;
        }

        @Override
        public int read() {
            final byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        @Override
        public int read(final byte[] aBytes, final int aOffset, final int aLength) {
            int offset = aOffset;
            int length = aLength;

            if (myPosition >= myFirst.mySize) {
                return -1;
            }

            final int rest = myFirst.mySize - myPosition;

            if (length > rest) {
                length = rest;
            }

            final int rc = length;

            while (length > 0) {
                if (myBlobOffs == myCurrent.myBody.length) {
                    final BlobImpl previous = myCurrent;

                    myCurrent = previous.myNext;
                    myCurrent.load();
                    myCurrent.myUsed += 1;

                    if (previous != myFirst) {
                        previous.discard(myFlags);
                    }

                    myBlobOffs = 0;
                }

                final int n = length > myCurrent.myBody.length - myBlobOffs ? myCurrent.myBody.length - myBlobOffs
                        : length;

                System.arraycopy(myCurrent.myBody, myBlobOffs, aBytes, offset, n);

                myBlobOffs += n;
                offset += n;
                length -= n;
                myPosition += n;
            }

            return rc;
        }

        @Override
        public long setPosition(final long aPosition) {
            if (aPosition < myPosition) {
                if (aPosition >= myPosition - myBlobOffs) {
                    myBlobOffs -= myPosition - aPosition;
                    return myPosition = (int) aPosition;
                }

                if (myFirst != myCurrent) {
                    myCurrent.discard(myFlags);
                    myCurrent = myFirst;
                }

                myPosition = 0;
                myBlobOffs = 0;
            }

            skip(aPosition - myPosition);
            return myPosition;
        }

        @Override
        public long getPosition() {
            return myPosition;
        }

        @Override
        public long size() {
            return myFirst.mySize;
        }

        @Override
        public long skip(final long aOffset) {
            final int rest = myFirst.mySize - myPosition;

            long offset = aOffset;

            if (offset > rest) {
                offset = rest;
            }

            int len = (int) offset;

            while (len > 0) {
                if (myBlobOffs == myCurrent.myBody.length) {
                    final BlobImpl prev = myCurrent;

                    myCurrent = prev.myNext;
                    myCurrent.load();
                    myCurrent.myUsed += 1;

                    if (prev != myFirst) {
                        prev.discard(myFlags);
                    }

                    myBlobOffs = 0;
                }

                final int n = len > myCurrent.myBody.length - myBlobOffs ? myCurrent.myBody.length - myBlobOffs : len;

                myPosition += n;
                len -= n;
                myBlobOffs += n;
            }

            return offset;
        }

        @Override
        public int available() {
            return myFirst.mySize - myPosition;
        }

        @Override
        public void close() {
            myCurrent.discard(myFlags);

            if (myFirst != myCurrent) {
                myFirst.discard(myFlags);
            }

            myCurrent = myFirst = null;
        }

    }

    static class BlobOutputStream extends RandomAccessOutputStream {

        protected BlobImpl myFirst;

        protected BlobImpl myCurrent;

        protected int myPosition;

        protected int myBlobOffs;

        protected int myFlags;

        protected boolean isModified;


        BlobOutputStream(final BlobImpl aFirst, final int aFlags) {
            myFlags = aFlags;
            myFirst = aFirst;
            aFirst.load();
            aFirst.myUsed += 1;
            myCurrent = aFirst;

            if ((aFlags & APPEND) != 0) {
                skip(aFirst.mySize);
            }
        }

        @Override
        public void write(final int aByte) {
            final byte[] bytes = new byte[1];

            bytes[0] = (byte) aByte;
            write(bytes, 0, 1);
        }

        @Override
        public void write(final byte[] aBytes, final int aOffset, final int aLength) {
            int offset = aOffset;
            int length = aLength;

            while (length > 0) {
                if (myBlobOffs == myCurrent.myBody.length) {
                    final BlobImpl previous = myCurrent;

                    if (previous.myNext == null) {
                        int bodyLength = myCurrent.myBody.length;

                        if ((myFlags & DOUBLE_SEGMENT_SIZE) != 0 && bodyLength << 1 > bodyLength) {
                            bodyLength = (bodyLength + HEADER_SIZE << 1) - HEADER_SIZE;
                        }

                        final BlobImpl next = new BlobImpl(myCurrent.getStorage(), bodyLength);

                        myCurrent = previous.myNext = next;
                        isModified = true;
                    } else {
                        myCurrent = previous.myNext;
                        myCurrent.load();
                    }

                    myCurrent.myUsed += 1;

                    if (previous != myFirst) {
                        if (isModified) {
                            previous.store();
                        }

                        previous.discard(myFlags);
                    }

                    myBlobOffs = 0;
                }

                final int n = length > myCurrent.myBody.length - myBlobOffs ? myCurrent.myBody.length - myBlobOffs
                        : length;

                System.arraycopy(aBytes, offset, myCurrent.myBody, myBlobOffs, n);

                isModified = true;
                myBlobOffs += n;
                offset += n;
                length -= n;
                myPosition += n;
            }

            if (myPosition > myFirst.mySize) {
                myFirst.mySize = myPosition;
            }
        }

        @Override
        public void close() {
            if ((myFlags & TRUNCATE_LAST_SEGMENT) != 0 && myBlobOffs < myCurrent.myBody.length &&
                    myCurrent.myNext == null) {
                final byte[] tmp = new byte[myBlobOffs];

                System.arraycopy(myCurrent.myBody, 0, tmp, 0, myBlobOffs);

                myCurrent.myBody = tmp;
            }

            myCurrent.store();
            myCurrent.discard(myFlags);

            if (myCurrent != myFirst) {
                myFirst.store();
                myFirst.discard(myFlags);
            }

            myFirst = myCurrent = null;
        }

        @Override
        public long setPosition(final long aNewPosition) {
            if (aNewPosition < myPosition) {
                if (aNewPosition >= myPosition - myBlobOffs) {
                    myBlobOffs -= myPosition - aNewPosition;
                    return myPosition = (int) aNewPosition;
                }

                if (myFirst != myCurrent) {
                    if (isModified) {
                        myCurrent.store();
                        isModified = false;
                    }

                    myCurrent.discard(myFlags);
                    myCurrent = myFirst;
                }

                myPosition = 0;
                myBlobOffs = 0;
            }

            skip(aNewPosition - myPosition);

            return myPosition;
        }

        @Override
        public long getPosition() {
            return myPosition;
        }

        @Override
        public long size() {
            return myFirst.mySize;
        }

        public long skip(final long aOffset) {
            final int rest = myFirst.mySize - myPosition;

            long offset = aOffset;

            if (offset > rest) {
                offset = rest;
            }

            int length = (int) offset;

            while (length > 0) {
                if (myBlobOffs == myCurrent.myBody.length) {
                    final BlobImpl prev = myCurrent;

                    myCurrent = prev.myNext;
                    myCurrent.load();
                    myCurrent.myUsed += 1;

                    if (prev != myFirst) {
                        if (isModified) {
                            prev.store();
                            isModified = false;
                        }

                        prev.discard(myFlags);
                    }

                    myBlobOffs = 0;
                }

                final int n = length > myCurrent.myBody.length - myBlobOffs ? myCurrent.myBody.length - myBlobOffs
                        : length;

                myPosition += n;
                length -= n;
                myBlobOffs += n;
            }

            return offset;
        }
    }

}
