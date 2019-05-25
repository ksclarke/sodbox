
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.IFile;

// Rc4Cipher - the RC4 encryption method
//
// Copyright (C) 1996 by Jef Poskanzer <jef@acme.com>.  All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// Visit the ACME Labs Java page for up-to-date versions of this and other
// fine Java utilities: http://www.acme.com/java/

public class Rc4File implements IFile {

    private final IFile myFile;

    private byte[] myCipherBuffer;

    private byte[] myPattern;

    private long myLength;

    private byte[] myZeroPage;

    /**
     * Creates a Rc4File.
     *
     * @param aFilePath A path for the Rc4File
     * @param aReadOnly Whether the file is read only
     * @param aNoFlush Whether the file is flushable
     * @param aKey A key
     */
    public Rc4File(final String aFilePath, final boolean aReadOnly, final boolean aNoFlush, final String aKey) {
        myFile = new OSFile(aFilePath, aReadOnly, aNoFlush);
        myLength = myFile.length() & ~(Page.PAGE_SIZE - 1);
        setKey(aKey.getBytes());
    }

    /**
     * Creates a Rc4File.
     *
     * @param aFile A Rc4File
     * @param aKey A key
     */
    public Rc4File(final IFile aFile, final String aKey) {
        myFile = aFile;
        myLength = aFile.length() & ~(Page.PAGE_SIZE - 1);
        setKey(aKey.getBytes());
    }

    @Override
    public void write(final long aPosition, final byte[] aBytes) {
        if (aPosition > myLength) {
            if (myZeroPage == null) {
                myZeroPage = new byte[Page.PAGE_SIZE];
                crypt(myZeroPage, myZeroPage);
            }

            do {
                myFile.write(myLength, myZeroPage);
            } while ((myLength += Page.PAGE_SIZE) < aPosition);
        }

        if (aPosition == myLength) {
            myLength += Page.PAGE_SIZE;
        }

        crypt(aBytes, myCipherBuffer);
        myFile.write(aPosition, myCipherBuffer);
    }

    @Override
    public int read(final long aPosition, final byte[] aBytes) {
        if (aPosition < myLength) {
            final int read = myFile.read(aPosition, aBytes);

            crypt(aBytes, aBytes);

            return read;
        }

        return 0;
    }

    private void setKey(final byte[] aKey) {
        final byte[] state = new byte[256];

        for (int counter = 0; counter < 256; ++counter) {
            state[counter] = (byte) counter;
        }

        int index1 = 0;
        int index2 = 0;

        for (int counter = 0; counter < 256; ++counter) {
            final byte temp;

            index2 = aKey[index1] + state[counter] + index2 & 0xff;
            temp = state[counter];
            state[counter] = state[index2];
            state[index2] = temp;
            index1 = (index1 + 1) % aKey.length;
        }

        myPattern = new byte[Page.PAGE_SIZE];
        myCipherBuffer = new byte[Page.PAGE_SIZE];

        int x = 0;
        int y = 0;

        for (int index = 0; index < Page.PAGE_SIZE; index++) {
            final byte temp;

            x = x + 1 & 0xff;
            y = y + state[x] & 0xff;
            temp = state[x];
            state[x] = state[y];
            state[y] = temp;
            myPattern[index] = state[state[x] + state[y] & 0xff];
        }
    }

    private void crypt(final byte[] aClearText, final byte[] aCipherText) {
        for (int index = 0; index < aClearText.length; index++) {
            aCipherText[index] = (byte) (aClearText[index] ^ myPattern[index]);
        }
    }

    @Override
    public void close() {
        myFile.close();
    }

    @Override
    public boolean tryLock(final boolean aSharedLock) {
        return myFile.tryLock(aSharedLock);
    }

    @Override
    public void lock(final boolean aSharedLock) {
        myFile.lock(aSharedLock);
        myLength = myFile.length() & ~(Page.PAGE_SIZE - 1);
    }

    @Override
    public void unlock() {
        myFile.unlock();
    }

    @Override
    public void sync() {
        myFile.sync();
    }

    @Override
    public long length() {
        return myFile.length();
    }

}
