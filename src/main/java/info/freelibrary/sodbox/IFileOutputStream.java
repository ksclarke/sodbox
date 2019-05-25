
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.OutputStream;

import info.freelibrary.sodbox.impl.Page;

/**
 * Stream class implementation on top of Sodbox file. Can be used in Storage.Backup method.
 */
public class IFileOutputStream extends OutputStream {

    IFile myFile;

    long myCurrentPosition;

    byte[] myPage;

    /**
     * Creates a file output stream.
     *
     * @param aFile An output file
     */
    public IFileOutputStream(final IFile aFile) {
        myFile = aFile;
        myPage = new byte[Page.PAGE_SIZE];
    }

    @Override
    public void write(final int aByte) throws IOException {
        final byte[] bytes = new byte[1];
        bytes[0] = (byte) aByte;
        write(bytes, 0, 1);
    }

    @Override
    public void write(final byte aBytes[], final int aSrcOffset, final int aLength) throws IOException {
        int srcOffset = aSrcOffset;
        int length = aLength;

        while (length > 0) {
            final int quant;
            int dstOffset = (int) (myCurrentPosition % Page.PAGE_SIZE);

            quant = myPage.length - dstOffset > length ? length : myPage.length - dstOffset;
            System.arraycopy(aBytes, srcOffset, myPage, dstOffset, quant);
            srcOffset += quant;
            dstOffset += quant;
            myCurrentPosition += quant;
            length -= quant;

            if (dstOffset == myPage.length) {
                myFile.write(myCurrentPosition - dstOffset, myPage);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        final int dstOff = (int) (myCurrentPosition % Page.PAGE_SIZE);
        myFile.write(myCurrentPosition - dstOff, myPage);
    }

    @Override
    public void close() throws IOException {
        flush();
        myFile.close();
    }
}
