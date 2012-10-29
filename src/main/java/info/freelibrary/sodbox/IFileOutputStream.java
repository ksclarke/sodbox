package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.OutputStream;

import info.freelibrary.sodbox.impl.Page;

/**
 * Stream class implementation on top of Sodbox file. Can be used in
 * Storage.Backup method.
 */
public class IFileOutputStream extends OutputStream {

	IFile file;
	long currPos;
	byte[] page;

	public void write(int b) throws IOException {
		byte[] bytes = new byte[1];
		bytes[0] = (byte) b;
		write(bytes, 0, 1);
	}

	public void write(byte b[], int srcOff, int len) throws IOException {
		while (len > 0) {
			int dstOff = (int) (currPos % Page.pageSize);
			int quant = page.length - dstOff > len ? len : page.length - dstOff;
			System.arraycopy(b, srcOff, page, dstOff, quant);
			srcOff += quant;
			dstOff += quant;
			currPos += quant;
			len -= quant;
			if (dstOff == page.length) {
				file.write(currPos - dstOff, page);
			}
		}
	}

	public void flush() throws IOException {
		int dstOff = (int) (currPos % Page.pageSize);
		file.write(currPos - dstOff, page);
	}

	public void close() throws IOException {
		flush();
		file.close();
	}

	public IFileOutputStream(IFile file) {
		this.file = file;
		page = new byte[Page.pageSize];
	}

}
