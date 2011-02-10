package edu.washington.cs.cse490h.lib;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * File reader abstraction. This is basically a wrapped BufferedReader except it
 * will store things in the correct place. Students should only use the provided
 * classes to access the disc.
 */
public class PersistentStorageInputStream extends FileInputStream {
	private final Node n;

	PersistentStorageInputStream(Node n, String filename)
			throws FileNotFoundException {
		super(Utility.realFilename(n.addr, filename));
		this.n = n;
	}

	@Override
	public int read() throws IOException {
		int ret = super.read();
		byte[] bytes = new byte[] { (byte) ret };
		n.handleDiskReadEvent("b:" + n.storageBytesToString(bytes));
		return ret;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int ret = super.read(b, off, len);
		n.handleDiskReadEvent("b:" + n.storageBytesToString(b) + " offset:"
				+ off + " len:" + len);
		return ret;
	}

	@Override
	public int read(byte[] b) throws IOException {
		int ret = super.read(b);
		n.handleDiskReadEvent("b:" + n.storageBytesToString(b));
		return ret;
	}

	@Override
	public long skip(long skipN) throws IOException {
		n.handleDiskReadEvent("skipN:" + skipN);
		return super.skip(skipN);
	}
}
