package edu.washington.cs.cse490h.lib;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Class that nodes use to write to persistent storage. This class can be used
 * exactly the same way as a BufferedWriter, and students should use only this
 * and the PersistentStorageReader to access disc
 * 
 * The key difference is that any action that modifies a file can potentially
 * crash before the modification is made. This is to model cases where the node
 * fails between calls to onReceive.
 * 
 * Note that ANY modification can cause a crash with equal probability, so
 * write('a'); write('b'); write('c'); newLine(); has a higher chance of causing
 * a crash than write("abc\n");
 */
public class PersistentStorageOutputStream extends FileOutputStream {
	private final File f;
	private final Node n;

	PersistentStorageOutputStream(Node n, File f, boolean append)
			throws IOException {
		super(f, append);
		this.n = n;
		this.f = f;
	}

	// methods for the file writer
	@Override
	public void write(byte[] b) throws IOException {
		n.handleDiskWriteEvent("write(b)", "b:" + n.storageBytesToString(b));

		super.write(b);
		super.flush();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		n.handleDiskWriteEvent("write(b, " + off + ", " + len + ")",
				"b:" + n.storageBytesToString(b) + " offset:" + off + " len:" + len);

		super.write(b, off, len);
		super.flush();
	}

	@Override
	public void write(int b) throws IOException {
		n.handleDiskWriteEvent("write(" + b + ")", "buf:" + b);

		super.write(b);
		super.flush();
	}

	public boolean delete() throws IOException {
		n.handleDiskWriteEvent("delete of" + f.getName(),
				"delete:" + f.getName());

		close();
		return f.delete();
	}
}
