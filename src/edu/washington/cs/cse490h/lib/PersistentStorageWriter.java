package edu.washington.cs.cse490h.lib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

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
 * 
 */
//TODO: byte stream rather than character stream
public class PersistentStorageWriter extends BufferedWriter {
	private File f;
	private Node n;

	PersistentStorageWriter(Node n, File f, boolean append) throws IOException{
		super(new FileWriter(f, append));
		this.n = n;
		this.f = f;
	}
	
	private String logEscape(String s) {
		s = s.replace(" ", "_");
		s = s.replace("\n", "|");
		return "'" + "'";
	}

	// methods for the file writer
	@Override
	public void write(int c) throws IOException {
		n.handleDiskWriteEvent("write(" + c + ")",
				"buf:" + logEscape("" + c));
		
		super.write(c);
		super.flush();
	}

	@Override
	public void write(char[] cbuf, int off, int len) throws IOException {
		n.handleDiskWriteEvent("write(cbuf, " + off +  ", " + len + ")",
				"buf:" + logEscape(new String(cbuf)) + " offset:" + off + " len:" + len);
		
		super.write(cbuf, off, len);
		super.flush();
	}

	@Override
	public void write(String s, int off, int len) throws IOException {
		n.handleDiskWriteEvent("write(s, " + off +  ", " + len + ")",
				"buf:" + logEscape(s) + " offset:" + off + " len:" + len);
		
		super.write(s, off, len);
		super.flush();
	}

	@Override
	public void newLine() throws IOException {
		n.handleDiskWriteEvent("newLine()",
				"newline");
		
		super.newLine();
		super.flush();
	}

	@Override
	public void write(char[] cbuf) throws IOException {
		n.handleDiskWriteEvent("write(cbuf)",
				"buf:" + logEscape(new String(cbuf)));
		
		super.write(cbuf);
		super.flush();
	}

	@Override
	public Writer append(CharSequence csq) throws IOException {
		n.handleDiskWriteEvent("append(csq)",
				"append buf:" + logEscape("" + csq));
		
		Writer ret = super.append(csq);
		super.flush();
		
		return ret;
	}

	@Override
	public Writer append(CharSequence csq, int start, int end)
			throws IOException {
		n.handleDiskWriteEvent("append(csq, " + start + ", " + end + ")",
				"append buf:" + logEscape("" + csq) + " start:" + start + " end:" + end);
		
		Writer ret = super.append(csq, start, end);
		super.flush();
		
		return ret;
	}

	@Override
	public Writer append(char c) throws IOException{
		n.handleDiskWriteEvent("append(" + c + ")",
				"append buf:" + logEscape(String.valueOf(c)));
		
		Writer ret = super.append(c);
		super.flush();
		
		return ret;
	}

	@Override
	public void write(String str) throws IOException{
		n.handleDiskWriteEvent("write(str)",
				"buf:" + logEscape(str));
		
		super.write(str);
		super.flush();
	}

	public boolean delete() throws IOException{
		n.handleDiskWriteEvent("delete of" + f.getName(),
				"delete:" + f.getName());
		
		this.close();
		return f.delete();
	}
}
