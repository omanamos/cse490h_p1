package edu.washington.cs.cse490h.lib;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * File reader abstraction. This is basically a wrapped BufferedReader except it
 * will store things in the correct place. Students should only use the provided
 * classes to access the disc.
 */
public class PersistentStorageReader extends BufferedReader {
	private Node n;
	
	PersistentStorageReader(Node n, String filename)
			throws FileNotFoundException {
		super(new FileReader(Utility.realFilename(n.addr, filename)));
		this.n = n;
	}
	
	@Override
	public int read() throws IOException {
		n.handleDiskReadEvent("");
		
		return super.read();
	}
	
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		n.handleDiskReadEvent("cbuf:" + new String(cbuf) + " offset:"+ off + " len:" + len);
		
		return super.read(cbuf, off, len);
	}
	
	@Override
	public String readLine() throws IOException {
		n.handleDiskReadEvent("readline");
		
		return super.readLine();
	}
}
