package edu.washington.cs.cse490h.lib;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Class to handle logging of Synoptic events. This class used in both
 * simulation and emulation modes.
 */
public class SynopticLogger {
	
	private BufferedWriter writer = null;
	private String filename = "";

	/**
	 * Opens the log file and sets up logging state.
	 */
	public void start(String filename) {
		if (filename == "") { 
			return;
		}
		
		this.filename = filename;
		
		try {
			// TODO: fail if the file exists
			this.writer = new BufferedWriter(new FileWriter(filename));
		} catch (IOException e) {
			System.out.println("Warning: unable to open logfile '" + this.filename+ "' for writing.");
			e.printStackTrace();
			System.out.println("...continuing");
		}
	}

	
	/**
	 * Closes the log file and tears down logging state.
	 */
	public void stop() {
		if (this.writer == null) {
			return;
		}
		
		try {
			this.writer.close();
		} catch (IOException e) {
			System.out.println("Warning: unable to close logfile '" + this.filename + "'.");
			e.printStackTrace();
			System.out.println("...continuing");
		}
	}
	
	
	/**
	 * Logs a single event to the synoptic log.
	 * 
	 * @param timeString
	 * @param node
	 * @param eventString
	 */
	public void logEvent(String timeStr, Node node, String eventStr) {
		if (this.writer == null) {
			return;
		}
		
		try {
			this.writer.write(timeStr + " " + node.toSynopticString() + " " + eventStr + "\n");
		} catch (IOException e) {
			System.out.println("Warning: unable to write to the synoptic log.");
			e.printStackTrace();
			System.out.println("...continuing");
		}
	}

}
