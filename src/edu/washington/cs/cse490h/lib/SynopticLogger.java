package edu.washington.cs.cse490h.lib;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Class to handle logging of Synoptic events. This class is used in both
 * simulation and emulation modes.
 */
public class SynopticLogger {
	private Writer writer = null;
	private String filename = "";

	/**
	 * Opens the log file and sets up logging state.
	 */
	public void start(String filename) {
		if (filename == null) {
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
	 * Sets up logging state and uses a custom writer for output.
	 *
	 * @param writer custom writer instance
	 */
	public void start(Writer writer) {
		this.writer = writer;
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
	public void logEvent(String timeStr, String eventStr) {
		if (this.writer == null) {
			return;
		}

		try {
			this.writer.write(timeStr + " " + eventStr + "\n");
		} catch (IOException e) {
			System.out.println("Warning: unable to write to the synoptic log.");
			e.printStackTrace();
			System.out.println("...continuing");
		}
	}

}
