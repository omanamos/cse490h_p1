package edu.washington.cs.cse490h.lib;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * <pre>
 * CommandsParser -- parses file and keyboard commands
 * 
 * COMMANDS ARE CASE SENSITIVE.
 *
 * The command file and the keyboard input file have the same format;
 * all the same commands can be entered from either one.  Both
 * are line-oriented (one command per line). 
 * Nodes (e.g., a, b) are referred to by their virtual address (0..254).
 * Emulation and Simulation modes have different commands.  Emulation
 * does not include addresses, and this is denoted by the [n] notation.
 *
 *	[// | #] <comment>  -- any line starting with // or # is ignored
 *	time -- any subsequent command is delayed until the next time step
 *	fail [n] -- this crashes a node n
 *	start [n]  -- this restarts a node.
 *	echo text -- print the text 
 *	exit  -- cleanly stop the simulation/emulation run and print statistics
 *	[n] <msg>  -- deliver command <msg> to node n
 *		Note that msg cannot start with any keyword defined above
 * </pre>
 */

public abstract class CommandsParser {

	private String filename;
	private BufferedReader reader;

	protected CommandsParser() {
		this.filename = null;
		this.reader = null;
	}

	/**
	 * Open and process a command file.
	 * 
	 * @param filename
	 *            The name of the command file.
	 * @throws FileNotFoundException
	 *             If the named filed does not exist, is a directory rather than
	 *             a regular file, or for some other reason cannot be opened for
	 *             reading
	 */
	protected ArrayList<Event> parseFile(String filename) throws FileNotFoundException {
		if(filename == null) {
			throw new FileNotFoundException("null filename");
		}
		this.filename = filename;
		reader = new BufferedReader(new FileReader(filename));
		return parse();
	}

	/**
	 * Parse the command file.
	 */
	protected ArrayList<Event> parse(){
		try {
			ArrayList<Event> eventQueue = new ArrayList<Event>();
			String line;
			while((line = reader.readLine()) != null) {
				Event e = parseLine(line);
				if(e == null){
					System.err.println("not a valid line: " + line);
				}else{
					eventQueue.add(e);
				}
			}
			return eventQueue;
		}catch(IOException e) {
			System.err.println("IOException occured while trying to read file: " + filename + "\nException: " + e);
		}
		
		return null;
	}

	/**
	 * Process one line of the command file or keyboard input.
	 * 
	 * @param line
	 *            A command line.
	 * @return The Event specified by the line
	 */
	protected Event parseLine(String line) {
		line = line.trim();

		if (skipLine(line)) {
			return null;
		}

		String[] cmd = line.split("\\s+");
		Event e;

		if ((e = parseFail(cmd)) != null) {
			return e;
		}

		if ((e = parseStart(cmd)) != null) {
			return e;
		}

		return parseCommonCmds(cmd);
	}

	/******************** Protected Functions ********************/

	/**
	 * Command to exit the manager.
	 */
	protected Event exit(String[] cmd) {
		return Event.getExit();
	}

	/**
	 * Parse a node command.
	 * 
	 * @param cmd
	 *            The tokens of the command
	 * @return The event representing a node command
	 */
	protected abstract Event parseNodeCmd(String[] cmd);

	/**
	 * Returns true if line should be skipped, either because its empty or is a
	 * comment
	 */
	protected boolean skipLine(String line) {
		if(line.equals("")) {
			return true;
		}
		String[] cmd = line.split("\\s+");

		if(cmd[0].startsWith("//") || cmd[0].startsWith("#")) {
			return true;
		}
		return false;
	}

	/**
	 * Parses exit, time, echo and node commands
	 * 
	 * @param cmd
	 *            The tokens of the command
	 */
	protected Event parseCommonCmds(String[] cmd) {
		if(cmd[0].equals("exit")) {
			return exit(cmd);
		}

		if(cmd[0].equals("time")) {
			return time(cmd);
		}
		
		if(cmd[0].equals("echo")) {
			return echo(cmd);
		}
		
		return parseNodeCmd(cmd);
	}

	/**
	 * Prints a string array. For use specifically with the echo command.
	 * 
	 * @param strArray
	 *            The string array that is to be printed
	 * @param stream
	 *            The stream to print to
	 */
	protected void printStrArray(String[] strArray, PrintStream stream) {
		if(strArray == null || stream == null) {
			return;
		}
		for(int i = 0; i < strArray.length; i++) {
			stream.print(strArray[i] + " ");
		}
		stream.println();
	}

	/******************** Private Functions ********************/

	private Event parseFail(String[] cmd) {
		if(cmd[0].equals("fail")) {
			return Event.getFailure(Integer.parseInt(cmd[1]));
		}
		return null;
	}

	private Event parseStart(String[] cmd) {
		if(cmd[0].equals("start")) {
			return Event.getStart(Integer.parseInt(cmd[1]));
		}
		return null;
	}

	private Event time(String[] cmd) {
		return Event.getTime();
	}

	private Event echo(String[] cmd) {
		//printStrArray(cmd, 1, cmd.length, System.out);
		return Event.getEcho(cmd);
	}
}
