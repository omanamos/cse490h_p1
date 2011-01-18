package edu.washington.cs.cse490h.lib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;

import plume.Option;
import plume.Options;
import plume.OptionGroup;

import edu.washington.cs.cse490h.lib.Manager.FailureLvl;

/**
 * <pre>
 * 
 * Class with main method that starts up a Manager. Either an Emulator or a Simulator
 *
 * Usage: java MessageLayer [options]
 * General Options:
 *  -h --help=<boolean>                               - Print usage message [default false]
 *  -v --version=<boolean>                            - Print program version [default false]
 *
 * Execution Options:
 *  -s --simulate=<boolean>                           - Simulate [default false]
 *  -e --emulate=<boolean>                            - Emulate [default false]
 *  -n --nodeClass=<string>                           - Node class to use [default ]
 *  --routerHostname=<string>                         - Router hostname [default localhost]
 *  --routerPort=<int>                                - Router port [default -1]
 *  -t --timestep=<long>                              - Time step, in ms [default 1000]
 *  -r --seed=<long>                                  - Random seed
 *  -c --commandFile=<string>                         - Command file [default ]
 *  -f --failureLvlInt=<int>                          - Failure level, a number between 0 and 4 [default 4]
 *
 * Debugging Options:
 *  -L --synopticTotallyOrderedLogFilename=<string>   - Synoptic totally ordered log filename [default ]
 *  -l --synopticPartiallyOrderedLogFilename=<string> - Synoptic partially ordered log filename [default ]
 *  -o --replayOutputFilename=<string>                - Replay output filename [default ]
 *  --replayInputFilename=<string>                    - Replay input filename [default ]
 *
 * </pre>   
 */
public class MessageLayer {

	/**
	 * The current version.
	 */
	public static final String versionString = "v0.2";

	////////////////////////////////////////////////////
	/**
	 * Print the usage message.
	 */
	@OptionGroup("General Options")
	@Option(value="-h Print usage message", aliases={"-help"})
	public static boolean help = false;

	/**
	 * Print the current version.
	 */
	@Option(value="-v Print program version", aliases={"-version"})
	public static boolean version = false;
	// end option group "General Options"


	////////////////////////////////////////////////////
	/**
	 * Perform simulation
	 */
	@OptionGroup("Execution Options")
	@Option(value="-s Simulate", aliases={"-simulate"})
	public static boolean simulate = false;

	/**
	 * Perform emulation
	 */
	@Option(value="-e Emulate", aliases={"-emulate"})
	public static boolean emulate = false;

	/**
	 * Node class to use for simulation\emulation
	 */
	@Option(value="-n Node class to use", aliases={"-node-cls"})
	public static String nodeClass = "";

	/**
	 * Router hostname
	 */
	@Option(value="Router hostname", aliases={"-router-host"})
	public static String routerHostname = "localhost";

	/**
	 * Router port
	 */
	@Option(value="Router port", aliases={"-router-port"})
	// TODO: specify a sane default    
	public static int routerPort = -1;

	/**
	 * Time step
	 */
	@Option(value = "-t Time step, in ms", aliases = { "-time-step" })
	public static long timestep = 1000;

	/**
	 * Seed to use
	 */
	@Option(value="-r Random seed", aliases={"-rand-seed"})
	public static Long seed = null;

	/**
	 * Command file
	 */
	@Option(value="-c Command file", aliases={"-command-file"})
	public static String commandFile = "";

	/**
	 * Failure level setting
	 */
	@Option(value="-f Failure level, a number between 0 and 4", aliases={"-failure-lvl"})
	public static int failureLvlInt = 4;
	// end option group "Execution Options"


	////////////////////////////////////////////////////
	/**
	 * The log filename for totally ordered synoptic output
	 */
	@OptionGroup("Debugging Options")
	@Option(value="-L Synoptic totally ordered log filename", aliases={"-synoptic-totally-ordered-logfile"})
	// TODO: specify a sane default
	public static String synopticTotalOrderLogFilename = "";
	
	/**
	 * The log filename for partially ordered synoptic output
	 */
	@Option(value="-l Synoptic partially ordered log filename", aliases={"-synoptic-partially-ordered-logfile"})
	// TODO: specify a sane default
	public static String synopticPartialOrderLogFilename = "";
	
	/**
	 * The log filename for replay output
	 */
	@Option(value="-o Replay output filename", aliases={"-replay-outfile"})
	public static String replayOutputFilename = "";
	
	/**
	 * The log filename for replay input
	 */
	@Option(value="Replay input filename", aliases={"-replay-infile"})
	public static String replayInputFilename = "";
	// end option group "Debugging Options"


	/** One line synopsis of usage */
	private static String usage_string
	= "java MessageLayer [options]";


	/**
	 * Prints out an warning message
	 * 
	 * @param msg warning msg string
	 */
	public static void printWarning(String msg) {
		System.err.println("Warning: " + msg);
	}

	/**
	 * Prints out an error message
	 * 
	 * @param msg error msg string
	 */
	public static void printError(String msg) {
		System.err.println("Error: " + msg);
	}


	/**
	 * The main method. Entry point to start a Manager
	 */
	public static void main(String[] args) {
		// this directly sets the static member options of the Main class
		Options options = new Options (usage_string, MessageLayer.class);
		
		@SuppressWarnings("unused")
		String[] cmdLineArgs = options.parse_or_usage(args);

		if (help) {
			options.print_usage();
			return;
		}

		if (version) {
			System.out.println(MessageLayer.versionString);
			return;
		}

		if (!simulate && !emulate) {
			printError("you must specify either -s or -e.");
			return;
		}

		if (nodeClass.equals("")) {
			printError("you must specify a node class with -n.");
			return;
		}

		if (synopticTotalOrderLogFilename.equals("")) {
			//printWarning("you did not specify a totally ordered synoptic log file.");	// TODO: re-enable when it's working
		} else {
			System.out.println("synopticTotalLogFilename = " + synopticTotalOrderLogFilename);
		}
		
		if (synopticPartialOrderLogFilename.equals("")) {
			//printWarning("you did not specify a partially ordered synoptic log file.");	// TODO: re-enable when it's working
		} else {
			System.out.println("synopticPartialLogFilename = " + synopticPartialOrderLogFilename);
		}

		FailureLvl failureLvl = FailureLvl.EVERYTHING;
		if (failureLvlInt == -1) {
			printWarning("you did not specify a failure level with -failure-lvl. Using failure-lvl 4 = EVERYTHING");
		} else {
			FailureLvl[] possibleFailureLvls = {
					FailureLvl.NOTHING,    // 0
					FailureLvl.CRASH,      // 1
					FailureLvl.DROP,       // 2
					FailureLvl.DELAY,      // 3
					FailureLvl.EVERYTHING, // 4
			};
			failureLvl = possibleFailureLvls[failureLvlInt];	
		}

		try {
			Manager manager = null;

			Class<? extends Node> nodeImpl = ClassLoader.getSystemClassLoader().loadClass(nodeClass).asSubclass(Node.class);

			if (simulate) {
				if(commandFile.equals("")) {
					// Simulation replay only really needs to record user input
					if (replayOutputFilename.equals("") && replayInputFilename.equals("")) {
						printWarning("You did not specify a replay input or output file for a user input simulation");
					} else if (replayOutputFilename.equals(replayInputFilename)) {
						printError("Replay output and input files are equal");
						return;
					} else if (!replayOutputFilename.equals("") && !replayInputFilename.equals("")) {
						printWarning("Both replay output and input files are specified");
					} else if (!replayInputFilename.equals("") && seed != null) {
						printWarning("Both seed and replay input are specified.  Seed will be ignored.");
					}
				}

				try {
					if(!commandFile.equals("")){
						manager = new Simulator(nodeImpl, failureLvl, seed, replayOutputFilename, replayInputFilename, commandFile);
					} else {
						manager = new Simulator(nodeImpl, failureLvl, seed, replayOutputFilename, replayInputFilename);
					}
				} catch (IllegalArgumentException e) {
					printError("Illegal arguments given to Simulator. Exception: " + e);
					return;
				} catch (FileNotFoundException e) {
					printError("Incorrect command file name given to Simulator. Exception: " + e);		    
					return;
				}


			} else { //emulate
				if (routerHostname == "" || routerPort == -1) {
					printError("For an emulation you must specify router hostname/port");
					return;
				}

				if (replayOutputFilename.equals("") && replayInputFilename.equals("")) {
					printWarning("You did not specify a replay input or output file");
				} else if (replayOutputFilename.equals(replayInputFilename)) {
					printError("Replay output and input files are equal");
					return;
				} else if (!replayOutputFilename.equals("") && !replayInputFilename.equals("")) {
					printWarning("Both replay output and input files are specified");
				} else if (!replayInputFilename.equals("") && seed != null) {
					printWarning("Both seed and replay input are specified.  Seed will be ignored.");
				}

				try {
					if (!commandFile.equals("")) {
						manager = new Emulator(nodeImpl, routerHostname, routerPort, failureLvl, seed, timestep, replayOutputFilename, replayInputFilename, commandFile);
					} else {
						manager = new Emulator(nodeImpl, routerHostname, routerPort, failureLvl, seed, timestep, replayOutputFilename, replayInputFilename);
					}
				} catch(UnknownHostException e) {
					printError("Router host name is unkown! Exception: " + e);
					return;
				} catch(IOException e) {
					printError("Encountered exception while trying to connect to Router. Exception " + e);
					return;
				} catch(IllegalArgumentException e) {
					printError("Illegal arguments given to Emulator. Exception: " + e);
					return;
				}
			}

			manager.start();

		} catch(Exception e) {
			printError("Exception occured in MessageLayer!! Exception: " + e);
			e.printStackTrace();
		}
	}
}
