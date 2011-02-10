package edu.washington.cs.cse490h.lib;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Abstract class defining generic routines for running network code under the
 * MessageLayer
 */
public abstract class Manager {
	protected static final int BROADCAST_ADDRESS = 255;
	protected static final int MAX_ADDRESS = 255;

	protected final double failureRate;
	protected final double recoveryRate;
	protected final double dropRate;
	protected final double delayRate;

    protected long seed;
    protected final Class<? extends Node> nodeImpl;

    // TODO: migrate to using Node.vtime instead of this once you figure out
	// how to embed vtime in Packet
    // Maps: node addr -> node's current vector time
	protected HashMap<Integer, VectorTime> vtimes;

	private int pktsSent;
	protected ArrayList<Event> sortedEvents;
	protected ArrayList<Timeout> waitingTOs;
	protected ArrayList<Packet> inTransitMsgs;
	protected CommandsParser parser;   // parser for commands file

	protected SynopticLogger synPartialOrderLogger = new SynopticLogger();

	protected FailureLvl userControl;
	protected enum FailureLvl{
		NOTHING,		// Everything is handled by the random number generator
		CRASH,			// The user only controls node crashes and restarts
		DROP,			// The user also controls message dropping
		DELAY,			// The user also controls message delays
		EVERYTHING		// The user controls everything, including message ordering
	}
	protected InputType cmdInputType;
	protected enum InputType{ USER, FILE }

	/**
	 * Class representing a timeout
	 */
	protected class Timeout{
		protected Node node;
		protected long fireTime;
		protected Callback cb;

		protected Timeout(Node node, long fireTime, Callback cb) {
			this.node = node;
			this.fireTime = fireTime;
			this.cb = cb;
		}

		public String toString() {
			return node.addr + ": " + cb + " at " + fireTime;
		}
	}

	private long time;

	/**
	 * Initialize Manager. Grabs all the relevant information from the students
	 * Node class, generates the seed, and initializes replay.
	 *
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param seed
	 *            Seed for the random number generator. Can be null to use the
	 *            current time as a seed
	 * @param replayOutputFilename
	 *            The log file for future relays of the current execution
	 * @param replayInputFilename
	 *            The log file to replay
	 * @throws IllegalArgumentException
	 *             If the arguments provided to the program are invalid
	 * @throws IOException
	 *             If creating the user input reader fails
	 */
	protected Manager(Class<? extends Node> nodeImpl, Long seed,
			String replayOutputFilename, String replayInputFilename)
			throws IllegalArgumentException, IOException {
		pktsSent = 0;
		waitingTOs = new ArrayList<Timeout>();
		inTransitMsgs = new ArrayList<Packet>();
		parser = null;

		this.nodeImpl = nodeImpl;
		try{
			// this block is not actually needed when the failure generator is
			// the user, but should work anyways
			failureRate = (Double)nodeImpl.getMethod("getFailureRate", (Class<?>[])null)
									.invoke(null, (Object[])null);
			recoveryRate = (Double)nodeImpl.getMethod("getRecoveryRate", (Class<?>[])null)
									.invoke(null, (Object[])null);
			dropRate = (Double)nodeImpl.getMethod("getDropRate", (Class<?>[])null)
									.invoke(null, (Object[])null);
			delayRate = (Double)nodeImpl.getMethod("getDelayRate", (Class<?>[])null)
									.invoke(null, (Object[])null);
		}catch(NoSuchMethodException e){
			throw new IllegalArgumentException("Error while finding get*rate functions: " + e);
		}catch(Exception e){
			throw new IllegalArgumentException("Error while executing get*rate functions: " + e);
		}

		Replay.parent = this;

		if(!replayOutputFilename.equals("")) {
			// initialize the replay output file
			File f = new File(replayOutputFilename);
			if (f.exists()) {
				throw new IllegalArgumentException("Replay output file already exists");
			}
			Replay.replayOut = new DataOutputStream(new FileOutputStream(replayOutputFilename));
		} else {
			Replay.replayOut = null;
		}

		if(!replayInputFilename.equals("")) {
			// initialize the replay input file and grab the old seed
			this.seed = Replay.init(new DataInputStream(new FileInputStream(replayInputFilename)), true);
		} else {
			// make a new seed and initialize keyboard input
			Replay.init(null, false);
			if (seed == null) {
				this.seed = System.currentTimeMillis();
			} else {
				this.seed = seed;
			}
		}

		if(Replay.replayOut != null) {
			Replay.replayOut.writeLong(this.seed);
		}
	}

	/**
	 * Executes the manager. The manager will sit in this method until it exits.
	 */
	protected abstract void start();

	/**
	 * Helpful stats about the manager that is exiting.
	 *
	 * @return The string that contains the helpful stats
	 */
	protected String stopString(){
		String s = "MessageLayer exiting.\nNumber of packets sent: " + String.valueOf(pktsSent);
		if(userControl != FailureLvl.EVERYTHING){
			s += "\nRandom Seed: " + seed;
		}
		return s;
	}

	/**
	 * Stops MessageLayer. This method should not return
	 */
	protected void stop() {
		System.out.println(stopString());
		System.exit(0);
	}

	/**
	 * Create a packet and put it on the channel. Crashes in the middle of a
	 * broadcast can be modeled by a post-send crash, plus a sequence of dropped
	 * messages
	 *
	 * @param fromNode
	 *            The node that is sending the packet
	 * @param to
	 *            Integer specifying the destination node
	 * @param protocol
	 *            The protocol of the message
	 * @param payload
	 *            The payload to be sent, serialized to a byte array
	 * @throws IllegalArgumentException
	 *             If the send is invalid
	 */
	protected void sendPkt(Node fromNode, int to, int protocol, byte[] payload) throws IllegalArgumentException {
		int from = fromNode.addr;
		if ( (payload.length > Packet.MAX_PAYLOAD_SIZE)
			|| !Packet.validAddress(to)
			|| !Packet.validAddress(from)) {

			throw new IllegalArgumentException("Either pkt is not valid, address is not valid, or TTL is not valid");
		}
		pktsSent++;
	}

	/**
	 * Sets the command parser that should be used.
	 *
	 * @param parser
	 *            The command parser instance to use
	 */
	protected void setParser(CommandsParser parser) {
		this.parser = parser;
	}

	/**
	 * Add a timer interrupt that will execute in a particular timestep.
	 *
	 * @param node
	 *            The node that added the the interrupt
	 * @param timeout
	 *            How many time steps to wait before firing
	 * @param cb
	 *            The callback to call when the timer fires
	 */
	protected void addTimeout(Node node, long timeout, Callback cb) {
		waitingTOs.add(new Timeout(node, now() + timeout, cb));
	}

	/**
	 * Gets the current time step of the execution.
	 *
	 * @return	The time step
	 */
	public long now(){
		return time;
	}

	/**
	 * Check if we should crash before a write.
	 *
	 * @param n
	 *            The node that is trying to write to the disc.
	 * @param description
	 *            The description of the write so the user knows what caused the
	 *            crash
	 */
	protected abstract void checkWriteCrash(Node n, String description);

	/**
	 * Set the current time. This should be used at the beginning, and after
	 * each time step.
	 *
	 * @param time
	 *            The time to set
	 */
	protected void setTime(long time) {
		this.time = time;
	}


	/**
	 * Triggered whenever the node attempts to write to the local storage device.
	 * @param node
	 * 			The node that is trying to write
	 * @param description
	 * 			The description of the write
	 */
	protected abstract void storageWriteEvent(Node node, String description);


	/**
	 * Triggered whenever the node attempts to read from the local storage device.
	 * @param node
	 * 			The node that is trying to read
	 * @param description
	 * 			The description of the read
	 */
	protected abstract void storageReadEvent(Node node, String description);

	/**
	 * Logs an event string for a node to synoptic partial log without a node
	 * field
	 *
	 * @param nodeAddr
	 *            Address of a node instance with which to associate the event
	 *            string (for timing)
	 * @param eventStr
	 *            the event string
	 */
	protected void logEvent(int nodeAddr, String eventStr) {
		// step() comes before logging because on communication, we've updated
		// the destination vtime to be at least the source, but it needs to be
		// strictly greater than the source.
		VectorTime vtime = vtimes.get(nodeAddr);
		vtime.step(nodeAddr);
		this.synPartialOrderLogger.logEvent("" + vtime.toString(), eventStr);
	}
}
