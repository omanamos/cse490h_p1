package edu.washington.cs.cse490h.lib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import edu.washington.cs.cse490h.lib.Node.NodeCrashException;

/**
 * Manages a simulation, where all nodes are running in the same process, in the
 * same thread
 */
public class Simulator extends Manager {

	public static final int MAX_NODES_TO_SIMULATE = Manager.MAX_ADDRESS - 1;
    
	private HashMap<Integer, Node> nodes;

	private HashSet<Integer> crashedNodes;
	
	// the global logical time ordering which increments by 1 on each
	// event in the simulated system.
	private int globalLogicalTime = 0;
	
	private HashSet<Timeout> canceledTimeouts;

	/**
	 * Base constructor for the Simulator. Does most of the work, but the
	 * command input method and failure level should be set before calling this
	 * constructor.
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
	public Simulator(Class<? extends Node> nodeImpl, Long seed, String replayOutputFilename, String replayInputFilename)
			throws IllegalArgumentException, IOException {
		super(nodeImpl, seed, replayOutputFilename, replayInputFilename);
		
		setParser(new SimulationCommandsParser());

		if (seed == null) {
			this.seed = System.currentTimeMillis();
		} else {
			this.seed = seed;
		}
		System.out.println("Starting simulation with seed: " + this.seed);
		Utility.randNumGen = new Random(this.seed);

		nodes = new HashMap<Integer, Node>();
		vtimes = new HashMap<Integer, VectorTime>();
		crashedNodes = new HashSet<Integer>();

		setTime(0);
		this.logSimulatorEvent("TIMESTEP time:" + this.now());
	}

	/**
	 * Constructor for a simulator that takes commands from a file.
	 * 
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param failureGen
	 *            How failures should be generated
	 * @param seed
	 *            Seed for the RNG. This can be null.
	 * @param replayOutputFilename
	 *            The log file for future relays of the current execution
	 * @param replayInputFilename
	 *            The log file to replay
	 * @param commandfile
	 *            File containing the list of commands
	 * @throws IllegalArgumentException
	 *             If the arguments provided to the program are invalid
	 * @throws FileNotFoundException
	 *             If the command file does not exist
	 * @throws IOException
	 *             If creating the user input reader fails
	 */
	public Simulator(Class<? extends Node> nodeImpl, FailureLvl failureGen, Long seed, String replayOutputFilename, String replayInputFilename, String commandFile)
			throws IllegalArgumentException, FileNotFoundException, IOException {
		this(nodeImpl, seed, replayOutputFilename, replayInputFilename);

		cmdInputType = InputType.FILE;
		userControl = failureGen;
		
		SimulationCommandsParser commandFileParser = new SimulationCommandsParser();
		sortedEvents = commandFileParser.parseFile(commandFile);
	}

	/**
	 * Constructor for a simulator that takes commands from the user.
	 * 
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param failureGen
	 *            How failures should be generated
	 * @param seed
	 *            Seed for the RNG. This can be null.
	 * @param replayOutputFilename
	 *            The log file for future relays of the current execution
	 * @param replayInputFilename
	 *            The log file to replay
	 * @throws IllegalArgumentException
	 *             If the arguments provided to the program are invalid
	 * @throws IOException
	 *             If creating the user input reader fails
	 */
	public Simulator(Class<? extends Node> nodeImpl, FailureLvl failureGen, Long seed, String replayOutputFilename, String replayInputFilename)
			throws IllegalArgumentException , IOException{
		this(nodeImpl, seed, replayOutputFilename, replayInputFilename);
		
		cmdInputType = InputType.USER;
		userControl = failureGen;
	}

	/********** Methods for starting and stopping the simulation **********/

	@Override
	protected void start() {
		// start the synoptic loggers
		this.synTotalOrderLogger.start(MessageLayer.synopticTotalOrderLogFilename);
		this.synPartialOrderLogger.start(MessageLayer.synopticPartialOrderLogFilename);

		if (cmdInputType == InputType.FILE) {
			while (!inTransitMsgs.isEmpty() || !sortedEvents.isEmpty()
					|| !waitingTOs.isEmpty()) {
				System.out.println("\nTime: " + now());

				ArrayList<Event> currentRoundEvents = new ArrayList<Event>();

				boolean advance = false;
				do {
					if (sortedEvents.isEmpty()) {
						advance = true;
					} else {
						Event ev = sortedEvents.remove(0);
						if (ev.t == Event.EventType.TIME) {
							advance = true;
						} else {
							currentRoundEvents.add(ev);
						}
					}
				} while (!advance);
				
				this.doTimestep(currentRoundEvents);
				
			}
		} else if (cmdInputType == InputType.USER) {
			while (true) {
				System.out.println("\nTime: " + now());

				ArrayList<Event> currentRoundEvents = new ArrayList<Event>();

				Event ev;
				boolean advance = false;
				System.out.println("Please input a sequence of commands terminated by a blank line or the TIME command:");

				do {
					// just in case an exception is thrown or input is null
					ev = null;

					try {
						// Process user input if there is any
						String input = Replay.getLine();

						if (input != null) {
							// A command will be converted into an Event.
							ev = parser.parseLine(input);
						}
					} catch (IOException e) {
						System.err.println("Error on user input: " + e);
					}

					// Empty/whitespace will be treated as a skipped
					// line, which will return null and cause an advance
					if (ev == null) {
						advance = true;
					} else {
						if (ev.t == Event.EventType.TIME) {
							advance = true;
						} else {
							currentRoundEvents.add(ev);
						}
					}
				} while (!advance);
				
				this.doTimestep(currentRoundEvents);
				
			}
		}

		stop();
	}
	
	
	/**
	 * Perform a single simulator time step with a set of events as argument
	 *  
	 * @param currentRoundEvents
	 */
	private void doTimestep(ArrayList<Event> currentRoundEvents) {
		// The order we check doesn't matter that much
		checkInTransit(currentRoundEvents);

		checkTimeouts(currentRoundEvents);

		checkCrash(currentRoundEvents);

		executeEvents(currentRoundEvents);

		setTime(now() + 1);
		logSimulatorEvent("TIMESTEP time:" + this.now());
	}
	
	
	@Override
	protected void stop(){
		System.out.println(stopString());
		for(Integer i: nodes.keySet()){
			System.out.println(i + ": " + nodes.get(i).toString());
			logEventWithNodeField(nodes.get(i), "STOPPED");
		}
		
		for(Integer i: crashedNodes){
			System.out.println(i + ": failed");
		}
		
		// stop the synoptic logger
		this.synTotalOrderLogger.stop();
		this.synPartialOrderLogger.stop();
		System.exit(0);
	}

	/******************* Methods to fail or restart a node *******************/

	/**
	 * Start up a node, crashed or brand new. If the node is alive, this method
	 * will crash it first.
	 * 
	 * @param node
	 *            The address at which to start the node
	 */
	private void startNode(int node){
		if(!validNodeAddress(node)){
			System.err.println("Invalid new node address: " + node);
			return;
		}
		
		if(nodes.containsKey(node)){
			failNode(node);
		}
		
		Node newNode;
		try{
			newNode = nodeImpl.newInstance();
		}catch(Exception e){
			System.err.println("Error while contructing node: " + e);
			failNode(node);
			return;
		}

		if (crashedNodes.contains(node)) {
			crashedNodes.remove(node);
		}
		nodes.put(node, newNode);
		
		newNode.init(this, node);
		vtimes.put(node, new VectorTime(MAX_ADDRESS));
		logEventWithNodeField(newNode, "START");
		
		try{
			newNode.start();
		}catch(NodeCrashException e) {
			failNode(newNode.addr);
		}
	}

	/**
	 * Fail a node. This method updates data structures, removes the failed
	 * nodes's timeouts and calls its fail() method
	 * 
	 * @param node
	 *            The node address to fail
	 * @return The exception thrown after calling the fail() method. This is so,
	 *         if the stack includes methods in Node, we can rethrow the
	 *         Exception as necessary
	 */
	private NodeCrashException failNode(int node){
		NodeCrashException crash = null;

		if(isNodeValid(node)) {
			Node crashingNode = nodes.get(node);
			try{
				crashingNode.fail();
			}catch(NodeCrashException e) {
				crash = e;
			}
			
			logEventWithNodeField(crashingNode, "FAILURE");
			
			nodes.remove(node);
			crashedNodes.add(node);
			
			Iterator<Timeout> iter = waitingTOs.iterator();
			while(iter.hasNext()){
				Timeout to = iter.next();
				if(to.node.addr == node){
					canceledTimeouts.add(to);
				}
			}
		}
		
		return crash;
	}
	
	@Override
	protected void checkWriteCrash(Node n, String description) {
		if(userControl.compareTo(FailureLvl.CRASH) < 0){
			if(Utility.getRNG().nextDouble() < failureRate) {
				System.out.println("Randomly failing before write: " + n.addr);
				NodeCrashException e = failNode(n.addr);
				// This function is called by Node, so we need to rethrow the
				// exception to fully stop execution
				throw e;
			}
		}else{
			try{
				System.out.println("Crash node " + n.addr + " before " + description + "? (y/n)");
				String input = Replay.getLine().trim();
				if (input.length() != 0 && input.charAt(0) == 'y') {
					NodeCrashException e = failNode(n.addr);
					// This function is called by Node, so we need to rethrow
					// the exception to fully stop execution
					throw e;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	protected void storageWriteEvent(Node node, String description) {
		logEventWithNodeField(node, "WRITE " + description);
	}
	
	@Override
	protected void storageReadEvent(Node node, String description) {
		logEventWithNodeField(node, "READ " + description);
	}

	/****************** Methods to check and handle events ******************/
	
	/**
	 * Logs an in transit event -- a DROP or a DELAY event.
	 */
	private void logInTransit(Packet p, String netEvent) {
		Node destNode = nodes.get(p.getDest());
		if (destNode == null) {
			// Node failed while the packet was in transit.
			// Ignore the transit event.
			return;
		}
		logEvent(destNode, netEvent + " " + p.toSynopticString(destNode));
	}
	

	/**
	 * Goes through all of the in transit messages and decides whether to drop,
	 * delay, or deliver.
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void checkInTransit(ArrayList<Event> currentRoundEvents) {
		if(inTransitMsgs.isEmpty()){
			return;
		}
		
		// See what we should do with all the in-transit messages
		ArrayList<Packet> currentPackets = inTransitMsgs;
		inTransitMsgs = new ArrayList<Packet>();
		
		if(userControl.compareTo(FailureLvl.DROP) < 0){		// userControl < DROP
			// Figure out if we need to drop the packet.
			Iterator<Packet> iter = currentPackets.iterator();
			while(iter.hasNext()) {
				Packet p = iter.next();
				double rand = Utility.getRNG().nextDouble();
				if(rand < dropRate){
					System.out.println("Randomly dropping: " + p.toString());
					this.logInTransit(p, "DROP");
					iter.remove();
				}
			}
		}else{
			System.out.println("The following messages are in transit: ");
			for(int i = 0; i < currentPackets.size(); ++i){
				System.out.println(i + ": " + currentPackets.get(i).toString());
			}

			try{
				System.out.println("Which should be dropped? (space delimited list or just press enter to drop none)");
				String input = Replay.getLine().trim();
				// hash set so we don't have to deal with duplicates
				HashSet<Packet> toBeRemoved = new HashSet<Packet>();
				
				if(!input.equals("")){
					String[] dropList = input.split("\\s+");
					Packet p;
					for(String s: dropList){
						p = currentPackets.get(Integer.parseInt(s));
						toBeRemoved.add(p);
						this.logInTransit(p, "DROP");
					}
				}
				
				if(toBeRemoved.size() == currentPackets.size()){
					return;
				}
				
				// If user drops and delays the same packet, result is undefined
				//   In current implementation, delay takes precedence
				if(userControl.compareTo(FailureLvl.DELAY) >= 0){		// userControl >= DELAY
					System.out.println("Which should be delayed? (space delimited list or just press enter to delay none)");
					input = Replay.getLine().trim();
					
					if(!input.equals("")){
						String[] delayList = input.split("\\s+");
						Packet p;
						for(String s: delayList){
							p = currentPackets.get(Integer.parseInt(s));
							inTransitMsgs.add(p);
							toBeRemoved.add(p);
							this.logInTransit(p, "DELAY");
						}
					}
					
					if(toBeRemoved.size() == currentPackets.size()){
						return;
					}
				}
				
				currentPackets.removeAll(toBeRemoved);
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
		if (userControl.compareTo(FailureLvl.DELAY) < 0) { // userControl < DELAY
			Iterator<Packet> iter = currentPackets.iterator();
			while (iter.hasNext()) {
				Packet p = iter.next();
				double rand = Utility.getRNG().nextDouble();
				// adjust the probability since these are not independent events
				//   Ex: 50% drop rate and 50% delay rate should mean that nothing gets through
				double adjustedDelay = delayRate / (1 - dropRate);
				if(rand < adjustedDelay){
					System.out.println("Randomly Delaying: " + p.toString());
					iter.remove();
					inTransitMsgs.add(p);
					this.logInTransit(p, "DELAY");
				}
			}
		}
		
		for(Packet p: currentPackets) {
			currentRoundEvents.add(Event.getDelivery(p));
		}
	}

	/**
	 * Checks whether to crash any live node or restart any failed node
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void checkCrash(ArrayList<Event> currentRoundEvents) {
		// Failures specified in the file are deprecated
		if (userControl.compareTo(FailureLvl.CRASH) < 0) {		// userControl < CRASH
			// make a copy so we don't have concurrent modification exceptions
			Integer[] addrCopy = nodes.keySet().toArray(new Integer[0]);

			for (Integer i : addrCopy) {
				double rand = Utility.getRNG().nextDouble();
				if (rand < failureRate) {
					currentRoundEvents.add(Event.getFailure(i));
				}
			}

			addrCopy = crashedNodes.toArray(new Integer[0]);
			for (Integer i : addrCopy) {
				double rand = Utility.getRNG().nextDouble();
				if (rand < recoveryRate) {
					currentRoundEvents.add(Event.getStart(i));
				}
			}
		} else {
			try {
				printLiveDead();
				String input;

				if (!nodes.isEmpty()) {
					System.out.println("Crash which nodes? (space-delimited list of addresses or just press enter)");
					input = Replay.getLine().trim();
					if(!input.equals("")){
						String[] crashList = input.split("\\s+");
						for(String s: crashList){
							currentRoundEvents.add(Event.getFailure(Integer.parseInt(s)));
						}
					}
				}
				
				// The user could also just use the start command, but not if the input method is file
				if(!crashedNodes.isEmpty()){
					System.out.println("Restart which nodes? (space-delimited list of addresses or just press enter)");
					input = Replay.getLine().trim();
					if(!input.equals("")){
						String[] restartList = input.split("\\s+");
						for(String s: restartList){
							currentRoundEvents.add(Event.getStart(Integer.parseInt(s)));
						}
					}
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * Check to see if any timeouts are supposed to fire during the current time
	 * step
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void checkTimeouts(ArrayList<Event> currentRoundEvents) {
		Iterator<Timeout> iter = waitingTOs.iterator();
		while (iter.hasNext()) {
			Timeout to = iter.next();
			if(now() >= to.fireTime) {
				iter.remove();
				currentRoundEvents.add(Event.getTimeout(to));
			}
		}
	}
	
	/**
	 * Reorders and executes all the events for the current round.
	 * 
	 * Note that commands can be executed in a different order than they appear
	 * in the command file!
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void executeEvents(ArrayList<Event> currentRoundEvents) {
		canceledTimeouts = new HashSet<Timeout>();

		if(userControl == FailureLvl.EVERYTHING){
			boolean doAgain = false;
			do{
				try{
					for(int i = 0; i < currentRoundEvents.size(); ++i){
						System.out.println(i + ": " + currentRoundEvents.get(i).toString());
					}
					System.out.println("In what order should the events happen? (enter for in-order)");
					String input = Replay.getLine().trim();

					if(input.equals("")){
						// enter for in-order
						for(Event ev: currentRoundEvents){
							handleEvent(ev);
						}
					}else{
						String[] order = input.split("\\s+");

						HashSet<Event> dupeMissCheck = new HashSet<Event>();
						for(String s: order){
							dupeMissCheck.add(currentRoundEvents.get(Integer.parseInt(s)));
						}
						
						if(dupeMissCheck.size() != currentRoundEvents.size()) {
							System.out.println("Not all of the events were specified!");
							doAgain = true;
							continue;
						}
						
						for(String s: order){
							Event ev = currentRoundEvents.get(Integer.parseInt(s));
							handleEvent(ev);
						}
					}
				}catch(IOException e){
					e.printStackTrace();
					doAgain = true;
				}
			}while(doAgain);
		}else{
			Collections.shuffle(currentRoundEvents, Utility.getRNG());
			System.out.println("Executing with order: ");
			for(Event ev: currentRoundEvents) {
				System.out.println(ev.toString());
				handleEvent(ev);
			}
		}
		
		waitingTOs.removeAll(canceledTimeouts);
	}

	/**
	 * Process an event.
	 * 
	 * @param ev
	 *            The event that should be processed
	 */
	private void handleEvent(Event ev){

		switch(ev.t){
		case FAILURE:
			failNode(ev.node);
			break;
		case START:
			startNode(ev.node);
			break;
		case EXIT:
			stop();
			break;
		case COMMAND:
			sendNodeCmd(ev.node, ev.command);
			break;
		case ECHO:
			// since this is not intended for any particular node, we can't
			// associate it with any node, and therefore we don't log it with
			// synoptic
			parser.printStrArray(ev.msg, System.out);
			break;
		case DELIVERY:
			deliverPkt(ev.p);
			break;
		case TIMEOUT:
			if(canceledTimeouts.contains(ev.to)) {
				break;
			}
			
			logEventWithNodeField(ev.to.node, "TIMEOUT fire-time:" + ev.to.fireTime + " " + ev.to.cb.toSynopticString());
						
			try{
				ev.to.cb.invoke();
			}catch(InvocationTargetException e) {
				Throwable t = e.getCause();
				if(t == null) {
					e.printStackTrace();
				} else if(t instanceof NodeCrashException) {
					failNode(ev.to.node.addr);
				} else {
					t.printStackTrace();
				}
			}catch(IllegalAccessException e) {
				e.printStackTrace();
			}
			break;
		default:
			System.err.println("Shouldn't happen. TIME here?");
		}
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
	@Override
	protected void sendPkt(Node fromNode, int to, int protocol, byte[] payload) throws IllegalArgumentException {
		int from = fromNode.addr;
		super.sendPkt(fromNode, to, protocol, payload);  // check arguments
		
		if(!isNodeValid(from)) {
			return;
		}
		
		if(to == Manager.BROADCAST_ADDRESS) {
			// We create a new packet for each msg in the broadcast since
			// delivery in the simulator is based on the destination address of
			// the packet. The student will never handle the packet so this is
			// OK.
			for(Integer i: nodes.keySet()) {
				if(i != from){
					Packet newPacket = new Packet(i, from, protocol, payload);
					logEvent(fromNode, "SEND " + newPacket.toSynopticString(fromNode));
					inTransitMsgs.add(newPacket);
				}
			}
			for(Integer i: crashedNodes) {
				Packet newPacket = new Packet(i, from, protocol, payload);
				logEvent(fromNode, "SEND " + newPacket.toSynopticString(fromNode));
				inTransitMsgs.add(newPacket);
			}
		}else{
			Packet newPacket = new Packet(to, from, protocol, payload);
			logEvent(fromNode, "SEND " + newPacket.toSynopticString(fromNode));
			inTransitMsgs.add(newPacket);
		}
	}
	
	/**
	 * Actually deliver an in transit packet to its intended destination.
	 * 
	 * @param destAddr
	 *            The address of the recipient
	 * @param destNode
	 *            The node object of the recipient
	 * @param srcAddr
	 *            The address of the sender
	 * @param pkt
	 *            The packet that should be delivered
	 */
	private void deliverPkt(Packet pkt) {
		int destAddr = pkt.getDest();
		int srcAddr = pkt.getSrc();
		if(!isNodeValid(destAddr)) {
			return;
		}

		Node destNode = nodes.get(destAddr);
		vtimes.get(destAddr).updateTo(vtimes.get(srcAddr));
				
		logEvent(destNode, "RECVD " + pkt.toSynopticString(destNode));
				
		try{
			destNode.onReceive(srcAddr, pkt.getProtocol(), pkt.getPayload());
		}catch(NodeCrashException e) {
			failNode(destAddr);
		}
	}

	/**
	 * Sends command to the specified node
	 * 
	 * @param nodeAddr
	 *            Address of the node to whom the message should be sent
	 * @param msg
	 *            The msg to send to the node
	 */
	private void sendNodeCmd(int nodeAddr, String msg) {
		if(!isNodeValid(nodeAddr)) {
			return;
		}

		Node n = nodes.get(nodeAddr);

		logEventWithNodeField(n, "COMMAND " + msg);

		try {
			n.onCommand(msg);
		} catch (NodeCrashException e) {
			failNode(n.addr);
		}
	}

	/**
	 * Check if the address is valid
	 * 
	 * @param addr
	 *            The address to check
	 * @return true if the address is valid, false otherwise
	 */
	protected static boolean validNodeAddress(int addr) {
		return (addr <= MAX_NODES_TO_SIMULATE && addr >= 0);
	}

	/**
	 * Check whether a given node is live and therefore valid to give msgs/cmds.
	 * Additionally, an error message will print out if the address itself is
	 * invalid.
	 * 
	 * @param nodeAddr
	 *            The node for which we want to check validity
	 * @return true If the node is alive, false if not.
	 */
	private boolean isNodeValid(int nodeAddr) {
		// up and running valid node
		if (nodes.containsKey(nodeAddr)) {
			return true;
		}

		// node is crashed but addr is still valid
		if (crashedNodes.contains(nodeAddr)) {
			return false;
		}

		// the node address is invalid
		System.err.println("Node address " + nodeAddr + " is invalid.");
		return false;
	}

	/**
	 * Print out a list of live and crashed nodes in a human-readable way.
	 */
	private void printLiveDead() {
		if (!nodes.isEmpty()) {
			Iterator<Integer> iter = nodes.keySet().iterator();
			StringBuffer live = new StringBuffer();
			// its not empty so we know it hasNext()
			live.append(iter.next());

			while (iter.hasNext()) {
				live.append(", " + iter.next());
			}

			System.out.println("Live nodes: " + live.toString());
		}

		if (!crashedNodes.isEmpty()) {
			Iterator<Integer> iter = crashedNodes.iterator();
			StringBuffer dead = new StringBuffer();
			// its not empty so we know it hasNext()
			dead.append(iter.next());

			while (iter.hasNext()) {
				dead.append(", " + iter.next());
			}

			System.out.println("Dead nodes: " + dead.toString());
		}
	}

	/**
	 * Log the event in the synoptic log using the simulator's global logical ordering with a node field
	 * 
	 * @param node node generating the event
	 * @param eventStr the event string description of the event
	 */
	public void logEventWithNodeField(Node node, String eventStr) {
		// The Simulator implicitly totally orders events (because it is single threaded)
		// so we also output a globally total order (in addition to the partial order
		// that is implemented in super).
		String eventStrNoded = "node:" + node.toSynopticString() + " " + eventStr;
		this.logEvent(node, eventStrNoded);
		super.logEvent(node, eventStrNoded);
	}
	
	
	/**
	 * Log the event in the synoptic log using the simulator's global logical ordering 
	 * 
	 * @param node node generating the event
	 * @param eventStr the event string description of the event
	 */
	@Override
	public void logEvent(Node node, String eventStr) {
		// The Simulator implicitly totally orders events (because it is single threaded)
		// so we also output a globally total order (in addition to the partial order
		// that is implemented in super).
		this.synTotalOrderLogger.logEvent("" + this.globalLogicalTime, eventStr);
		this.globalLogicalTime += 1;
		super.logEvent(node, eventStr);
	}
	
	/**
	 * Logs a simulator event across _all_ simulated nodes. The TIMESTEP event
	 * is of this form -- its reported for every node being simulated.
	 * 
	 * @param eventStr
	 */
	public void logSimulatorEvent(String eventStr) {
		for(Node node: nodes.values()) {
			this.logEventWithNodeField(node, eventStr);
		}
	}
}
