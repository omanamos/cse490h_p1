package edu.washington.cs.cse490h.lib;

import java.net.UnknownHostException;
import java.io.IOException;
import java.lang.Integer;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import edu.washington.cs.cse490h.lib.Node.NodeCrashException;
import edu.washington.cs.cse490h.lib.Packet.CorruptPacketException;

/**
 * Manager that runs under a single emulated node on the client machine.
 */
//FIXME: persistent storage and dynamic addresses
public class Emulator extends Manager {
	private Node node;
	private NodeServer server;
	private int address;
	private long timeStep;
	
	private String routerName;
	private int routerPort;

	// if the node or server are down
	private boolean failed;
	private boolean IOFinished;

	/**
	 * Base constructor for the Emulator. Does most of the work, but the command
	 * input method and failure level should be set before calling this
	 * constructor.
	 * 
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param routerName
	 *            The name of the machine on which the router is running
	 * @param routerPort
	 *            The port on which the router listens
	 * @param seed
	 *            Seed for the random number generator. Can be null to use the
	 *            current time as a seed
	 * @param timeStep
	 *            The number of milliseconds to wait between rounds
	 * @param replayOutputFilename
	 *            The log file for future relays of the current execution
	 * @param replayInputFilename
	 *            The log file to replay
	 * @throws IllegalArgumentException
	 *             If the arguments provided to the program are invalid
	 * @throws IOException
	 *             If creating the user input reader fails
	 */
	public Emulator(Class<? extends Node> nodeImpl, String routerName,
			int routerPort, Long seed, long timeStep,
			String replayOutputFilename, String replayInputFilename)
			throws IOException, IllegalArgumentException {
		super(nodeImpl, seed, replayOutputFilename, replayInputFilename);

		setParser(new EmulationCommandsParser());

		if (Replay.isReplaying()) {
			// We never want the node to kill itself cause the server did
			IOFinished = false;
		} else {
			IOFinished = true;
		}

		System.out.print("Starting emulation ");
		if (Replay.isReplaying()) {
			System.out.print("in replay mode ");
		}
		System.out.println("with seed: " + this.seed);
		Utility.randNumGen = new Random(this.seed);
		
		this.routerName = routerName;
		this.routerPort = routerPort;

		failed = false;

		this.timeStep = timeStep;
		setTime(0);
		this.logEventWithNodeField(node, "TIMESTEP time:" + this.now());
	}

	/**
	 * Create a new emulator that takes commands through user input
	 * 
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param routerName
	 *            Name of the machine that the Router is on
	 * @param routerPort
	 *            The port that the Router is listening on
	 * @param failureGen
	 *            How failures should be generated
	 * @param seed
	 *            Seed for the RNG. This can be null if the failure generator is
	 *            not a RNG
	 * 
	 * @throws UnknownHostException
	 *             If the router's name cannot be resolved
	 * @throws IOException
	 *             If there is an error in writing to the TCP socket
	 * @throws IllegalArgumentException
	 *             If the local port given is already in use
	 */
	public Emulator(Class<? extends Node> nodeImpl, String routerName,
			int routerPort, FailureLvl failureGen, Long seed, long timeStep,
			String replayOutputFilename, String replayInputFilename)
			throws UnknownHostException, IOException, IllegalArgumentException {
		this(nodeImpl, routerName, routerPort, seed, timeStep, replayOutputFilename, replayInputFilename);

		cmdInputType = InputType.USER;
		userControl = failureGen;
	}
	
	/**
	 * Create a new emulator that takes commands through a file
	 * 
	 * @param nodeImpl
	 *            The Class object for the student's node implementation
	 * @param routerName
	 *            Name of the machine that the Router is on
	 * @param routerPort
	 *            The port that the Router is listening on
	 * @param failureGen
	 *            How failures should be generated
	 * @param commandFile
	 *            File containing the list of commands
	 * @param seed
	 *            Seed for the RNG. This can be null if the failure generator is
	 *            not a RNG
	 * 
	 * @throws UnknownHostException
	 *             If the router's name cannot be resolved
	 * @throws IOException
	 *             If there is an error in writing to the TCP socket
	 * @throws IllegalArgumentException
	 *             If the local port given is already in use
	 */
	public Emulator(Class<? extends Node> nodeImpl, String routerName,
			int routerPort, FailureLvl failureGen, Long seed, long timeStep,
			String replayOutputFilename, String replayInputFilename,
			String commandFile) throws UnknownHostException, IOException,
			IllegalArgumentException {
		this(nodeImpl, routerName, routerPort, seed, timeStep, replayOutputFilename, replayInputFilename);

		cmdInputType = InputType.FILE;
		userControl = failureGen;

		EmulationCommandsParser commandFileParser = new EmulationCommandsParser();
		sortedEvents = commandFileParser.parseFile(commandFile);
	}

	/**
	 * Perform a single emulator time step with a set of events as argument
	 * 
	 * @param currentRoundEvents
	 */
	private void doTimestep(ArrayList<Event> currentRoundEvents) {
		// The order we check doesn't really matter
		checkInTransit(currentRoundEvents);

		checkTimeouts(currentRoundEvents);

		checkCrash(currentRoundEvents);

		executeEvents(currentRoundEvents);
		
	}
	
	/**
	 * Starts the emulated node
	 */
	@Override
	protected void start() {
		startNode();

		if (cmdInputType == InputType.FILE) {
			while (node != null || failed) {
				if (IOFinished && node != null) {
					System.err.println("Network I/O thread failed, killing the node...");

					failNode();
				}

				System.out.println("\nTime: " + now());

				if (node == null) {
					checkRecover();
				} else {
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

				setTime(now() + 1);
				this.logEventWithNodeField(node, "TIMESTEP time:" + this.now());
				
				try {
					// We sleep here to give a chance for messages to travel
					// over the network
					Thread.sleep(timeStep);
				} catch (InterruptedException e) {
				}
			}
		}else if(cmdInputType == InputType.USER) {
			while (node != null || failed) {
				if (IOFinished && node != null) {
					System.err.println("Network I/O thread failed, killing the node...");

					failNode();
				}

				System.out.println("\nTime: " + now());

				if (node == null) {
					checkRecover();
					
					if (userControl.compareTo(FailureLvl.CRASH) < 0) {
						try {
							// We sleep here to give a chance for messages to travel
							// over the network
							Thread.sleep(timeStep);
						} catch (InterruptedException e) {
						}
					}
					
					//FIXME: automatic recoveries = no user input at the time
					/*
					do{
						// just in case an exception is thrown or input is null
						Event ev = null;

						try{
							// A command will be converted into an Event and passed to the node later in the loop
							// A quit command will be matched later in this try block
							// Empty/whitespace will be treated as a skipped line, which will return null and cause a continue
							String input = keyboard.readLine();
							// Process user input if there is any
							if (input != null) {
								ev = parser.parseLine(input);
							}
						} catch (IOException e) {
							System.err.println("Error on user input: " + e);
						}

						if (ev == null) {
							advance = true;
						} else {
							if (ev.t == Event.EventType.TIME) {
								advance = true;
							} else {
								currentRoundEvents.add(ev);
							}
						}
					} while (!advance);*/
				} else {
					ArrayList<Event> currentRoundEvents = new ArrayList<Event>();

					// just in case an exception is thrown or input is null
					Event ev;
					boolean advance = false;
					System.out.println("Please input a sequence of commands terminated by a blank line or the TIME command:");

					do{
						// just in case an exception is thrown or input is null
						ev = null;

						try{
							// A command will be converted into an Event and passed to the node later in the loop
							// A quit command will be matched later in this try block
							// Empty/whitespace will be treated as a skipped line, which will return null and cause a continue
							String input = Replay.getLine();

							// Process user input if there is any
							if (input != null) {
								ev = parser.parseLine(input);
							}
						} catch (IOException e) {
							System.err.println("Error on user input: " + e);
						}

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
				
				setTime(now() + 1);
				this.logEventWithNodeField(node, "TIMESTEP time:" + this.now());
			}
		}
		
		stop();
	}
	
	@Override
	protected void stop() {
		System.out.println(stopString());
		if (node != null) {
			System.out.println(node.addr + ": " + node.toString());
			logEventWithNodeField(node, "STOPPED");
		} else {
			System.out.println("failed");
		}

		this.synTotalOrderLogger.stop();
		this.synPartialOrderLogger.stop();
		System.exit(0);
	}

	/******************* Methods to fail or restart a node *******************/

	/**
	 * Start up a node, crashed or brand new. If the node is alive, this method
	 * will crash it first.
	 */
	private void startNode() {
		if (node != null) {
			failNode();
		}
		if (server != null) {
			System.err.println("Error: Node was null but server wasn't?");
			killServer();
		}

		if (Replay.isReplaying()) {
			// grab the address from the replay input file
			try {
				Packet addrPkt = Replay.getPacket();
				
				if (Replay.isAddrPacket(addrPkt)) {
					address = addrPkt.getDest();
				} else {
					throw new Replay.ReplayException("Address packet expected! Instead, we got: " + addrPkt.toString());
				}
			} catch (CorruptPacketException e) {
				throw new Replay.ReplayException("Address packet expected, but packet was corrupted");
			}
		} else {
			// start up the server and get an address from it
			try {
				server = new NodeServer(routerName, routerPort, this);			
			} catch (IOException e) {
				System.err.println("Error while constructing server");
				e.printStackTrace();
				stop();
			}
			address = server.getAddress();
			IOFinished = false;
		}

		if (Replay.replayOut != null) {
			try {
				Packet addrPkt = Replay.getAddrPacket(address);
				Replay.replayOut.write(addrPkt.pack());
			} catch (IOException e) {
				throw new Replay.ReplayException(e.getMessage());
			}
		}

		if(address == Manager.BROADCAST_ADDRESS) {
			// Router returns broadcast address to signal it has no more free addresses
			System.err.println("We couldn't get a port from the Router.  Maybe there are no more free addresses?");
			stop();
		}

		// start up the node
		try {
			node = nodeImpl.newInstance();
		} catch (Exception e) {
			System.err.println("Error while constructing node: " + e);
			killServer();
			stop();
		}

		node.init(this, address);
		logEventWithNodeField(node, "START");
		failed = false;

		try {
			node.start();
		} catch (NodeCrashException e) {
			failNode();
		}
	}

	/**
	 * Fail the node. This attempts to kill both the node/its saved state, and
	 * the server.
	 * 
	 * @return The exception thrown after calling the fail() method. This is so,
	 *         if the stack includes methods in Node, we can rethrow the
	 *         Exception as necessary
	 */
	private NodeCrashException failNode() {
		killServer();
		return killNode();
	}

	/**
	 * Kill the node and its associated data structures.
	 * 
	 * @return The exception thrown after calling the fail() method. See
	 *         failNode() for why this happens
	 */
	private NodeCrashException killNode() {
		if (node == null) {
			return null;
		}
		
		NodeCrashException crash = null;
		
		try {
			node.fail();
		} catch (NodeCrashException e) {
			crash = e;
		}

		logEventWithNodeField(node, "FAILURE");

		waitingTOs.clear();
		node = null;
		failed = true;
		
		return crash;
	}

	/**
	 * Kill the NodeServer and get rid of the in transit messages.
	 */
	private void killServer() {
		if (server == null) {
			return;
		}
		
		if (!IOFinished) {
			server.close();
			IOFinished = true;
		}
		inTransitMsgs.clear();
		server = null;
	}

	@Override
	protected void checkWriteCrash(Node n, String description) {
		if(userControl.compareTo(FailureLvl.CRASH) < 0){
			if(Utility.getRNG().nextDouble() < failureRate) {
				System.out.println("Randomly failing before write");
				NodeCrashException e = failNode();
				// This function is called by Node, so we need to rethrow the
				// exception to fully stop execution
				throw e;
			}
		}else{
			try{
				System.out.println("Crash before " + description + "? (y/n)");
				String input = Replay.getLine().trim();

				if(input.length() != 0 && input.charAt(0) == 'y'){
					NodeCrashException e = failNode();
					// This function is called by Node, so we need to rethrow the
					// exception to fully stop execution
					throw e;
				}
			}catch(IOException e){
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
		logEventWithNodeField(node, "READ" + description);
	}

	
	/**
	 * Called by the NodeServer to signal that it has finished execution
	 */
	protected void IOFinish() {
		IOFinished = true;
	}

	/****************** Methods to check and handle events ******************/

	/**
	 * Goes through all of the in transit messages and decides whether to drop,
	 * delay, or deliver.
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void checkInTransit(ArrayList<Event> currentRoundEvents) {
		// Load in all the newly received messages
		Packet pkt;
		try {
			if (Replay.isReplaying()) {
				pkt = Replay.getPacket();
				if (Replay.isNullPacket(pkt)) {
					pkt = null;
				}
			} else {
				pkt = server.getPacket();
			}
			while(pkt != null) {
				if (Replay.replayOut != null) {
					Replay.replayOut.write(pkt.pack());
				}
				inTransitMsgs.add(pkt);
				if (Replay.isReplaying()) {
					pkt = Replay.getPacket();
					if (Replay.isNullPacket(pkt)) {
						pkt = null;
					}
				} else {
					pkt = server.getPacket();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (Replay.replayOut != null) {
			try {
				Replay.replayOut.write(Replay.getNullPacket().pack());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
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
					logEvent(node, "DROP " + p.toSynopticString(node));
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
						logEvent(node, "DROP " + p.toSynopticString(node));
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
						for(String s: delayList){
							Packet p = currentPackets.get(Integer.parseInt(s));
							inTransitMsgs.add(p);
							toBeRemoved.add(p);
							logEvent(node, "DELAY " + p.toSynopticString(node));
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
		
		if(userControl.compareTo(FailureLvl.DELAY) < 0){		// userControl < DELAY
			Iterator<Packet> iter = currentPackets.iterator();
			while(iter.hasNext()) {
				Packet p = iter.next();
				double rand = Utility.getRNG().nextDouble();
				// adjust the probability since these are not independent events
				//   Ex: 50% drop rate and 50% delay rate should mean that nothing gets through
				double adjustedDelay = delayRate / (1 - dropRate);
				if(rand < adjustedDelay){
					System.out.println("Randomly Delaying: " + p.toString());
					logEvent(node, "DELAY " + p.toSynopticString(node));
					iter.remove();
					inTransitMsgs.add(p);
				}
			}
		}

		for (Packet p : currentPackets) {
			currentRoundEvents.add(Event.getDelivery(p));
		}
	}

	/**
	 * Checks whether to crash a live node
	 * 
	 * @param currentRoundEvents
	 *            The list of the current round's events that we should add to
	 */
	private void checkCrash(ArrayList<Event> currentRoundEvents) {
		// See if we should crash.
		// Failures and restarts specified in the file are deprecated
		if (userControl.compareTo(FailureLvl.CRASH) < 0){
			double rand = Utility.getRNG().nextDouble();
			if(rand < failureRate){
				currentRoundEvents.add(Event.getFailure(address));
			}
		} else {
			try {
				System.out.println("Crash? (y/n)");
				String input = Replay.getLine().trim();
				if (input.charAt(0) == 'y') {
					currentRoundEvents.add(Event.getFailure(address));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Checks whether to recover a crashed node
	 */
	private void checkRecover() {
		// See if we should recover
		if (userControl.compareTo(FailureLvl.CRASH) < 0) { // userControl < CRASH
			// make a copy so we don't have concurrent modification exceptions
			double rand = Utility.getRNG().nextDouble();
			if (rand < recoveryRate) {
				startNode();
			}
		} else {
			try {
				// The user could also just use the start command, but not if
				// the input method is file
				System.out.println("Restart? (y/n)");
				String input = Replay.getLine().trim();
				if (input.charAt(0) == 'y') {
					startNode();
				}
			} catch (IOException e) {
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
		ArrayList<Timeout> currentTOs = waitingTOs;
		waitingTOs = new ArrayList<Timeout>();
		
		Iterator<Timeout> iter = currentTOs.iterator();
		while(iter.hasNext()) {
			Timeout to = iter.next();
			if(now() >= to.fireTime) {
				iter.remove();
				currentRoundEvents.add(Event.getTimeout(to));
			}
		}
		
		waitingTOs.addAll(currentTOs);
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
			failNode();
			break;
		case START:
			startNode();
			break;
		case EXIT:
			stop();
			break;
		case COMMAND:
			sendNodeCmd(ev.command);
			break;
		case ECHO:
			parser.printStrArray(ev.msg, System.out);
			break;
		case DELIVERY:
			deliverPkt(ev.p);
			break;
		case TIMEOUT:
			logEventWithNodeField(ev.to.node, "TIMEOUT fire-time:" + ev.to.fireTime + " " + ev.to.cb.toString());
						
			try{
				ev.to.cb.invoke();
			}catch(InvocationTargetException e) {
				Throwable t = e.getCause();
				if(t == null) {
					e.printStackTrace();
				} else if (t instanceof NodeCrashException) {
					failNode();
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
		super.sendPkt(fromNode, to, protocol, payload);  // check arguments
		
		if(node == null) {
			//shouldn't ever happen
			return;
		}
		
		Packet newPacket = new Packet(to, fromNode.addr, protocol, payload);
		logEvent(fromNode, "SEND " + newPacket.toSynopticString(fromNode));	//XXX: broadcasts are one msg here, whereas simulator they are multiple
		sendToRouter(to, newPacket.pack());
		return;
	}

	/**
	 * Send a packet off to the router.
	 * 
	 * @param destAddr
	 *            The virtual address of the destination
	 * @param pkt
	 *            The serialized version of the Packet to be sent
	 */
	private void sendToRouter(int destAddr, byte[] pkt) {
		if(!Replay.isReplaying()) {
			server.send(pkt);
		}
		// else ignore it
	}

	/**
	 * Actually deliver an in transit packet.
	 * 
	 * @param pkt
	 *            The packet that should be delivered
	 */
	private void deliverPkt(Packet pkt) {
		if(node == null) {
			return;
		}
		
		logEvent(node, "RECVD " + pkt.toSynopticString(node));
		
		if(pkt.getDest() == address || pkt.getDest() == Manager.BROADCAST_ADDRESS) {
			try{
				node.onReceive(pkt.getSrc(), pkt.getProtocol(), pkt.getPayload());
			} catch (NodeCrashException e) {
				failNode();
			}
		}
		// drop if not for me. This can happen if we took a port that was recently occupied by another node
	}

	/**
	 * Sends command to the node
	 * 
	 * @param msg
	 *            The msg to send to the node
	 */
	private void sendNodeCmd(String msg) {
		if(node == null) {
			return;
		}
		
		logEventWithNodeField(node, "COMMAND" + msg);
		
		try{
			node.onCommand(msg);
		} catch (NodeCrashException e) {
			failNode();
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

	@Override
	public void logEvent(Node node, String eventStr) {
		super.logEvent(node, eventStr);
	}
}
