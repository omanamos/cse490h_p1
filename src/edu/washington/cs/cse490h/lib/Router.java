package edu.washington.cs.cse490h.lib;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.lang.Integer;

import plume.Option;
import plume.OptionGroup;
import plume.Options;

/**
 * <pre>
 *
 * Simply routes packets in the network and stores queued packets to failed
 * nodes. It works by listening for new TCP connections. The Router sends the
 * address that the emulated node should use and forks off a new thread to deal
 * with the connection.
 *
 * Usage: java Router [options]
 *
 * General Options:
 *   -h --help=<boolean>    - Print usage message [default false]
 *   -v --version=<boolean> - Print program version [default false]
 *
 * Execution Options:
 *   -p --localPort=<int>   - Local UDP port [default -1]
 *
 * </pre>
 */
public class Router {
	private static Router router = null;
	private ServerSocket socket;
	// emulatedNodes is also used as a lock
	protected Map<Integer, NodeContainer> emulatedNodes;

	/**
	 * Construct a new Router
	 *
	 * @param port
	 *            The port to listen on. This must be a valid port (>1024)
	 * @throws IOException
	 *             If there is an error creating the socket server
	 */
	private Router(int port) throws IOException {
		socket = new ServerSocket(port);
		emulatedNodes = Collections.synchronizedMap( new HashMap<Integer, NodeContainer>() ) ;
	}

	/**
	 * Start up the Router. It sits and listens for new connections, and spawns
	 * a new thread for each one.
	 */
	protected void start() {
		System.out.println("Router awaiting nodes...");

		while(true) {
			try {
				Socket nodeSocket = socket.accept();

				InetAddress ipAddress = nodeSocket.getInetAddress();
				int port = nodeSocket.getPort();

				if (port < 1024) {
					System.err.println("Router: Shouldn't happen! Illegal port: " + port);
					nodeSocket.getOutputStream().write(Manager.BROADCAST_ADDRESS);
					nodeSocket.getOutputStream().flush();
					nodeSocket.close();
				} else {
					NodeContainer old = portConflict(ipAddress, port);
					if (old != null) {
						old.quit(null);
					}

					// find a virtual address to assign to the new node
					int address = freeAddr();

					if(address == -1) {
						System.err.println("Router: out of addresses");
						nodeSocket.getOutputStream().write(Manager.BROADCAST_ADDRESS);
						nodeSocket.getOutputStream().flush();
						nodeSocket.close();
					} else {
						//Disable Nagle
						nodeSocket.setTcpNoDelay(true);

						System.out.println("Connecting to " + ipAddress + ":" + port + " Assigning addr: " + address);
						nodeSocket.getOutputStream().write(address);
						nodeSocket.getOutputStream().flush();

						EmulatedNode newNode = new EmulatedNode(this, nodeSocket, address, ipAddress, port);
						nodeJoin(address, newNode);
					}
				}
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Stop the Router
	 */
	protected void exit() {
		System.out.println("Router exiting...");
		System.exit(0);
	}

	/**
	 * Called by an emulated node to signal that it is quitting
	 *
	 * @param address
	 *            The virtual address of the node that is quitting
	 * @param queue
	 *            The queue of in-transit packets gotten from the termination
	 *            protocol.  Can be empty.
	 */
	protected void nodeQuit(int address, LinkedList<Packet> queue) {
		emulatedNodes.get(address).quit(queue);
	}

	/**
	 * Called by the start thread to create a new emulated node
	 *
	 * @param address
	 *            The virtual address of the new node
	 * @param newNode
	 *            The new emulated node object
	 */
	private void nodeJoin(int address, EmulatedNode newNode) {
		if (emulatedNodes.containsKey(address)) {
			emulatedNodes.get(address).restart(newNode);
		} else {
			emulatedNodes.put(address, new NodeContainer(newNode));
		}
	}

	/**
	 * Gets the next available address.
	 *
	 * Even without locking, this guarantees the address is free since joins are
	 * only handled in this thread. The returned address is not guaranteed to be
	 * the lowest free address however
	 *
	 * @return The next available address, or -1 if there are no more free
	 *         addresses
	 */
	private int freeAddr() {
		for (int i = 0; i < Manager.BROADCAST_ADDRESS; i++) {
			if (!emulatedNodes.containsKey(i) || !emulatedNodes.get(i).isUp()) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Check if there is already a node using a specific IP and port.
	 *
	 * @param ipAddress
	 *            The IP address of the machine on which the node is running
	 * @param port
	 *            The port that the node is using
	 * @return A node container that is using the same IP/port, or null if it
	 *         does not exist
	 */
	private NodeContainer portConflict(InetAddress ipAddress, int port) {
		synchronized(emulatedNodes) {
			for (NodeContainer node : emulatedNodes.values()) {
				if (node.hasConflict(ipAddress, port)) {
					return node;
				}
			}
			return null;
		}
	}

	/**
	 * The current version.
	 */
	public static final String versionString = "v0.1";

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
	 * Local port
	 */
	@OptionGroup("Execution Options")
	@Option(value="-p Local port", aliases={"-local-port"})
	// TODO: specify a sane default
	public static int localPort = -1;
	// end option group "Execution Options"


	/** One line synopsis of usage */
	private static String usage_string
	= "java Router [options]";

	/**
	 * Prints out an warning message
	 *
	 * @param msg
	 *            warning msg string
	 */
	public static void printWarning(String msg) {
		System.err.println("Warning: " + msg);
	}

	/**
	 * Prints out an error message
	 *
	 * @param msg
	 *            error msg string
	 */
	public static void printError(String msg) {
		System.err.println("Error: " + msg);
	}

	/**
	 * Entry point to start Router
	 */
	public static void main(String[] args) {
		// this directly sets the static member options of the Main class
		Options options = new Options (usage_string, Router.class);

		@SuppressWarnings("unused")
		String[] cmdLineArgs = options.parse_or_usage(args);

		if (help) {
			options.print_usage();
			return;
		}

		if (version) {
			System.out.println(Router.versionString);
			return;
		}

		if (localPort == -1) {
			System.out.println("you must specify a port with -p.");
			return;
		}

		try {
			router = new Router(localPort);
			router.start();
		}catch(IOException e) {
			System.err.println("Invalid port given to Router. Exception: " + e);
		}catch(Exception e) {
			System.err.println("Exception occured in Router!! Exception: " + e);
		}
	}
}

/**
 * A container that represents an address. It either wraps an emulated node, or
 * a queue of messages to a failed node.
 */
class NodeContainer {
	private boolean up;
	private EmulatedNode node;
	private List<Packet> downQueue;

	NodeContainer(EmulatedNode node) {
		up = true;
		this.node = node;
		downQueue = new LinkedList<Packet>();
	}

	synchronized void quit(LinkedList<Packet> queue) {
		if(node != null) {
			node.finish();
		}
		up = false;
		node = null;

		if(queue != null) {
			downQueue.addAll(queue);
		}
	}

	synchronized void restart(EmulatedNode node) {
		if(this.node != null) {
			this.node.finish();
		}
		up = true;
		this.node = node;

		for (Packet pkt : downQueue) {
			send(pkt);
		}
	}

	synchronized boolean isUp() {
		return up;
	}

	synchronized boolean hasConflict(InetAddress ipAddress, int port) {
		if (up == false) {
			return false;
		}
		return ipAddress.equals(node.getIPAddress()) && port == node.getPort();
	}

	synchronized void send(Packet p) {
		if (up) {
			System.out.println("Sending: " + p);
			if (!node.send(p)) {
				System.out.println("Failed to send because node is going down.  Queueing: " + p);
				downQueue.add(p);
			}
		} else {
			System.out.println("Queueing to failed node: " + p);
			downQueue.add(p);
		}
	}
}
