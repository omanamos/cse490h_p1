package edu.washington.cs.cse490h.lib;

import java.net.Socket;
import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * <pre>   
 * Keeps track of information about an emulated node
 * The emulated is the Router's abstraction of the client.  It reliably handles messages to and from a single client.
 * </pre>   
 */
public class EmulatedNode implements Runnable{
	private Router parent;
	private Socket socket;
	private OutputStream out;
	private InputStream in;
	private int addr;
	
	// TODO: implement and use 
	// A node's local vector clock -- one per node.
	// public VectorTime vtime = null;

	//This is the IP address and port of the node that this EmulatedNode represents
	private InetAddress ipAddress;
	private int port;
	
	private boolean cleanQuit;
	private boolean finished;

	/**
	 * Create a new EmulatedNode
	 * 
	 * @param parent
	 *            A pointer to the router so that it can signal failures
	 * @param socket
	 *            The socket to use to talk to the emulated node
	 * @param addr
	 *            The virtual address of the emulated node
	 * @param ipAddress
	 *            The IP address of the machine that the node is on
	 * @param port
	 *            The port that the emulated node is on
	 * @throws IOException
	 *             If creation of the socket fails
	 */
	public EmulatedNode(Router parent, Socket socket, int addr, InetAddress ipAddress, int port) throws IOException {
		this.parent = parent;
		this.socket = socket;
		this.addr = addr;
		// this.vtime = new VectorTime(Manager.MAX_ADDRESS);
		this.ipAddress = ipAddress;
		this.port = port;

		out = socket.getOutputStream();
		in = socket.getInputStream();

		cleanQuit = false;
		finished = false;

		Thread t = new Thread(this);
		t.start();
	}

	public void run() {
		try {
			while(isUp()) {
				Packet packet = Packet.unpack(in);

				if(packet == null) {
					// The other side closed the connection
					break;
				}

				if((packet.getFlags() & Packet.FIN) != 0) {
					// start termination protocol
					LinkedList<Packet> queue = close();
					// if a send occurs here it's OK cause finished = true
					// we don't call nodeQuit inside close because it could cause deadlock
					parent.nodeQuit(addr, queue);
				} else if(packet.getDest() == Manager.BROADCAST_ADDRESS) {
					Collection<Integer> c = parent.emulatedNodes.keySet();
					System.out.println("Broadcasting: " + packet);

					synchronized(parent.emulatedNodes) {
						for(Integer dest: c) {
							if(dest != addr) {
								parent.emulatedNodes.get(dest).send(packet);
							}
						}
					}
				} else {
					parent.emulatedNodes.get(packet.getDest()).send(packet);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if(!cleanQuit) {
			// If the termination protocol did not finish, quit
			try {
				socket.close();
			} catch (IOException e) {
			}
			if(parent != null) {
				parent.nodeQuit(addr, null);
			}
		}
	}

	/**
	 * Called by other EmulatedNodes to send a packet to this node.
	 * 
	 * Locking methodology is that this and close() should not call other
	 * synchronized blocks (except each other)
	 * 
	 * @param pkt
	 *            The packet to send
	 * @return true if the send was successful, false otherwise. This is to let
	 *         the router know when the node is closing and packets should be
	 *         queued
	 */
	protected synchronized boolean send(Packet pkt){
		if(finished) {
			return false;
		}

		try {
			out.write(pkt.pack());
			out.flush();
		} catch (IOException e) {
			finished = true;
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * Close the the connection to the node server cleanly.
	 * 
	 * Locking methodology is that this and close() should not call other
	 * synchronized blocks (except each other)
	 */
	private synchronized LinkedList<Packet> close() {
		Packet fin = Packet.getFinPacket(addr);
		// send our FIN packet to signal that no new packets will arrive
		send(fin);
		finished = true;

		try {
			// make sure that we don't send new packets
			socket.shutdownOutput();

			LinkedList<Packet> queue = new LinkedList<Packet>();

			while(true) {
				// grab all the undelivered messages
				Packet packet = Packet.unpack(in);

				if(packet == null) {
					throw new IOException("Corrupted packet.  Cannot recover from misalignment.");
				}

				if((packet.getFlags() & Packet.FIN) != 0) {
					// if we get the second FIN, everything is done
					cleanQuit = true;
					socket.close();
					return queue;
				} else {
					queue.add(packet);
				}
			}
		} catch (IOException e) {
			System.err.println("Encountered IO Exception while trying to close socket in EmulatedNode: "
					+ addr + "Exception Stack Trace:");
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get the virtual address of this node
	 * @return The virtual address of this node
	 */
	protected int getAddr() {
		return addr;
	}

	/**
	 * Get the IP address of the machine that this emulated node is on
	 * @return The IP address of the machine that this emulated node is on
	 */
	protected InetAddress getIPAddress() {
		return ipAddress;
	}

	/**
	 * Get the port that this emulated node is using to talk to its neighbors
	 * @return The port that this emulated node is using to talk to its neighbors
	 */
	protected int getPort() {
		return port;
	}

	/**
	 * Return a string containing details of this emulated node
	 * @return A string containing details of this emulated node
	 */
	public String toString() {
		return new String("<TCP: " + socket.getInetAddress() + ":" + socket.getPort() + " Fish: " + addr + ">");
	}

	/**
	 * Called by the router to tell the emulated node to stop and that the
	 * router has already removed the emulated node
	 */
	protected void finish() {
		// parent is set to null only when the router wants to close the
		// connection first
		parent = null;
		finished = true;
	}
	
	/**
	 * Check if the emulated node is still alive
	 * @return True if the node is still alive
	 */
	protected boolean isUp() {
		return (!socket.isClosed() &&
				!socket.isInputShutdown() &&
				!finished);
	}
}
