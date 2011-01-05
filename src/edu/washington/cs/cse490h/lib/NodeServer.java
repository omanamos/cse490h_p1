package edu.washington.cs.cse490h.lib;

import java.lang.Thread;
import java.util.ArrayList;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * In a separate thread, this class listens to incoming messages from the router
 * and stores the data received.
 */
public class NodeServer implements Runnable {
	private Socket socket;
	private ArrayList<Packet> packetsReceived;
	private InputStream in;
	private OutputStream out;
	private int address;
	private Emulator parent;

	// termination state variables
	private boolean gotFIN;
	private boolean finished;

	protected int getAddress() {
		return address;
	}

	/**
	 * Creates a new NodeServer.
	 * 
	 * @param name
	 *            The name of the machine on which the router resides
	 * @param port
	 *            The port on which the router is listening
	 * @param parent
	 *            The emulator that this server is associated to
	 * @throws IOException
	 *             If there is a problem creating the socket
	 */
	public NodeServer(String name, int port, Emulator parent) throws IOException{
		socket = new Socket(name, port);
		packetsReceived = new ArrayList<Packet>();
		in = socket.getInputStream();
		out = socket.getOutputStream();
		gotFIN = false;
		this.parent = parent;
		finished = false;

		address = in.read();

		if(address != Manager.BROADCAST_ADDRESS) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void run() {
		try {
			while(!finished && !socket.isClosed()) {
				Packet packet = Packet.unpack(in);

				if(packet == null) {
					// The other side closed the connection
					break;
				}
				//System.out.println("Received packet: " + packet);

				if((packet.getFlags() & Packet.FIN) != 0) {
					gotFIN = true;
					return;
				}

				// store it
				storePacket(packet);
			}
		}catch(IOException e) {
			System.err.println("Encountered IOException when trying to receive packet.");
			e.printStackTrace();
		}

		try {
			socket.close();
		} catch (IOException e) {
		}
		synchronized (parent) {
			if (parent != null) {
				parent.IOFinish();
			}
		}
	}

	/**
	 * Gets the first packet stored
	 * 
	 * @return The first packet stored
	 */
	protected Packet getPacket() {
		synchronized (packetsReceived) {
			if (packetsReceived.isEmpty()) {
				return null;
			}
			return packetsReceived.remove(0);
		}
	}

	/**
	 * Send a packet to the EmulatedNode at the router
	 * 
	 * @param pkt
	 *            Serialized version of the packet
	 */
	protected void send(byte[] pkt) {
		try {
			out.write(pkt);
			out.flush();
		} catch (IOException e) {
			finished = true;
			e.printStackTrace();
		}
	}

	/**
	 * Store a packet to be fetched by the emulator later.
	 * 
	 * @param packet
	 *            The packet to store
	 */
	private void storePacket(Packet packet) {
		synchronized (packetsReceived) {
			packetsReceived.add(packet);
		}
	}
	
	/**
	 * Close the connection cleanly.
	 */
	protected void close() {
		try {
			Packet fin = Packet.getFinPacket(address);
			send(fin.pack());

			// wait until the router acknowledges our closing attempt
			while (!gotFIN) {
				if (socket.isClosed()) {
					// to prevent a race condition:
					if (!gotFIN) {
						throw new IOException("Socket closed before we got a FIN back!");
					}
				}
				Thread.yield();
			}

			// send back all the in-transit messages
			synchronized (packetsReceived) {
				for (Packet pkt : packetsReceived) {
					send(pkt.pack());
				}
			}
			for (Packet pkt : parent.inTransitMsgs) {
				send(pkt.pack());
			}

			// send a second fin to finalize the close
			send(fin.pack());
		} catch (IOException e) {
			System.err.println("Error while sending back packets.");
			e.printStackTrace();
		}

		packetsReceived.clear();
		synchronized (parent) {
			parent = null;
		}
		finished = true;

		try {
			socket.close();
		} catch (IOException e) {
			System.err.println("Error while closing socket!");
			e.printStackTrace();
		}
	}
}
