package edu.washington.cs.cse490h.lib;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.washington.cs.cse490h.lib.Packet.CorruptPacketException;

/**
 * Class that handles deterministic replay in both the simulator and emulator.
 * All external events (seed generation, user commands, incoming messages) are
 * recorded and replayed from a log file. File commands are assumed to not
 * change.
 * 
 * This utility is meant to provide an opportunity to output more debugging
 * information about a previous execution. In the emulator environment
 * particularly, the execution may not be valid if the implementation changes.
 */
public class Replay {
	static class ReplayException extends RuntimeException {
		private static final long serialVersionUID = -3490291954459395688L;
		ReplayException(String msg) {
			super(msg);
		}
	}

	protected static Manager parent;

	private static DataInputStream replayIn;
	protected static DataOutputStream replayOut;
	private static BufferedReader keyboard;
	private static boolean controlInput;	//TODO: enable replay without user input

	// protocol values for replay packets
	protected static final int NULL = 0;
	protected static final int ADDR = 1;
	protected static final int USER = 2;

	/**
	 * Initialize the replay.
	 * 
	 * @param in
	 *            The datastream for the replay input file, or null if this is
	 *            not a replay execution
	 * @return The seed for the replay if this is a replay execution, -1
	 *         otherwise
	 * @throws IOException
	 *             If there is a problem with the keyboard BufferedReader
	 */
	protected static long init(DataInputStream in, boolean controlInput) throws IOException {
		replayIn = in;
		Replay.controlInput = controlInput;

		if (in != null) {
			return Replay.replayIn.readLong();
		} else {
			keyboard = new BufferedReader(new InputStreamReader(System.in));
			return -1;
		}
	}

	/**
	 * Test if this is a replay execution
	 * 
	 * @return true if this is a replay execution, false otherwise
	 */
	protected static boolean isReplaying() {
		return replayIn != null;
	}

	/**
	 * Get a null packet that delimits the end of a sequence of incoming
	 * messages
	 * 
	 * @return A Packet object that represents a null packet
	 */
	protected static Packet getNullPacket() {
		return Packet.getReplayPacket(Manager.BROADCAST_ADDRESS, NULL, new byte[0]);
	}

	/**
	 * Test if a packet is a null packet.
	 * 
	 * @param pkt
	 *            The packet to test
	 * @return true if the packet is a null packet, false otherwise
	 */
	protected static boolean isNullPacket(Packet pkt) {
		return ((pkt.getFlags() & Packet.REPLAY) != 0)
				&& (pkt.getProtocol() == NULL);
	}

	/**
	 * Get an address packet that represents an address assignment.
	 * 
	 * @param addr
	 *            The address that was assigned
	 * @return The address packet object
	 */
	protected static Packet getAddrPacket(int addr) {
		return Packet.getReplayPacket(addr, ADDR, new byte[0]);
	}

	/**
	 * Test if a packet is an address packet
	 * 
	 * @param pkt
	 *            The packet to test
	 * @return true if the packet is an address packet, false otherwise
	 */
	protected static boolean isAddrPacket(Packet pkt) {
		return ((pkt.getFlags() & Packet.REPLAY) != 0)
				&& (pkt.getProtocol() == ADDR);
	}

	/**
	 * Get a user packet that represents user input.
	 * 
	 * @param input
	 *            The input that the user provided
	 * @return The user packet object
	 */
	protected static Packet getUserPacket(String input) {
		return Packet.getReplayPacket(Manager.BROADCAST_ADDRESS, USER, Utility.stringToByteArray(input));
	}

	/**
	 * Test if a packet is a user packet
	 * 
	 * @param pkt
	 *            The packet to test
	 * @return true if the packet is a user packet, false otherwise
	 */
	protected static boolean isUserPacket(Packet pkt) {
		return ((pkt.getFlags() & Packet.REPLAY) != 0)
				&& (pkt.getProtocol() == USER);
	}

	/**
	 * Read the next packet from the replay input file. If there are no more
	 * packets, replay is stopped.
	 * 
	 * @return The packet that was read
	 * @throws CorruptPacketException
	 *             If there is an error in the read packet
	 */
	protected static Packet getPacket() throws CorruptPacketException {
		Packet pkt;
		
		do {
			pkt = Packet.unpack(replayIn);
			if (pkt == null) {
				System.out.println("Reached end of deterministic replay.  Stopping...");
				parent.stop();
			}
		} while (!controlInput && isUserPacket(pkt));
		
		//System.out.println("DEBUG: got packet: " + pkt);
		
		return pkt;
	}

	/**
	 * Get a line of user input from either the keyboard or the replay input
	 * file.
	 * 
	 * @return The read line
	 * @throws IOException
	 *             If there was an error while reading the line.
	 */
	protected static String getLine() throws IOException{
		String input;

		if (replayIn != null && controlInput) {
			// get a packet from the replay input file
			Packet line;
			try {
				line = getPacket();
			} catch (CorruptPacketException e) {
				throw new ReplayException("Error while reading replay file");
			}

			// verify that the packet is a user packet
			if (Replay.isUserPacket(line)) {
				input = Utility.byteArrayToString(line.getPayload());
			} else {
				throw new Replay.ReplayException("Expected user input, got: " + line.toString());
			}

			System.out.println("Replaying user input: '" + input + "'");
		} else {
			// get a line from the keyboard
			input = keyboard.readLine();
		}

		if (replayOut != null) {
			// record the user input to the replay output file
			try {
				if (input != null) {
					replayOut.write(Replay.getUserPacket(input).pack());
				} else {
					replayOut.write(Replay.getUserPacket("").pack());
				}
			} catch (IOException e) {
				throw new ReplayException("Error while writing replay file");
			}
		}

		return input;
	}
}
