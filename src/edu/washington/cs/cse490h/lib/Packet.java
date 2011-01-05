package edu.washington.cs.cse490h.lib;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * <pre>   
 * Packet defines the MessageLayer packet headers and some constants.
 * </pre>   
 */
public class Packet {
	public static final int HEADER_SIZE = 8;
	public static final int MAX_PACKET_SIZE = java.lang.Integer.MAX_VALUE;  // bytes
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;  // bytes

	private int dest;
	private int src;
	private int protocol;
	private int flags;
	
	protected static final byte FIN = 1;
	protected static final byte REPLAY = 2;
	
	private byte[] payload;

	static class CorruptPacketException extends IOException {
		private static final long serialVersionUID = -8471415959243642433L;
	}

	/**
	 * Constructs a new packet that is meant to carry data
	 * 
	 * @param dest
	 *            The destination address.
	 * @param src
	 *            The source address.
	 * @param protocol
	 *            What type of packet this is.
	 * @param payload
	 *            The payload of the packet.
	 * @throws IllegalArgumentException
	 *             If the given arguments are invalid
	 */
	protected Packet(int dest, int src, int protocol, byte[] payload) throws IllegalArgumentException {
		if(!isValid(dest, src, payload.length + Packet.HEADER_SIZE)) {
			throw new IllegalArgumentException("Arguments passed to constructor of Packet are invalid");
		}

		this.dest = dest;
		this.src = src;
		this.protocol = protocol;
		this.flags = 0;
		this.payload = payload;
	}

	/**
	 * Constructs a new Packet and allows setting of the flags. This should only
	 * be used to unpack arbitrary packets while reading a byte stream
	 * 
	 * @param dest
	 *            The destination address.
	 * @param src
	 *            The source address.
	 * @param protocol
	 *            What type of packet this is.
	 * @param flags
	 *            The flags byte of the packet
	 * @param payload
	 *            The payload of the packet.
	 * @throws IllegalArgumentException
	 */
	private Packet(int dest, int src, int protocol, int flags, byte[] payload) throws IllegalArgumentException {
		if (!isValid(dest, src, payload.length + Packet.HEADER_SIZE)) {
			throw new IllegalArgumentException("Arguments passed to constructor of Packet are invalid");
		}

		this.dest = dest;
		this.src = src;
		this.protocol = protocol;
		this.flags = flags;
		this.payload = payload;
	}

	/**
	 * Constructs a new FIN packet that is meant to signal a node quit in
	 * Emulator mode and facilitate connection closing
	 * 
	 * @param node
	 *            The address of the node that is leaving the network
	 * @return The FIN packet
	 */
	protected static Packet getFinPacket(int node) {
		return new Packet(node, node, 0, FIN, new byte[0]);
	}

	/**
	 * Constructs a new replay packet that is meant to represent non packet
	 * external input for replays
	 * 
	 * @param addr
	 *            The address value
	 * @param protocol
	 *            The replay protocol (see Replay class)
	 * @param payload
	 *            The payload of the packet
	 * @return The REPLAY packet
	 */
	protected static Packet getReplayPacket(int addr, int protocol, byte[] payload) {
		return new Packet(addr, addr, protocol, REPLAY, payload);
	}

	@Override
	public String toString() {
		if((flags & FIN) != 0) {
			return new String("Packet: " + src + ": FIN");
		}
		return new String("Packet: " + src + "->" + dest + " protocol: " + protocol + 
				" contents: " + Utility.byteArrayToString(payload));
	}

	/**
	 * @return The virtual address of the destination node
	 */
	protected int getDest() {
		return dest;
	}

	/**
	 * @return The virtual address of the src node
	 */
	protected int getSrc() {
		return src;
	}

	/**
	 * @return The protocol used for this packet
	 */
	protected int getProtocol() {
		return protocol;
	}

	/**
	 * @return The flags for this packet
	 */
	protected int getFlags() {
		return flags;
	}
	
	/**
	 * @return The payload of this packet
	 */
	protected byte[] getPayload() {
		return payload;
	}

	/**
	 * Convert the Packet object into a byte array for sending over the wire.
	 * Format:
	 *        destination address: 1 byte
	 *        source address: 1 byte
	 *        protocol: 1 byte
	 *        flags: 1 byte
	 *        payload length: 4 byte
	 *        payload: <= MAX_PAYLOAD_SIZE bytes
	 * @return A byte[] for transporting over the wire. Null if failed to pack for some reason
	 */
	protected byte[] pack() {	
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);
			out.writeByte(dest);
			out.writeByte(src);
			out.writeByte(protocol);
			out.writeByte(flags);
			out.writeInt(payload.length);

			out.write(payload, 0, payload.length);
			
			out.flush();
			return byteStream.toByteArray();
		} catch(IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Unpacks a byte array to create a Packet object. Assumes the array has
	 * been formatted using pack method in Packet
	 * 
	 * @param packedPacket
	 *            String representation of the packet
	 * @return Packet Object created or null if the byte[] is empty
	 * @throws CorruptPacketException
	 *             If the byte[] representation was corrupted
	 */
	protected static Packet unpack(byte[] packedPacket) throws CorruptPacketException{
		return unpack( new DataInputStream(new ByteArrayInputStream(packedPacket)) );
	}

	/**
	 * Reads an input stream to create a Packet object. Assumes the array has
	 * been formatted using pack method in Packet
	 * 
	 * @param stream
	 *            Input stream (probably from a socket)
	 * @return Packet object created or null if the stream is at EOF
	 * @throws CorruptPacketException
	 *             If the stream contains a corrupted packet
	 */
	protected static Packet unpack(InputStream stream) throws CorruptPacketException {
		return unpack( new DataInputStream(stream) );
	}

	/**
	 * General method to read a data stream to create a Packet object. Assumes
	 * the array has been formatted using pack method in Packet. Called by the
	 * other unpack methods.
	 * 
	 * @param in
	 *            The data stream
	 * @return Packet object created or null if the input stream is at EOF
	 * @throws CorruptPacketException
	 *             If the stream contains a corrupted packet
	 */
	private static Packet unpack(DataInputStream in) throws CorruptPacketException {
		try {
			// If the end of stream is reached normally, this will be -1
			int dest = in.read();
			if(dest == -1) {
				// return null if we were at EOF
				return null;
			}
			int src = in.read();
			int protocol = in.read();
			int flags = in.read();
			int payloadLength = in.readInt();
			
			byte[] payload = new byte[payloadLength];
			in.readFully(payload);

			return new Packet(dest, src, protocol, flags, payload);
		}catch(Exception e) {
			e.printStackTrace();
		}
		throw new CorruptPacketException();
	}

	/**
	 * Tests if the address is a valid one
	 * 
	 * @param addr
	 *            Address to check
	 * @return True is address is valid, else false
	 */
	protected static boolean validAddress(int addr) {
		return (addr <= Manager.MAX_ADDRESS && addr >= 0);
	}

	/**
	 * Tests if this Packet is valid or not
	 * 
	 * @return True if packet is valid, else false
	 */
	protected boolean isValid() {
		return isValid(dest, src, payload.length + HEADER_SIZE);
	}

	/**
	 * Tests if a Packet is valid or not
	 * 
	 * @param dest
	 *            The destination of the packet
	 * @param src
	 *            The source of the packet
	 * @param size
	 *            The total size of the packet
	 * @return True if the packet is valid, false otherwise
	 */
	private static boolean isValid(int dest, int src, int size) {
		return (dest <= Manager.MAX_ADDRESS && dest >= 0   &&
				Packet.validAddress(src)           &&
				size <= MAX_PACKET_SIZE);

	}

	protected String toSynopticString() {
		return new String("src:" + src + " dest:" + dest + " proto:" + protocol + 
				" contents:" + Utility.byteArrayToString(payload));
	}
}
