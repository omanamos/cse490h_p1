import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Packet;

/**
 * This conveys the header for reliable, in-order message transfer. This is
 * carried in the payload of a Packet, and in turn the data being transferred is
 * carried in the payload of the RIOPacket packet.
 */
public class RIOPacket {

	public static final int MAX_PACKET_SIZE = Packet.MAX_PAYLOAD_SIZE;
	public static final int HEADER_SIZE = 9;
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

	protected int protocol;
	protected int seqNum;
	protected int sessionID;
	protected byte[] payload;

	/**
	 * Constructing a new RIO packet.
	 * @param type The type of packet. Either 
	 * @param seqNum The sequence number of the packet
	 * @param payload The payload of the packet.
	 */
	public RIOPacket(int protocol, int seqNum, byte[] payload, int sessionID) throws IllegalArgumentException {
		this(protocol, seqNum, payload, sessionID, MAX_PAYLOAD_SIZE, !Protocol.isPktProtocolValid(protocol));
	}
	
	protected RIOPacket(int protocol, int seqNum, byte[] payload, int sessionID, int maxPayloadSize, boolean hasInvalidProtocol) throws IllegalArgumentException {
		if (hasInvalidProtocol) {
			throw new IllegalArgumentException("Illegal arguments given to Packet: Invalid protocol");
		}else if(payload.length > maxPayloadSize){
			throw new IllegalArgumentException("Illegal arguments given to Packet: Payload to large");
		}

		this.protocol = protocol;
		this.seqNum = seqNum;
		this.payload = payload;
	}
	
	public int getSessionID(){
		return this.sessionID;
	}
	
	/**
	 * @return The protocol number
	 */
	public int getProtocol() {
		return this.protocol;
	}
	
	/**
	 * @return The sequence number
	 */
	public int getSeqNum() {
		return this.seqNum;
	}

	/**
	 * @return The payload
	 */
	public byte[] getPayload() {
		return this.payload;
	}
	
	/**
	 * Convert the RIOPacket packet object into a byte array for sending over the wire.
	 * Format:
	 *        protocol = 1 byte
	 *        sequence number = 4 bytes
	 *        payload <= MAX_PAYLOAD_SIZE bytes
	 * @return A byte[] for transporting over the wire. Null if failed to pack for some reason
	 */
	public byte[] pack() {
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);

			out.writeByte(protocol);
			out.writeInt(seqNum);

			out.write(payload, 0, payload.length);

			out.flush();
			out.close();
			return byteStream.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Unpacks a byte array to create a RIOPacket object
	 * Assumes the array has been formatted using pack method in RIOPacket
	 * @param packet String representation of the transport packet
	 * @return RIOPacket object created or null if the byte[] representation was corrupted
	 */
	public static RIOPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();
			int seqNum = in.readInt();
			int sid = in.readInt();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			int bytesRead = in.read(payload, 0, payload.length);

			if (bytesRead != payload.length || bytesRead == -1 && payload.length == 0) {
				return null;
			}

			return new RIOPacket(protocol, seqNum, payload, sid);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
