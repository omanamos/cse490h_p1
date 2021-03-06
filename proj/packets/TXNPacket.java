package packets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import protocols.TXNProtocol;

import utils.Queueable;

public class TXNPacket extends Queueable{

	public static final int MAX_PACKET_SIZE = RIOPacket.MAX_PAYLOAD_SIZE;
	public static final int HEADER_SIZE = 5;
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

	protected int protocol;
	private int seqNum;
	
	protected byte[] payload;

	/**
	 * Constructing a new RIO packet.
	 * @param protocol The type of packet
	 * @param seqNum The sequence number of the packet
	 * @param payload The payload of the packet.
	 * @param sessionId The sessionId between the sender and receiver
	 */
	public TXNPacket(int protocol, int seqNum, byte[] payload) throws IllegalArgumentException {
		this(protocol, seqNum, payload, MAX_PAYLOAD_SIZE, !TXNProtocol.isTXNProtocol(protocol));
	}
	
	protected TXNPacket(int protocol, int seqNum, byte[] payload, int maxPayloadSize, boolean hasInvalidProtocol) throws IllegalArgumentException {
		if (hasInvalidProtocol) {
			throw new IllegalArgumentException("Illegal arguments given to Packet: Invalid protocol: " + protocol);
		}else if(payload.length > maxPayloadSize){
			throw new IllegalArgumentException("Illegal arguments given to Packet: Payload to large");
		}
		this.protocol = protocol;
		this.seqNum = seqNum;
		this.payload = payload;
	}

	/**
	 * @return The protocol number
	 */
	public int getProtocol() {
		return this.protocol;
	}
	
	/**
	 * @return The payload
	 */
	public byte[] getPayload() {
		return this.payload;
	}
	
	public int getSeqNum(){
		return this.seqNum;
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

			out.writeByte(this.protocol);
			out.writeInt(this.seqNum);
			
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
	public static TXNPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();
			int seqNum = in.readInt();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			in.read(payload, 0, payload.length);

			return new TXNPacket(protocol, seqNum, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
