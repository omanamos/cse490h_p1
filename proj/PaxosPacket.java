import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class PaxosPacket extends TXNPacket {

	public static final int HEADER_SIZE = 5;
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;
	
	private int proposalNumber;
	
	public PaxosPacket(int protocol, int proposalNumber, byte[] payload){
		super(protocol, 0, payload, MAX_PAYLOAD_SIZE, !PaxosProtocol.isPaxosProtocol(protocol));
		this.proposalNumber = proposalNumber;
	}
	
	public int getProposalNumber(){
		return this.proposalNumber;
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
			out.writeInt(proposalNumber);
			
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
	public static PaxosPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();
			int proposalNumber = in.readInt();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			in.read(payload, 0, payload.length);

			return new PaxosPacket(protocol, proposalNumber, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
