import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class SessionPacket extends RPCPacket {
	/**
	 * Constructing a new RIO packet.
	 * @param protocol The type of packet
	 * @param seqNum The sequence number of the packet
	 * @param payload The payload of the packet.
	 * @param sessionId The sessionId between the sender and receiver
	 */
	public SessionPacket(int protocol, byte[] payload) throws IllegalArgumentException {
		super(protocol, payload, MAX_PAYLOAD_SIZE, !SessionProtocol.isSessionProtocol(protocol));
	}
	
	/**
	 * Unpacks a byte array to create a SessionPacket object
	 * Assumes the array has been formatted using pack method in RIOPacket
	 * @param packet String representation of the transport packet
	 * @return SessionPacket object created or null if the byte[] representation was corrupted
	 */
	public static SessionPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();
			
			byte[] payload = new byte[packet.length - HEADER_SIZE];
			int bytesRead = in.read(payload, 0, payload.length);
			
			if (bytesRead != payload.length && !(bytesRead == -1 && payload.length == 0)) {
				return null;
			}

			return new SessionPacket(protocol, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
