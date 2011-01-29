import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RTNPacket extends RPCPacket{
	
	public RTNPacket(int protocol, byte[] payload) throws IllegalArgumentException {
		super(protocol, payload, MAX_PAYLOAD_SIZE, !RTNProtocol.isRTNProtocol(protocol));
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
			
			out.write(payload, 0, payload.length);

			out.flush();
			out.close();
			return byteStream.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Unpacks a byte array to create a RTNPacket object
	 * Assumes the array has been formatted using pack method in RIOPacket
	 * @param packet String representation of the transport packet
	 * @return RIOPacket object created or null if the byte[] representation was corrupted
	 */
	public static RTNPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			int bytesRead = in.read(payload, 0, payload.length);

			if (bytesRead != payload.length || bytesRead == -1 && payload.length == 0) {
				return null;
			}

			return new RTNPacket(protocol, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
