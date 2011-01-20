
public class CCProtocol extends Protocol {
	
	public final static int READ_QUERY = 5;
	public final static int READ_FORWARD = 6;
	public final static int READ_DATA = 7;
	public final static int READ_CONFIRM = 8; 
	
	public final static int WRITE_QUERY = 9;
	public final static int WRITE_FORWARD = 10;
	public final static int WRITE_DATA = 11;
	public final static int WRITE_CONFIRM = 12;
	
	public final static int INVALIDATE = 13;
	public final static int INVALIDATE_CONFIRM = 14;
	
	
	
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isCCProtocol(p);
	}
	
	public static boolean isCCProtocol(int p){
		return p == READ_QUERY || p == READ_FORWARD || p == READ_DATA || p == READ_CONFIRM ||
			p == WRITE_QUERY || p == WRITE_FORWARD || p == WRITE_DATA || p == WRITE_CONFIRM ||
			p == INVALIDATE || p == INVALIDATE_CONFIRM;

	}

	/**
	 * Returns a string representation of the given protocol. Can be used for
	 * debugging
	 * 
	 * @param protocol
	 *            The protocol whose string representation is desired
	 * @return The string representation of the given protocol.
	 *         "Unknown Protocol" if the protocol is not recognized
	 */
	public static String protocolToString(int protocol) {
		switch (protocol) {
		case READ_QUERY: return "read query";
		case READ_FORWARD: return "read forward";
		case READ_DATA: return "read data";
		case READ_CONFIRM: return "read confirm";
		
		case WRITE_QUERY: return "write query";
		case WRITE_FORWARD: return "qrite forward";
		case WRITE_DATA: return "write_data";
		case WRITE_CONFIRM: return "write_confirm";
		
		case INVALIDATE: return "invalidate";
		case INVALIDATE_CONFIRM: return "invalidate_confirm";
		
		default:
			return Protocol.protocolToString(protocol);
		}

	}
}
