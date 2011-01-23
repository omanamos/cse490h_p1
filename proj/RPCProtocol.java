
/**
 *	These protocols are sent by clients and received by servers.
 */
public class RPCProtocol extends Protocol{
	public static final int CREATE = 6;
	public static final int GET = 7;
	public static final int PUT = 8;
	public static final int APPEND = 9;
	public static final int DELETE = 10;
	public static final int INV = 11;
	
	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isRPCProtocol(p);
	}
	
	public static boolean isRPCProtocol(int p){
		return p == CREATE || p == GET || p == PUT || p == APPEND || p == DELETE || p == INV;
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
		case CREATE: return "create";
		case GET: return "get";
		case PUT: return "put";
		case APPEND: return "append";
		case DELETE: return "delete";
		case INV: return "invalidate";
		default:
			return Protocol.protocolToString(protocol);
		}
	}
}
