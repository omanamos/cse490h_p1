import java.util.ArrayList;
import java.util.HashMap;

import edu.washington.cs.cse490h.lib.Utility;


public class CacheCoherenceLayer {
	
	private final static int MASTER_NODE = 0;
	
	private ReliableInOrderMsgLayer RIOLayer;
	private DistNode n;
	private HashMap<String, File> cache;
	
	public CacheCoherenceLayer(RIONode n) {
		this.n = (DistNode)n;
		this.RIOLayer = new ReliableInOrderMsgLayer( n, this );
		this.cache = new HashMap<String, File>();
	}
	
	public void sendCC( int server, int commandType, byte[] payload) {
		this.RIOLayer.sendRIO(server, commandType, payload);
	}

	public void receiveSession(Integer from, SessionPacket unpack) {
		this.RIOLayer.receiveSession(from, unpack);
	}


	public void receiveRPC(Integer from, RPCPacket unpack) {
		this.RIOLayer.receiveRPC(from, unpack);
	}

	public void receiveAck(Integer from, ACKPacket msg) {
		this.RIOLayer.receiveAck(from, msg);
	}

	public void onRIOReceive(int from, int protocol, byte[] payload) {
		String filename = Utility.byteArrayToString(payload);
		File f = this.cache.get(filename);
		switch(protocol){
			case RPCProtocol.CREATE:
				if(f == null) {
					this.n.onCCReceive(from, RPCProtocol.CREATE, payload);
					this.cache.put( filename, new MasterFile(filename) );
					MasterFile mf = (MasterFile) this.cache.get(filename);
					mf.changePermissions(from, File.RW);
					this.sendCC(from, RPCProtocol.PUT, Utility.stringToByteArray(filename));
				} else {
					this.n.returnError(from, protocol, filename, Error.ERR_11);
				}
			case RPCProtocol.GET:
				if(f == null) {
					this.n.onCCReceive(from, RPCProtocol.CREATE, payload);
					this.cache.put( filename, new MasterFile(filename) );
					MasterFile mf = (MasterFile) this.cache.get(filename);
					HashMap<Integer, ArrayList<Integer>> updates = mf.getUpdates(from, File.RW);
					if (updates.containsKey(File.RO)){
						for(Integer i : updates.get(File.RO))
							this.sendCC(i, RPCProtocol.INV, Utility.stringToByteArray(filename));
						
					} else {
						this.sendCC(updates.get(File.RW).get(0), RPCProtocol.GET, Utility.stringToByteArray(filename));
					}
				}
				

			case RPCProtocol.PUT:
				if(f == null){
					this.n.onCCReceive(from, RPCProtocol.PUT, payload);
					this.cache.put(filename, new File(File.RW, filename));
				} else if(f instanceof MasterFile){
					
					//I am a master node
				}else{
					//I am a slave node
				}
		}
		n.onCCReceive(from, protocol, payload);
	}
	
	
	
	
	
	
	
	public void create(String filename){
		this.sendCC(MASTER_NODE, RPCProtocol.CREATE, Utility.stringToByteArray(filename) );
	}
	
	public void get(String filename){
		this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(filename));
	}
	
	//doing nothing with contents right now
	public void put(String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename);
	//	if(payload.length > RPCPacket.MAX_PAYLOAD_SIZE)
		//	System.out.println(DistNode.buildErrorString(this.n.addr, MASTER_NODE, RPCProtocol.PUT, filename, Error.ERR_30));
		this.sendCC(MASTER_NODE, RPCProtocol.GET, payload);
	}
	
	public void append(String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename);
		//if(payload.length > RPCPacket.MAX_PAYLOAD_SIZE)
		//	System.out.println(DistNode.buildErrorString(this.n.addr, MASTER_NODE, RPCProtocol.APPEND, filename, Error.ERR_30));
		this.sendCC(MASTER_NODE, RPCProtocol.GET, payload);
	}
	
	public void delete(String filename){
		this.sendCC(MASTER_NODE, RPCProtocol.DELETE, Utility.stringToByteArray(filename));
	}
	

}
