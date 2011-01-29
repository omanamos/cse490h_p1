import java.io.IOException;
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
		this.RIOLayer = n.getRIOLayer();
		this.cache = new HashMap<String, File>();
	}
	
	public void sendCC( int server, int commandType, byte[] payload) {
		this.RIOLayer.sendRIO(server, commandType, payload);
	}
	
	public void returnCC( int source, int protocol, byte[] payload){
		this.RIOLayer.sendRIO(source, protocol, payload);
	}

	public void receiveSession(Integer from, SessionPacket unpack) {
		this.RIOLayer.receiveSession(from, unpack);
	}

	public void onRPCReceive(int from, int protocol, byte[] payload) {
		String filename = Utility.byteArrayToString(payload);
		File f = this.cache.get(filename);
		switch(protocol){
			case RPCProtocol.CREATE:
				if(f == null) {
					//this.n.onCCReceive(from, RPCProtocol.CREATE, payload);
					this.cache.put( filename, new MasterFile(filename) );
					MasterFile mf = (MasterFile) this.cache.get(filename);
					mf.changePermissions(from, File.RW);
					this.sendCC(from, RPCProtocol.PUT, Utility.stringToByteArray(filename));
				} else {
					this.n.returnError(from, protocol, filename, Error.ERR_11);
				}
			case RPCProtocol.GET:
				if(f == null) {
					//this.n.onCCReceive(from, RPCProtocol.CREATE, payload);
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
					//this.n.onCCReceive(from, RPCProtocol.PUT, payload);
					this.cache.put(filename, new File(File.RW, filename));
				} else if(f instanceof MasterFile){
					
					//I am a master node
				}else{
					//I am a slave node
				}
		}
		//n.onCCReceive(from, protocol, payload);
	}
	
	
	
	
	
	
	//CLIENT METHODS FOR INITIATING COMMANDS
	public void create(String filename){
		Command c = new Command(MASTER_NODE, Command.CREATE, filename);
		File f = this.cache.get(filename);
		
		if(f == null){
			f = new File(File.INV, filename);
			this.cache.put(filename, f);
		}
		
		if(f.execute(c)){
			if(f.getState() != File.INV){
				this.sendCC(MASTER_NODE, RPCProtocol.CREATE, Utility.stringToByteArray(filename));
			}else{
				f.execute();
				this.n.printError(c, Error.ERR_11);
			}
		}
	}
	
	public void get(String filename){
		Command c = new Command(MASTER_NODE, Command.GET, filename);
		File f = this.cache.get(filename);
		
		if(f == null){
			f = new File(File.INV, filename);
			this.cache.put(filename, f);
		}
		
		if(f.execute(c)){
			if(f.getState() == File.INV){
				this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(filename + " " + File.RO));
			}else{
				try {
					f.execute();
					this.n.printData(this.n.get(filename));
				} catch (IOException e) {
					this.n.printError(c, Error.ERR_10);
				}
			}
		}
	}
	
	public void put(String fileName, String content){
		Command c = new Command(MASTER_NODE, Command.PUT, fileName, content);
		File f = this.cache.get(fileName);
		
		if(f == null){
			f = new File(File.INV, fileName);
			this.cache.put(fileName, f);
		}
		
		if(f.execute(c)){
			if(f.getState() != File.RW){
				this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(fileName + " " + File.RW));
			}else{
				f.execute();
				try {
					this.n.write(fileName, content, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void append(String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename + " " + File.RW);
		//if(payload.length > RPCPacket.MAX_PAYLOAD_SIZE)
		//	System.out.println(DistNode.buildErrorString(this.n.addr, MASTER_NODE, RPCProtocol.APPEND, filename, Error.ERR_30));
		this.sendCC(MASTER_NODE, RPCProtocol.GET, payload);
	}
	
	public void delete(String filename){
		this.sendCC(MASTER_NODE, RPCProtocol.DELETE, Utility.stringToByteArray(filename));
	}
	

}
