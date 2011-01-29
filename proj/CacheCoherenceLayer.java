import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	public void sendCC( int server, int protocol, byte[] payload) {
		RPCPacket pkt = new RPCPacket(protocol, payload);
		this.RIOLayer.sendRIO(server, Protocol.RPC, pkt.pack());
	}
	
	public void onRPCReceive(int from, byte[] payload) {
		RPCPacket packet = RPCPacket.unpack(payload);
		if(this.n.addr == MASTER_NODE)
			masterReceive(from, packet);
		else
			slaveReceive(packet);
	}
	
	
	public void masterReceive(int from, RPCPacket packet){
		String fileName;
		MasterFile f;
		String data;
		int permissions;
		String[] split;
		Queueable pkt;
		
		switch(packet.getProtocol()){
			case RPCProtocol.GET: //Payload structure: "[filename] [permissions]"
				split = Utility.byteArrayToString(packet.getPayload()).split(" ");
				fileName = split[0];
				permissions = Integer.parseInt(split[1]);
				
				f = (MasterFile)this.getFileFromCache(fileName);
				
				if(f.execute(packet)){ //If the file isn't already executing a request
					if(f.getState() == File.INV){ //The file doesn't exist.
						String error = DistNode.buildErrorString(MASTER_NODE, from, packet.getProtocol(), fileName, Error.ERR_10);
						this.sendCC(from, RPCProtocol.ERROR, Utility.stringToByteArray(error));
						return;
					}
					
					Map<Integer, List<Integer>> updates = f.getUpdates(from, permissions);
					
					if(updates.containsKey(File.RW)){
						int ownerAddr = updates.get(File.RW).get(0);
						byte[] payload = Utility.stringToByteArray(fileName + " " + permissions);
						
						this.sendCC(ownerAddr, RPCProtocol.GET, payload); //Send RF to owner and wait for RD back
						
					}else if(updates.containsKey(File.RO)){ //There are only RO copies checked out
						
						if(permissions == File.RO){	//This is a RQ
							try{
								String contents = this.n.get(fileName);
								byte[] payload = Utility.stringToByteArray(fileName + " " + permissions + " " + contents);
								this.sendCC(from, RPCProtocol.PUT, payload);
							}catch(Exception e){
								this.sendCC(from, RPCProtocol.ERROR, Utility.stringToByteArray("Fatal Error: File: " + fileName + " doesn't exist on master node"));
							}
						}else{			//This is a WQ
							
							for(Integer addr : updates.get(File.RO)){	//Send invalidates to every client that has a RO copy and wait for ICs back
								this.sendCC(addr, RPCProtocol.INV, Utility.stringToByteArray(fileName));
							}
						}
					}else{ //No one has a copy checked out
						try{
							String contents = this.n.get(fileName);
							byte[] payload = Utility.stringToByteArray(fileName + " " + permissions + " " + contents);
							this.sendCC(from, RPCProtocol.PUT, payload);
						}catch(Exception e){
							this.sendCC(from, RPCProtocol.ERROR, Utility.stringToByteArray("Fatal Error: File: " + fileName + " doesn't exist on master node"));
						}
					}
				}
				
				break;
			case RPCProtocol.PUT: //Payload structure: "[filename] [permissions] [contents]"
				data = Utility.byteArrayToString(packet.getPayload());
				int index = data.indexOf(' ');
				fileName = data.substring(0, index);
				index = data.indexOf(' ', fileName.length());
				permissions = Integer.parseInt(data.substring(fileName.length(), index));
				String contents = data.substring(index + 1);
				
				f = (MasterFile)this.getFileFromCache(fileName);
				
				//Update file contents
				try {
					this.n.write(fileName, contents, false, true);
				} catch (IOException e) {
					this.n.printError("Fatal Error: Couldn't replace file " + fileName);
				}
				
				pkt = f.execute();	//Get the RQ that requested the file & dequeue it
				f.changePermissions(pkt.getSource(), permissions);
				f.setState(permissions);
				
				//Return the Data to the client that requested it
				this.sendCC(pkt.getSource(), RPCProtocol.PUT, packet.getPayload());
				
				this.executePacketQueue(f);
				
				break;
			case RPCProtocol.CONF:
				split = Utility.byteArrayToString(packet.getPayload()).split(" ");
				fileName = split[0];
				permissions = Integer.parseInt(split[1]);
				
				f = (MasterFile)this.getFileFromCache(fileName);
				if(permissions == File.INV){ //This is a IC
					f.changePermissions(from, File.INV);
					Map<Integer, List<Integer>> updates = f.getUpdates(from, permissions);
					if(updates.containsKey(File.RW)){	//Error in code
						this.n.printError("Fatal Error: Invalid state, sent INV to owner instead of WF.");
					}else if(!updates.containsKey(File.RO)){ //There aren't any more RO copies out there
						pkt = f.execute();
						
						//Return the Data to the client that requested it
						this.sendCC(pkt.getSource(), RPCProtocol.PUT, packet.getPayload());
						
						this.executePacketQueue(f);
					}
				}
				break;
			case RPCProtocol.CREATE:
				break;//TODO
			case RPCProtocol.DELETE:
				break;//TODO
		}
	}
	
	public void executePacketQueue(MasterFile f){
		//TODO
	}
	
	public void slaveReceive(RPCPacket packet){
		String fileName;
		File f;
		String data;
		int permissions;
		switch(packet.getProtocol()){
			case RPCProtocol.GET: //Payload structure: "[filename] [permissions]"
				data = Utility.byteArrayToString(packet.getPayload());
				String[] dataSplit = data.split(" ");
				
				permissions = Integer.parseInt(dataSplit[1]);
				fileName = dataSplit[0];
				
				f = this.getFileFromCache(fileName);
				if(f.getState() != File.INV){ //This is a RF
					if(permissions == File.RO){
						f.setState(File.RO);
						String payload;
						
						try {
							payload = fileName + " " + File.RO + " " + this.n.get(fileName);
							sendCC(MASTER_NODE, RPCProtocol.PUT, Utility.stringToByteArray(payload));
						} catch (IOException e) {
							this.n.printError("Fatal Error: Missing File " + fileName);
						}				
			
					} else { //This is a WF
						f.setState(File.INV);
						String payload;
						
						try {
							payload = fileName + " " + File.RW + " " + this.n.get(fileName);
							sendCC(MASTER_NODE, RPCProtocol.PUT, Utility.stringToByteArray(payload));
						} catch (IOException e) {
							this.n.printError("Fatal Error: Missing File " + fileName);
						}	
					}
				} else {
					sendCC(MASTER_NODE, RPCProtocol.DELETE, Utility.stringToByteArray(fileName));
				}		
				break;
			case RPCProtocol.INV: //Payload structure: "[filename]"
				 fileName = Utility.byteArrayToString(packet.getPayload());
				 f = this.getFileFromCache(fileName);
				 f.setState(File.INV);
				 
				 String payload = fileName + " " + File.INV;
				 sendCC(MASTER_NODE, RPCProtocol.CONF,  Utility.stringToByteArray(payload));
				
				break;
			case RPCProtocol.PUT: //Payload structure: "[filename] [permissions] [contents]"
				data = Utility.byteArrayToString(packet.getPayload());
				int index = data.indexOf(' ');
				fileName = data.substring(0, index);
				index = data.indexOf(' ', fileName.length());
				permissions = Integer.parseInt(data.substring(fileName.length(), index));
				String contents = data.substring(index + 1);
				
				f = this.getFileFromCache(fileName);
				f.setState(permissions);
				
				Command c = (Command)f.execute(); //Get command that originally requested this Query
				try {
					this.n.write(fileName, contents, false, true);
				} catch (IOException e) {
					this.n.printError("Fatal Error: Couldn't create file " + fileName);
				}
				
				if(permissions == File.RO){	//This was a RQ
					this.n.printData(contents);
					//FIXME: Don't need to send RC. sendCC(MASTER_NODE, RPCProtocol.CONF, Utility.stringToByteArray(fileName + " " + File.RO));
				} else {					//This was a WQ
					this.n.printSuccess(c);
					//FIXME: Don't need to send WC. sendCC(MASTER_NODE, RPCProtocol.CONF, Utility.stringToByteArray(fileName + " " + File.RW));
				}
				executeCommandQueue(f);
				break;
			case RPCProtocol.ERROR:
				this.n.printError(Utility.byteArrayToString(packet.getPayload()));
				break;
		}
		
	}
	
	public void executeCommandQueue(File f){
		Command c = (Command)f.peek();
		boolean stop = false;
		while(c != null && !stop){
			switch(c.getType()){
			case Command.APPEND:
				stop = !append(c, f);
				break;
			case Command.CREATE:
				stop = !create(c, f);
				break;
			case Command.DELETE:
				stop = !delete(c, f);
				break;
			case Command.PUT:
				stop = !put(c, f);
				break;
			case Command.GET:
				stop = !get(c, f);
				break;
			}
			c = (Command)f.peek();
		}
	}
	

	//CLIENT METHODS FOR INITIATING COMMANDS
	public boolean create(String filename){
		boolean rtn = false;
		Command c = new Command(MASTER_NODE, Command.CREATE, filename);
		
		File f = getFileFromCache( filename );
		
		if(f.execute(c)){
			return create(c, f);
		}
		
		return rtn;
	}
	
	private boolean create(Command c, File f){
		if(f.getState() == File.INV){
			this.sendCC(MASTER_NODE, RPCProtocol.CREATE, Utility.stringToByteArray(f.getName()));
			return false;
		}else{
			f.execute();
			this.n.printError(c, Error.ERR_11);
			return true;
		}
	}
	
	public boolean get(String filename){
		Command c = new Command(MASTER_NODE, Command.GET, filename);
		
		File f = getFileFromCache( filename );
		
		if(f.execute(c)){
			return get(c, f);
		}
		
		return false;
	}
	
	private boolean get(Command c, File f){
		if(f.getState() == File.INV){
			this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName() + " " + File.RO)); //RQ
			return false;
		}else{

			try {
				f.execute();
				this.n.printData(this.n.get(f.getName()));
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}	
	}
	
	public boolean put(String filename, String content){
		Command c = new Command(MASTER_NODE, Command.PUT, filename, content);
		
		File f = getFileFromCache( filename );
		
		if(f.execute(c)){
			return put(c, f);
		}
		
		return false;
	}
	
	private boolean put(Command c, File f){
		if(f.getState() != File.RW){
			this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName() + " " + File.RW)); //WQ
			return false;
		}else{
			f.execute();
			try {
				this.n.write(f.getName(), c.getContents(), false, false);
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}
		
	}
	
	public boolean append(String filename, String content){
		Command c = new Command(MASTER_NODE, Command.APPEND, filename, content);
		
		File f = getFileFromCache( filename );
		
		if(f.execute(c)) {
			return append(c, f);
		}
		return false;
	}
	
	private boolean append(Command c, File f){
		if(f.getState() != File.RW) {
			this.sendCC(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName() + " " + File.RW)); //WQ
			return false;
		}else{
			f.execute();
			try {
				this.n.write(f.getName(), c.getContents(), true, false);
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}
		
	}
	
	public boolean delete(String filename){
		Command c = new Command(MASTER_NODE, Command.DELETE, filename);
	
		File f = getFileFromCache( filename );
		
		if(f.execute(c)) {
			return delete(c, f);
		}
		return false;
	}
	
	private boolean delete(Command c, File f){
		if(f.getState() != File.RW) {
			this.sendCC(MASTER_NODE, RPCProtocol.DELETE, Utility.stringToByteArray(f.getName() + " " + File.RW));
			return false;//WQ
		} else {
			f.execute();
			try {
				this.n.delete(f.getName());
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
			
		}
		
	}
	
	private File getFileFromCache(String filename) {
		File f = this.cache.get(filename);
		
		if(f == null){
			f = new File(File.INV, filename);
			this.cache.put(filename, f);
		}
		return f;
	}
	

}
