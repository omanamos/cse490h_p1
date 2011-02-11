import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.text.Utilities;

import edu.washington.cs.cse490h.lib.Utility;


public class TransactionLayer {

	private final static int MASTER_NODE = 0;
	
	private DistNode n;
	private ReliableInOrderMsgLayer RIOLayer;
	private Map<String, File> cache;
	private int lastTXNnum;
	private Transaction txn;
	
	public TransactionLayer(RIONode n, ReliableInOrderMsgLayer RIOLayer){
		this.cache = new HashMap<String, File>();
		this.n = (DistNode)n;
		this.RIOLayer = RIOLayer;
		this.lastTXNnum = n.addr;
	}

	public void send(int server, int protocol, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, payload);
		this.RIOLayer.sendRIO(server, Protocol.TXN, pkt.pack());
	}
	
	public void onRPCReceive(int from, byte[] payload) {
		TXNPacket packet = TXNPacket.unpack(payload);
		if(this.n.addr == MASTER_NODE)
			masterReceive(from, packet);
		else
			slaveReceive(packet);
	}
	
	public void onTimeout(int from, byte[] payload){
		
	}
	
	private void masterReceive(int from, TXNPacket pkt){
		MasterFile f;
		
		switch(pkt.getProtocol()){
			case TXNProtocol.WQ:
				f = (MasterFile)this.getFileFromCache(Utility.byteArrayToString(pkt.getPayload()));
				
				for(Integer client : f){
					f.changePermissions(client, MasterFile.WF);
					this.send(client, TXNProtocol.WF, Utility.stringToByteArray(f.getName()));
				}
				
				break;
			case TXNProtocol.ERROR:
				break;
			case TXNProtocol.WD:
				break;
			case TXNProtocol.COMMIT_DATA:
				break;
			case TXNProtocol.COMMIT:
				break;
		}
	}
	
	//TODO: execute command queue somewhere in here	
	private void slaveReceive(TXNPacket pkt){
		String fileName;
		
		switch(pkt.getProtocol()){
			case TXNProtocol.WF:
				fileName = Utility.byteArrayToString(pkt.getPayload());
				try {
					String contents = this.txn.getVersion(this.n.get(fileName));
					this.send(MASTER_NODE, TXNProtocol.WD, Utility.stringToByteArray(fileName + " " + contents));
				} catch (IOException e) {
					this.send(MASTER_NODE, TXNProtocol.ERROR, Utility.stringToByteArray(fileName + " " + Error.ERR_10));
				}
				break;
			case TXNProtocol.WD:
				String contents = Utility.byteArrayToString(pkt.getPayload());
				int i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				int lastSpace = i + 1;
				int version = Integer.parseInt(contents.substring(lastSpace, i));
				contents = contents.substring(i + 1);
				
				File f = this.getFileFromCache(fileName);
				Command c = (Command)f.execute(); //Get command that originally requested this Query
				try {
					this.n.write(fileName, contents, false, true);
					f.setState(File.RW);
					f.setVersion(version);
					this.txn.add(c);
					this.n.printSuccess(c);
				} catch (IOException e) {
					this.n.printError("Fatal Error: Couldn't update file: " + fileName + " to version: " + version);
				}
				
				executeCommandQueue(f);
				break;
			case TXNProtocol.ABORT:
				this.abort();
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


	

	/*=====================================================
	 * Methods DistNode uses to talk to TXNLayer
	 *=====================================================*/
	
	public boolean get(String filename){
		File f = this.cache.get( filename );
		Command c = new Command(MASTER_NODE, Command.GET, f);
		
		if(f.execute(c)){
			return get(c, f);
		}
		
		return false;
	}
	
	private boolean get(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName()));
			return false;
		}else{

			try {
				f.execute();
				this.n.printData(this.txn.getVersion( this.n.get(f.getName()) ));
				this.txn.add( c );
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}	
	}

	//TODO: Decide what to do for creates/deletes and transactions
	public boolean create(String filename){
		boolean rtn = false;
		File f = getFileFromCache( filename );
		Command c = new Command(MASTER_NODE, Command.CREATE, f, "");
		
		if(f.execute(c)){
			return create(c, f);
		}
		
		return rtn;
	}
	
	private boolean create(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, RPCProtocol.CREATE, Utility.stringToByteArray(f.getName()));
			return false;
		}else{
			f.execute();
			this.n.printError(c, Error.ERR_11);
			return true;
		}
	}

	public boolean put(String filename, String content){
		File f = getFileFromCache( filename );
		Command c = new Command(MASTER_NODE, Command.PUT, f, content);
		
		if(f.execute(c)){
			return put(c, f);
		}
		
		return false;
	}
	
	private boolean put(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName() + " " + File.RW)); //WQ
			return false;
		}else{
			f.execute();
			this.txn.add( c );
			this.n.printSuccess(c);

			return true;
		}
	}

	public boolean append(String filename, String content){
		File f = getFileFromCache( filename );
		Command c = new Command(MASTER_NODE, Command.APPEND, f, content);
		
		if(f.execute(c)) {
			return append(c, f);
		}
		return false;
	}
	
	private boolean append(Command c, File f){
		if(f.getState() != File.RW) {
			this.send(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName() + " " + File.RW)); //WQ
			return false;
		}else{
			f.execute();
			this.txn.add(c);
			this.n.printSuccess(c);
			return true;
		}
		
	}

	//TODO: Decide what to do for creates/deletes and transactions
	public boolean delete(String filename){
		File f = getFileFromCache( filename );
		Command c = new Command(MASTER_NODE, Command.DELETE, f);
	
		if(f.execute(c)) {
			return delete(c, f);
		}
		return false;
	}
	
	private boolean delete(Command c, File f){
		if(f.getState() != File.RW) {
			this.send(MASTER_NODE, RPCProtocol.DELETE, Utility.stringToByteArray(f.getName()));
			return false;//WQ
		} else {
			f.execute();
			try {
				this.n.delete(f.getName());
				this.n.printSuccess(c);
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
			
		}
	}

	public void abort() {
		this.txn = null;
	}

	public void commit() {
		//Send all of our commands to the master node
		for( Command c : this.txn ) {
			String payload = c.getType() + " " + c.getFileName();
			if( c.getType() == Command.PUT || c.getType() == Command.APPEND ) {
				payload += " " + c.getContents();
			}
			this.send(MASTER_NODE, TXNProtocol.COMMIT_DATA, Utility.stringToByteArray(payload));
		}
		//Send the final commit message
		this.send(MASTER_NODE, TXNProtocol.COMMIT, Utility.stringToByteArray(this.txn.id + "") );
	}

	public void start() {
		int newTXNnum = this.lastTXNnum + RIONode.NUM_NODES;
		
		//start a new transaction by creating a new transaction object
		this.txn = new Transaction( newTXNnum );
	}
	
	private File getFileFromCache(String fileName) {
		File f = this.cache.get(fileName);
		
		if(f == null){
			f = this.n.addr == MASTER_NODE ? new MasterFile(fileName) : new File(File.INV, fileName);
			this.cache.put(fileName, f);
		}
		return f;
	}

}
