import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
				
				break;
			case TXNProtocol.ABORT:
				this.abort(true);
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


	

	private boolean put(Command c, File f) {
		return false;
	}
	
	
	private boolean delete(Command c, File f) {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean create(Command c, File f) {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean append(Command c, File f) {
		// TODO Auto-generated method stub
		return false;
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
				this.n.printData(this.n.get(f.getName()));
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}	
	}

	public void create(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void put(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void append(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void delete(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void abort(boolean retry) {
		// TODO Auto-generated method stub
		
	}

	public void commit() {
		// TODO Auto-generated method stub
		
	}

	public void start() {
		int newTXNnum = this.lastTXNnum + RIONode.NUM_NODES;
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
