import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Utility;


public class TransactionLayer {

	public final static int MASTER_NODE = 0;
	
	private DistNode n;
	private ReliableInOrderMsgLayer RIOLayer;
	private PaxosLayer paxos;
	
	private Map<String, File> cache;
	
	//CLIENT FIELD
	private int lastTXNnum;
	//CLIENT FIELD
	private Transaction txn;
	
	//SERVER FIELDS
	private Set<Integer> assumedCrashed;
	/**
	 * key = addr of node uploading its commit
	 * value = list of commands received so far of the commit
	 */
	private Map<Integer, List<Command>> commitQueue;
	/**
	 * key = addr of node trying to commit
	 * value = commit status
	 */
	private Map<Integer, Commit> waitingQueue;

	private TimeoutManager timeout;
	
	
	public TransactionLayer(RIONode n, ReliableInOrderMsgLayer RIOLayer){
		this.cache = new HashMap<String, File>();
		this.n = (DistNode)n;
		this.RIOLayer = RIOLayer;
		this.lastTXNnum = n.addr;
		this.timeout = new TimeoutManager(5, this.n, this);
		
		if(this.n.addr == MASTER_NODE){
			this.paxos = new PaxosLayer(this);
			this.commitQueue = new HashMap<Integer, List<Command>>();
			this.waitingQueue = new HashMap<Integer, Commit>();
			this.assumedCrashed = new HashSet<Integer>();
		}
	}
	
	public void send(int server, int protocol, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, timeout.nextSeqNum(), payload);
		this.RIOLayer.sendRIO(server, Protocol.TXN, pkt.pack());
	}
	
	/**
	 * Starts or stops a heartbeat to a given node
	 * @param dest node to start or stop the heartbeat
	 * @param heartbeat true = start, false = stop
	 */
	public void setHB(int dest, boolean heartbeat){
		this.RIOLayer.setHB(dest, heartbeat);
	}
	
	public void onAck(int from, byte[] payload){
		TXNPacket pkt = TXNPacket.unpack(payload);
		
		this.timeout.createTimeoutListener(pkt);
	}
	
	public void onReceive(int from, byte[] payload) {
		TXNPacket packet = TXNPacket.unpack(payload);
		if(packet.getProtocol() == TXNProtocol.PAXOS){
			this.paxos.onReceive(from, packet.getPayload());
		}else if(this.n.addr == MASTER_NODE){
			masterReceive(from, packet);
		}else
			slaveReceive(packet);
	}
	
	/**
	 * Called when a message times out on the RIOLayer
	 * @param dest
	 * @param payload
	 */
	public void onRIOTimeout(int dest, byte[] payload){
		TXNPacket pkt = TXNPacket.unpack(payload);
		if(this.n.addr == MASTER_NODE){ //This is the server
			if(pkt.getProtocol() == TXNProtocol.HB){
				//This is a heartbeat that timed out, meaning either a client has crashed or we assume it has.
				//We must tell all commits waiting on this client to abort and flag this client as crashed, so if
				//it didn't and tries to commit, it will have to abort.
				this.assumedCrashed.add(dest);
				for(String fileName : this.cache.keySet()){
					MasterFile f = (MasterFile)this.cache.get(fileName);
					f.changePermissions(dest, File.INV);
				}
				for(Integer committer : waitingQueue.keySet()){
					Commit com = waitingQueue.get(committer);
					for(Integer dep : com){
						if(dep == dest){
							this.send(committer, TXNProtocol.ABORT, new byte[0]);
							break;
						}
					}
				}
			}else if(pkt.getProtocol() == TXNProtocol.WF){ 
				//a write forward timed out. We should check to see if we are waiting on any other WF, if not, send a response to the requester.
				String fileName = Utility.byteArrayToString(pkt.getPayload());
				MasterFile f = (MasterFile)this.getFileFromCache(fileName);
				f.changePermissions(dest, File.INV);
				if(!f.isWaiting()){
					Update u = f.chooseProp(f.requestor);
					this.send(f.requestor, TXNProtocol.WD, Utility.stringToByteArray(fileName + " " + u.version + " " + u.contents));
					f.requestor = -1;
				}
			}else
				this.n.printError(DistNode.buildErrorString(dest, this.n.addr, pkt.getProtocol(), Utility.byteArrayToString(pkt.getPayload()), Error.ERR_20));
		}else{ //this is the client and we should just print out the message
			if(pkt.getProtocol() == TXNProtocol.START)
				this.txn = null;
			else if(pkt.getProtocol() == TXNProtocol.WD || pkt.getProtocol() == TXNProtocol.ERROR){
				this.send(dest, pkt.getProtocol(), pkt.getPayload());
				return;
			}
			this.n.printError(DistNode.buildErrorString(dest, this.n.addr, pkt.getProtocol(), Utility.byteArrayToString(pkt.getPayload()), Error.ERR_20));
		}
	}
	
	private void masterReceive(int from, TXNPacket pkt){
		MasterFile f;
		String contents, fileName;
		int i, lastSpace;
		
		switch(pkt.getProtocol()){
			case TXNProtocol.WQ://payload structure: "[fileName]"
				fileName = Utility.byteArrayToString(pkt.getPayload());
				f = (MasterFile)this.getFileFromCache(fileName);
				
				if(f.getState() == File.INV){ //the file doesn't exist on the server, return an error
					String payload = fileName + " " + Error.ERR_10;
					this.send(from, TXNProtocol.ERROR, Utility.stringToByteArray(payload));
				}else if(!f.isCheckedOut()){ //The file hasn't been checked out by anyone, return the last committed version.
					try{
						contents = this.n.get(fileName);
						byte[] payload = Utility.stringToByteArray(f.getName() + " " + f.getVersion() + " " + contents);
						f.addDep(from, new Update(contents, f.getVersion(), MASTER_NODE));
						f.changePermissions(from, MasterFile.FREE);
						this.send(from, TXNProtocol.WD, payload);
					}catch(IOException e){
						String payload = fileName + " " + Error.ERR_10;
						this.send(from, TXNProtocol.ERROR, Utility.stringToByteArray(payload));
					}
				}else if(f.isWaiting()){ //The server is currently waiting for some WFs to return from clients. Enqueue this request to execute once they have returned.
					pkt.setSource(from);
					f.execute(pkt);
				}else{ //The server must send out WFs to all clients that have copies of the file, and pick the highest version that is returned.
					f.requestor = from;
					try{
						f.propose(this.n.get(fileName), f.getVersion(), MASTER_NODE);
					}catch(IOException e){
						f.propose("", f.getVersion(), MASTER_NODE);
					}
					for(Integer client : f){
						if(!this.assumedCrashed.contains(client)){ //only ask clients who aren't assumed to be crashed
							f.changePermissions(client, MasterFile.WF);
							this.send(client, TXNProtocol.WF, Utility.stringToByteArray(f.getName()));
						}
					}
				}
				break;
			case TXNProtocol.WD: //A client has returned its most recent version to the server.
				contents = Utility.byteArrayToString(pkt.getPayload());
				i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				lastSpace = i + 1;
				i = contents.indexOf(' ', lastSpace);
				int version = Integer.parseInt(contents.substring(lastSpace, i));
				contents = i == contents.length() - 1 ? "" : contents.substring(i + 1);
				f = (MasterFile)this.getFileFromCache(fileName);
				
				f.changePermissions(from, MasterFile.FREE);
				if(f.getVersion() < version){
					f.propose(contents, version, from);
				}
				if(!f.isWaiting()){
					Update u = f.chooseProp(f.requestor);
					byte[] payload = Utility.stringToByteArray(fileName + " " + u.version + " " + u.contents);
					this.send(f.requestor, TXNProtocol.WD, payload);
					f.changePermissions(f.requestor, MasterFile.FREE);
					f.requestor = -1;
					while(f.peek() != null){ //also return queued requests for the file
						TXNPacket p = (TXNPacket)f.execute();
						this.send(p.getSource(), TXNProtocol.WD, payload);
						f.changePermissions(p.getSource(), MasterFile.FREE);
					}
				}
				break;
			case TXNProtocol.ERROR:
				String[] parts = Utility.byteArrayToString(pkt.getPayload()).split(" ");
				
				if(parts.length == 2){
					fileName = parts[0];
					int errCode = Integer.parseInt(parts[1]);
					if(errCode == Error.ERR_10){
						//This is a client saying it doesn't have a file after the server sent it a WF
						//This means the MasterFile has some corrupted state, change permissions for that client to invalid.
						f = (MasterFile)this.getFileFromCache(fileName);
						f.changePermissions(from, File.INV);
					}
				}
				break;
			case TXNProtocol.COMMIT_DATA:
				if(!this.commitQueue.containsKey(from))
					this.commitQueue.put(from, new ArrayList<Command>());
				
				contents = Utility.byteArrayToString(pkt.getPayload());
				i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				lastSpace = i + 1;
				i = contents.indexOf(' ', lastSpace);
				int commandType = Integer.parseInt(contents.substring(lastSpace, i));
				f = (MasterFile)this.getFileFromCache(fileName);
				
				Command c = null;
				if(commandType == Command.APPEND || commandType == Command.PUT || commandType == Command.UPDATE){
					contents = i == contents.length() ? "" : contents.substring(i + 1);
					c = new Command(MASTER_NODE, commandType, f, contents);
				} else {
					c = new Command(MASTER_NODE, commandType, f);
				}
				
				this.commitQueue.get(from).add(c);
				break;
			case TXNProtocol.COMMIT:
				this.commit(from, Integer.parseInt(Utility.byteArrayToString(pkt.getPayload())));
				break;
			case TXNProtocol.CREATE:
				fileName = Utility.byteArrayToString(pkt.getPayload());
				f = (MasterFile)this.getFileFromCache(fileName);
				
				if(f.getState() == File.INV){
					f.addDep(from, new Update("", 0, MASTER_NODE));
					f.setState(File.RW);
					f.changePermissions(from, MasterFile.FREE);
					String payload = fileName + " " + f.getVersion() + " ";
					this.send(from, TXNProtocol.WD, Utility.stringToByteArray(payload));
				}else{
					String payload = DistNode.buildErrorString(this.n.addr, from, TXNProtocol.CREATE, fileName, Error.ERR_11);
					this.send(from, TXNProtocol.ERROR, Utility.stringToByteArray(payload));
				}
				break;
			case TXNProtocol.START:
				this.assumedCrashed.remove(from);
				for(String fName : this.cache.keySet()){
					f = (MasterFile)this.cache.get(fName);
					f.changePermissions(from, File.INV);
				}
				for(Integer committer : waitingQueue.keySet()){
					Commit com = waitingQueue.get(committer);
					for(Integer dep : com){
						if(dep == from){
							this.send(committer, TXNProtocol.ABORT, new byte[0]);
							break;
						}
					}
				}
				this.send(from, TXNProtocol.START, new byte[0]);
				break;
			case TXNProtocol.ABORT:
				for(String fName : this.cache.keySet()){
					f = (MasterFile)this.cache.get(fName);
					f.abort(from);
				}
				for(Integer committer : waitingQueue.keySet()){
					Commit com = waitingQueue.get(committer);
					for(Integer dep : com){
						if(dep == from){
							this.commit(committer, com.getLog());
							break;
						}
					}
				}
				this.send(from, TXNProtocol.ABORT, new byte[0]);
				break;
		}
	}
	
	private void commit(int client, int size){
		List<Command> commands = this.commitQueue.get(client);
		
		if(this.assumedCrashed.contains(client)){
			this.assumedCrashed.remove(client);
			for(String fName : this.cache.keySet()){
				MasterFile f = (MasterFile)this.cache.get(fName);
				f.commit(client);
			}
			this.send(client, TXNProtocol.ABORT, new byte[0]);
		}else if( commands != null && size != commands.size()){
			this.send(client, TXNProtocol.ERROR, Utility.stringToByteArray(" " + Error.ERROR_STRINGS[Error.ERR_40]));
		} else if( commands == null ) {
			this.send(client, TXNProtocol.COMMIT, new byte[0]);
		}else {
			Log log = new Log(client, commands);
			this.commit(client, log);
		}
		this.commitQueue.get(client).clear();
	}
	
	private void commit(int client, Log log){
		Commit c = new Commit(client, log, this.assumedCrashed);
		
		if(c.abort()){
			for(MasterFile f : log)
				f.abort(client);
			this.send(client, TXNProtocol.ABORT, new byte[0]);
		}else if(c.isWaiting()){
			//add commit to queue and send heartbeat to nodes that the commit is waiting for
			for(Integer addr : c){
				this.setHB(addr, true);
			}
			this.waitingQueue.put(client, c);
		}else{
			//push changes to disk and put most recent version in memory in MasterFile
			try{
				Map<MasterFile, Update> updates = new HashMap<MasterFile, Update>();
				for(MasterFile f : log){
						int version = f.getVersion();
						Update u = f.getInitialVersion(client);
						String contents = u.contents;
						boolean deleted = false;
						
						for(Command cmd : log.getCommands(f)){
							 if(cmd.getType() == Command.CREATE){
								 contents = "";
								 deleted = false;
								 version++;
							 }else if(cmd.getType() == Command.APPEND){
								contents += cmd.getContents();
								version++;
							}else if(cmd.getType() == Command.PUT){
								contents = cmd.getContents();
								version++;
							} else if(cmd.getType() == Command.DELETE ) {
								version++;
								contents = "";
								deleted = true;
							}
						}
						if(!deleted)
							updates.put(f, new Update(contents, version, client));
						else
							updates.put(f, new Update("", version, -1));
						
				}
				this.n.write(".wh_log", Update.toString(updates), false, true);
				this.pushUpdatesToDisk(updates);
			}catch(IOException e){
				e.printStackTrace();
				return;
			}
			this.send(client, TXNProtocol.COMMIT, new byte[0]);
			
			this.setHB(client, false);
			//Allow any transactions dependent on this one to commit
			for(Integer committer : waitingQueue.keySet()){
				Commit com = waitingQueue.get(committer);
				for(Integer dep : com){
					if(dep == client){
						com.remove(client);
						if(!com.isWaiting()){
							this.waitingQueue.remove(committer);
							this.commit(committer, com.getLog());
						}
						break;
					}
				}
			}
		}
	}
	
	public void pushUpdatesToDisk(Map<MasterFile, Update> updates) throws IOException{
		for(MasterFile f : updates.keySet()){
			Update u = updates.get(f);
			if(u.source == -1){
				f.setState(File.INV);
				this.n.delete(f.getName());
			}else{
				f.setState(File.RW);
				this.n.write(f.getName(), u.contents, false, true);
			}
			f.setVersion(u.version);
			f.commit(u.source);
		}
		this.n.delete(".wh_log");
	}
	
	private void slaveReceive(TXNPacket pkt){
		String fileName;
		File f;
		
		switch(pkt.getProtocol()){
			case TXNProtocol.WF:
				fileName = Utility.byteArrayToString(pkt.getPayload());
				f = this.getFileFromCache(fileName);
				
				if(f.getState() == File.INV){
					byte[] payload = Utility.stringToByteArray(fileName + " " + -1 + " ");
					this.send(MASTER_NODE, TXNProtocol.WD, payload);
				}else{
					try {
						byte[] payload = this.txn.getVersion(f, this.n.get(fileName));
						this.send(MASTER_NODE, TXNProtocol.WD, payload);
					} catch (IOException e) {
						this.send(MASTER_NODE, TXNProtocol.ERROR, Utility.stringToByteArray(fileName + " " + Error.ERR_10));
					}
				}
				break;
			case TXNProtocol.WD:
				String contents = Utility.byteArrayToString(pkt.getPayload());
				int i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				int lastSpace = i + 1;
				i = contents.indexOf(' ', lastSpace);
				int version = Integer.parseInt(contents.substring(lastSpace, i));
				contents = i == contents.length() - 1 ? "" : contents.substring(i + 1);
				
				f = this.getFileFromCache(fileName);
				Command c = (Command)f.execute(); //Get command that originally requested this Query
				try {
					this.n.write(fileName, contents, false, true);
					f.setState(File.RW);
					f.setVersion(version);
					this.txn.add(new Command(MASTER_NODE, Command.UPDATE, f, version + ""));
					this.txn.add(c);
					this.txnExecute();
				} catch (IOException e) {
					this.n.printError("Fatal Error: Couldn't update file: " + fileName + " to version: " + version);
				}
				
				executeCommandQueue(f);
				break;
			case TXNProtocol.ABORT:
				this.abort(false);
				break;
			case TXNProtocol.COMMIT:
				this.commitChangesLocally();
				this.commitConfirm();
				break;
			case TXNProtocol.ERROR:
				contents = Utility.byteArrayToString(pkt.getPayload());
				i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				contents = contents.substring(i + 1);
				
				f = this.getFileFromCache(fileName);
				c = (Command)f.execute();
				try{
					int code = Integer.parseInt(contents.trim());
					this.n.printError(c, code);
				}catch(Exception e){
					this.n.printError(contents);
				}
				executeCommandQueue(f);
				break;
			case TXNProtocol.START:
				this.txn.isStarted = true;
				this.n.printData("Success: Transaction Started");
				break;
		}
	}
	
	//CLIENT METHOD
	public void commitChangesLocally() {
		for( Command c : this.txn ) {
			try {
				int type = c.getType();
				switch( type ) {
				case Command.GET :
					this.n.printData(this.n.get(c.getFileName() ));
					break;
				case Command.APPEND:
					this.n.write(c.getFileName(), c.getContents(), true, false);
					break;
				case Command.PUT:
					this.n.write(c.getFileName(), c.getContents(), false, false);
					break;
				case Command.DELETE:
					this.n.delete(c.getFileName());
					break;
				}
			} catch(IOException e) {
				this.n.printError("Fatal Error: When applying commit locally on: " + c.getFileName() + "  command: " + c ) ;
			}
			if(c.getType() != Command.UPDATE)
				this.n.printSuccess(c);
		}
		
		this.n.printData("Success: Transaction successfully committed.");
	}
	
	//CLIENT METHOD
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
	 * CLIENT METHODS
	 * Methods DistNode uses to talk to TXNLayer
	 *=====================================================*/
	
	public boolean get(String fileName){
		if( assertTXNStarted() && notCommited() ) {
			File f = this.getFileFromCache(fileName);
			Command c = new Command(MASTER_NODE, Command.GET, f);
			
			if(f.execute(c)){
				return get(c, f);
			}
		}
		return false;
	}
	
	private boolean get(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, TXNProtocol.WQ, Utility.stringToByteArray(f.getName()));
			return false;
		}else{
			f.execute();
			if(this.txn.isDeleted(f))
				this.n.printError(c, Error.ERR_10);
			else
				this.txn.add( c );
			txnExecute();
			return true;
		}
	}

	public boolean create(String filename){
		boolean rtn = false;
		if( assertTXNStarted() && notCommited() ) {
			File f = getFileFromCache( filename );
			Command c = new Command(MASTER_NODE, Command.CREATE, f, "");
			
			if(f.execute(c)){
				return create(c, f);
			}
		}
		return rtn;
	}
	
	private boolean create(Command c, File f){
		if(f.getState() == File.INV && !this.txn.isDeleted(f)){
			this.send(MASTER_NODE, TXNProtocol.CREATE, Utility.stringToByteArray(f.getName()));
			return false;
		}else{
			f.execute();
			if(this.txn.isDeleted(f)){
				this.txn.add(c);
				f.setState(File.RW);
			}else
				this.n.printError(c, Error.ERR_11);
			this.txnExecute();
			return true;
		}
	}

	public boolean put(String filename, String content){
		if( assertTXNStarted() && notCommited() ) {
			File f = getFileFromCache( filename );
			Command c = new Command(MASTER_NODE, Command.PUT, f, content);
			
			if(f.execute(c)){
				return put(c, f);
			}
		}
		return false;
	}
	
	private boolean put(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, TXNProtocol.WQ, Utility.stringToByteArray(f.getName())); //WQ
			return false;
		}else{
			f.execute();
			if(this.txn.isDeleted(f)) {
				this.n.printError(c, Error.ERR_10);
			}
			else {
				this.txn.add( c );
			}
			txnExecute();
			return true;
		}
	}

	public boolean append(String filename, String content){
		if( assertTXNStarted() && notCommited() ) {
			File f = getFileFromCache( filename );
			Command c = new Command(MASTER_NODE, Command.APPEND, f, content);
			
			if(f.execute(c)) {
				return append(c, f);
			}
		}
		return false;
	}
	
	private boolean append(Command c, File f){
		if(f.getState() != File.RW) {
			this.send(MASTER_NODE, TXNProtocol.WQ, Utility.stringToByteArray(f.getName())); //WQ
			return false;
		}else{
			f.execute();
			
			if(this.txn.isDeleted(f)) {
				this.n.printError(c, Error.ERR_10);
			} else {
				this.txn.add( c );
			}
			txnExecute();
			return true;
		}
		
	}

	public boolean delete(String filename){
		if( assertTXNStarted() && notCommited() ) {
			File f = getFileFromCache( filename );
			Command c = new Command(MASTER_NODE, Command.DELETE, f);
		
			if(f.execute(c)) {
				return delete(c, f);
			}
		}
		return false;
	}
	
	private boolean delete(Command c, File f){
		if(f.getState() != File.RW) {
			this.send(MASTER_NODE, TXNProtocol.WQ, Utility.stringToByteArray(f.getName()));
			return false;//WQ
		} else {
			f.execute();
			f.setState(File.INV);
			this.txn.add(c);
			txnExecute();
			return true;
			
		}
	}

	public void abort(boolean notifyServer) {
		if( (this.txn == null || !this.txn.isStarted) && notifyServer ){
			this.assertTXNStarted();
		}else{
			this.txn = null;
			this.cache.clear();
			if(notifyServer)
				this.send(MASTER_NODE, TXNProtocol.ABORT, new byte[0]);
			else
				this.n.printError("Node " + this.n.addr + " : Transaction aborted, please start a new transaction and try again.");
		}
	}
	
	public void txnExecute() {
		if( this.txn.willCommit ) {
			this.txn.decrementNumQueued();
			if( this.txn.getNumQueued() == 0 ) {
				this.commit();
			}
		}
	}

	public void commit() {

		if( assertTXNStarted() ) {
			//Check to see if there are queued commands before committing
			if( noQueuedCommands() ) {
				//Send all of our commands to the master node
				int cnt = 0;
				for( Command c : this.txn ) {
					String payload = c.getFileName() + " " + c.getType() + " ";
					if( c.getType() == Command.PUT || c.getType() == Command.APPEND || c.getType() == Command.UPDATE ) {
						payload += c.getContents();
					}
					this.send(MASTER_NODE, TXNProtocol.COMMIT_DATA, Utility.stringToByteArray(payload));
					cnt++;
				}
				//Send the final commit message
				this.send(MASTER_NODE, TXNProtocol.COMMIT, Utility.stringToByteArray(cnt + "") );
			} else {
				//set will commit to true to that the txn commits after all queued commands complete
				this.txn.willCommit = true;
			}
		}
	}
	
	public boolean noQueuedCommands() {
		int commandCount = 0;
		for( File f : this.cache.values() ) {
			commandCount += f.numCommandsOnQueue();
		}
		if( commandCount > 0 ) {
			this.txn.setNumQueued( commandCount );
			return false;
		}
		return true;
	}
	
	public void commitConfirm() {
		this.txn = null;
		this.cache.clear();
	}

	public void start() {
		if( this.txn == null ) {
			int newTXNnum = this.lastTXNnum + RIONode.NUM_NODES;
			
			//start a new transaction by creating a new transaction object
			this.txn = new Transaction( newTXNnum );
			this.send(MASTER_NODE, TXNProtocol.START, new byte[0]);
		} else {
			this.n.printError("ERROR: Transaction in progress on node " + this.n.addr + " : can not start new transaction");
		}
	}
	
	public boolean assertTXNStarted() {
		if( this.txn == null ) {
			this.n.printError("ERROR: No transaction in progress on node " + this.n.addr + " : please start new transaction");
			return false;
		}else if(!this.txn.isStarted){
			this.n.printError("ERROR: Could not execute command. Transaction currently starting on node " + this.n.addr + " : please wait until it finishes.");
			return false;
		}
		return true;
	}
	
	public boolean notCommited() {
		if( this.txn.willCommit ) {
			this.n.printError("ERROR: Current transaction to be commited on node " + this.n.addr + " : please start new transaction");
			return false;
		}
		return true;
	}
	
	public File getFileFromCache(String fileName) {
		File f = this.cache.get(fileName);
		
		if(f == null){
			f = this.n.addr == MASTER_NODE ? new MasterFile(fileName, "") : new File(File.INV, fileName);
			if(!fileName.isEmpty())
				this.cache.put(fileName, f);
		}
		return f;
	}

	public void setupCache(HashSet<String> fileList) {
		for(String fileName : fileList){
			File f = getFileFromCache(fileName);
			f.setState(File.RW);
		}
	}

}
