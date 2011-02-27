import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Utility;


public class TransactionLayer {

	public final static int MASTER_NODE = 0;
	
	public  DistNode n;
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
	 * key = addr of node trying to commit
	 * value = commit status
	 */
	private Map<Integer, Commit> waitingQueue;

	private TimeoutManager timeout;
	
	private Map<Integer, Boolean> txnLog;
	
	
	public TransactionLayer(RIONode n, ReliableInOrderMsgLayer RIOLayer){
		this.cache = new HashMap<String, File>();
		this.n = (DistNode)n;
		this.RIOLayer = RIOLayer;
		this.lastTXNnum = this.n.addr;
		this.timeout = new TimeoutManager(7, this.n, this);
		
		if(this.n.addr == MASTER_NODE){
			//TODO: connect txn layer with paxos layer this.paxos = new PaxosLayer(this);
			this.waitingQueue = new HashMap<Integer, Commit>();
			this.assumedCrashed = new HashSet<Integer>();
			this.txnLog = new HashMap<Integer, Boolean>();
		}
	}
	
	public void initializeLastTxnNumber(int txnID){
		this.lastTXNnum = txnID;
	}
	
	public void initializeLog(Map<Integer, Boolean> txnLog){
		this.txnLog.putAll(txnLog);
	}
	
	public void send(int dest, int protocol, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, this.timeout.nextSeqNum(dest), payload);
		int p = pkt.getProtocol();
		if(p == TXNProtocol.WF || p == TXNProtocol.WQ || p == TXNProtocol.CREATE || (!this.n.isMaster() && (p == TXNProtocol.ABORT || p == TXNProtocol.COMMIT || p == TXNProtocol.START)))
			this.timeout.createTimeoutListener(dest, pkt);
		this.RIOLayer.sendRIO(dest, Protocol.TXN, pkt.pack());
	}
	
	public void rtn(int dest, int protocol, int seqNum, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, seqNum, payload);
		this.RIOLayer.sendRIO(dest, Protocol.RTN, pkt.pack());
	}
	
	/**
	 * Starts or stops a heartbeat to a given node
	 * @param dest node to start or stop the heartbeat
	 * @param heartbeat true = start, false = stop
	 */
	public void setHB(int dest, boolean heartbeat){
		this.RIOLayer.setHB(dest, heartbeat);
	}
	
	public void onReceive(int from, byte[] payload) {
		TXNPacket packet = TXNPacket.unpack(payload);
		if(packet.getProtocol() == TXNProtocol.PAXOS){
			this.paxos.onReceive(from, packet.getPayload());
		}else if(this.n.addr == MASTER_NODE){
			masterReceive(from, packet);
		}else
			slaveReceive(from, packet);
	}
	
	/**
	 * Called when a message times out on the RIOLayer
	 * @param dest
	 * @param payload
	 */
	public void onRIOTimeout(int dest, byte[] payload){
		TXNPacket pkt = TXNPacket.unpack(payload);
		this.timeout.onRtn(dest, pkt.getSeqNum());
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
							this.rtn(committer, TXNProtocol.ABORT, pkt.getSeqNum(), new byte[0]);
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
					TXNPacket p = (TXNPacket)f.execute();
					Update u = f.chooseProp(p.getSource());
					payload = Utility.stringToByteArray(fileName + " " + u.version + " " + u.contents);
					this.rtn(p.getSource(), TXNProtocol.WD, p.getSeqNum(), payload);
					f.changePermissions(p.getSource(), MasterFile.FREE);
					while(f.peek() != null){ //also return queued requests for the file
						TXNPacket p1 = (TXNPacket)f.execute();
						this.rtn(p1.getSource(), TXNProtocol.WD, p1.getSeqNum(), payload);
						f.changePermissions(p1.getSource(), MasterFile.FREE);
					}
				}
			}else
				this.n.printError(DistNode.buildErrorString(dest, this.n.addr, pkt.getProtocol(), Utility.byteArrayToString(pkt.getPayload()), Error.ERR_20));
		}else{ //this is the client and we should just print out the message
			if(pkt.getProtocol() == TXNProtocol.START)
				this.txn = null;
			else if(pkt.getProtocol() == TXNProtocol.WD || pkt.getProtocol() == TXNProtocol.ERROR){
				//this.rtn(dest, pkt.getProtocol(), pkt.getSeqNum(), pkt.getPayload());
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
					this.rtn(from, TXNProtocol.ERROR, pkt.getSeqNum(), Utility.stringToByteArray(payload));
				}else if(!f.isCheckedOut()){ //The file hasn't been checked out by anyone, return the last committed version.
					try{
						contents = this.n.get(fileName);
						byte[] payload = Utility.stringToByteArray(f.getName() + " " + f.getVersion() + " " + contents);
						f.addDep(from, new Update(contents, f.getVersion(), MASTER_NODE));
						f.changePermissions(from, MasterFile.FREE);
						this.rtn(from, TXNProtocol.WD, pkt.getSeqNum(), payload);
					}catch(IOException e){
						String payload = fileName + " " + Error.ERR_10;
						this.rtn(from, TXNProtocol.ERROR, pkt.getSeqNum(), Utility.stringToByteArray(payload));
					}
				}else if(f.isWaiting()){ //The server is currently waiting for some WFs to return from clients. Enqueue this request to execute once they have returned.
					pkt.setSource(from);
					f.execute(pkt);
				}else{ //The server must send out WFs to all clients that have copies of the file, and pick the highest version that is returned.
					pkt.setSource(from);
					f.execute(pkt);
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
				
				if(this.timeout.onRtn(from, pkt.getSeqNum()) && f.isWaiting()){
					f.changePermissions(from, MasterFile.FREE);
					if(f.getVersion() < version){
						f.propose(contents, version, from);
					}
					if(!f.isWaiting()){
						TXNPacket p = (TXNPacket)f.execute();
						Update u = f.chooseProp(p.getSource());
						byte[] payload = Utility.stringToByteArray(fileName + " " + u.version + " " + u.contents);
						this.rtn(p.getSource(), TXNProtocol.WD, p.getSeqNum(), payload);
						f.changePermissions(p.getSource(), MasterFile.FREE);
						while(f.peek() != null){ //also return queued requests for the file
							TXNPacket p1 = (TXNPacket)f.execute();
							this.rtn(p1.getSource(), TXNProtocol.WD, p1.getSeqNum(), payload);
							f.changePermissions(p1.getSource(), MasterFile.FREE);
						}
					}
				}
				break;
			case TXNProtocol.ERROR:
				String[] parts = Utility.byteArrayToString(pkt.getPayload()).split(" ");
				
				if(parts.length == 2){
					fileName = parts[0];
					int errCode = Integer.parseInt(parts[1]);
					if(this.timeout.onRtn(from, pkt.getSeqNum()) && errCode == Error.ERR_10){
						//This is a client saying it doesn't have a file after the server sent it a WF
						//This means the MasterFile has some corrupted state, change permissions for that client to invalid.
						f = (MasterFile)this.getFileFromCache(fileName);
						f.changePermissions(from, File.INV);
						if(!f.isWaiting()){
							TXNPacket p = (TXNPacket)f.execute();
							Update u = f.chooseProp(p.getSource());
							byte[] payload = Utility.stringToByteArray(fileName + " " + u.version + " " + u.contents);
							this.rtn(p.getSource(), TXNProtocol.WD, p.getSeqNum(), payload);
							f.changePermissions(p.getSource(), MasterFile.FREE);
							while(f.peek() != null){ //also return queued requests for the file
								TXNPacket p1 = (TXNPacket)f.execute();
								this.rtn(p1.getSource(), TXNProtocol.WD, p1.getSeqNum(), payload);
								f.changePermissions(p1.getSource(), MasterFile.FREE);
							}
						}
					}
				}
				break;
			case TXNProtocol.COMMIT:
				this.commit(from, pkt.getSeqNum(), CommitPacket.unpack(pkt.getPayload(), this.cache));
				break;
			case TXNProtocol.CREATE:
				fileName = Utility.byteArrayToString(pkt.getPayload());
				f = (MasterFile)this.getFileFromCache(fileName);
				
				if(f.getState() == File.INV){
					f.addDep(from, new Update("", 0, MASTER_NODE));
					f.setState(File.RW);
					f.changePermissions(from, MasterFile.FREE);
					String payload = fileName + " " + f.getVersion() + " ";
					this.rtn(from, TXNProtocol.WD, pkt.getSeqNum(), Utility.stringToByteArray(payload));
				}else{
					String payload = fileName + " " + Error.ERR_11;
					this.rtn(from, TXNProtocol.ERROR, pkt.getSeqNum(), Utility.stringToByteArray(payload));
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
							int txID = com.getLog().getTXN().id;
							this.updateLog(txID, false);
							this.rtn(committer, TXNProtocol.ABORT, com.getSeqNum(), Utility.stringToByteArray(txID + ""));
							break;
						}
					}
				}
				this.rtn(from, TXNProtocol.START, pkt.getSeqNum(), new byte[0]);
				break;
			case TXNProtocol.ABORT:
				int txnID = Integer.parseInt(Utility.byteArrayToString(pkt.getPayload()));
				this.updateLog(txnID, false);
				for(String fName : this.cache.keySet()){
					f = (MasterFile)this.cache.get(fName);
					f.abort(from);
				}
				for(Integer committer : waitingQueue.keySet()){
					Commit com = waitingQueue.get(committer);
					for(Integer dep : com){
						if(dep == from){
							this.commit(committer, pkt.getSeqNum(), com.getLog());
							break;
						}
					}
				}
				this.rtn(from, TXNProtocol.ABORT, pkt.getSeqNum(), Utility.stringToByteArray(txnID+""));
				break;
		}
	}
	
	private void commit(int client, int seqNum, CommitPacket pkt){
		Transaction txn = pkt.getTransaction();
		//TODO: store most recently committed txn for each client on disk on server and clients
		
		if(this.assumedCrashed.contains(client)){
			this.assumedCrashed.remove(client);
			for(String fName : this.cache.keySet()){
				MasterFile f = (MasterFile)this.cache.get(fName);
				f.commit(client);
			}
			this.send(client, TXNProtocol.ABORT, Utility.stringToByteArray(txn.id+""));
		}else if( txn.isEmpty() ) {
			this.send(client, TXNProtocol.COMMIT, Utility.stringToByteArray(txn.id+""));
		}else {
			Log log = new Log(client, txn);
			this.commit(client, seqNum, log);
		}
	}
	
	private void commit(int client, int seqNum,  Log log){
		Commit c = new Commit(client, log, this.assumedCrashed, seqNum);
		
		if(c.abort()){
			for(MasterFile f : log)
				f.abort(client);
			this.rtn(client, TXNProtocol.ABORT, seqNum, Utility.stringToByteArray(log.getTXN().id+""));
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
				this.n.write(".wh_log", log.getTXN().id + "\n" + Update.toString(updates), true, true);
				this.pushUpdatesToDisk(log.getTXN().id, updates);
			}catch(IOException e){
				e.printStackTrace();
				return;
			}
			this.rtn(client, TXNProtocol.COMMIT, seqNum, Utility.stringToByteArray(log.getTXN().id+""));
			
			this.setHB(client, false);
			//Allow any transactions dependent on this one to commit
			for(Integer committer : waitingQueue.keySet()){
				Commit com = waitingQueue.get(committer);
				for(Integer dep : com){
					if(dep == client){
						com.remove(client);
						if(!com.isWaiting()){
							this.waitingQueue.remove(committer);
							this.commit(committer, com.getSeqNum(), com.getLog());
						}
						break;
					}
				}
			}
		}
	}
	
	public void pushUpdatesToDisk(int txID, Map<MasterFile, Update> updates) throws IOException{
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
		this.updateLog(txID, true);
		this.n.delete(".wh_log");
	}
	
	private void updateLog(int txID, boolean committed){
		this.txnLog.put(txID, committed);
		String contents = "";
		for(Integer id : this.txnLog.keySet()){
			contents += id + " " + (this.txnLog.get(id) ? 1 : 0);
		}
		try {
			this.n.write(".txn_log", contents, false, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void slaveReceive(int from, TXNPacket pkt){
		String fileName;
		File f;
		String contents;
		int i;
		Command c;
		
		switch(pkt.getProtocol()){
			case TXNProtocol.WF:
				fileName = Utility.byteArrayToString(pkt.getPayload());
				f = this.getFileFromCache(fileName);
				
				if(f.getState() == File.INV){
					byte[] payload = Utility.stringToByteArray(fileName + " " + -1 + " ");
					this.rtn(MASTER_NODE, TXNProtocol.WD, pkt.getSeqNum(), payload);
				}else{
					try {
						byte[] payload = this.txn.getVersion(f, this.n.get(fileName));
						this.rtn(MASTER_NODE, TXNProtocol.WD, pkt.getSeqNum(), payload);
					} catch (IOException e) {
						this.rtn(MASTER_NODE, TXNProtocol.ERROR, pkt.getSeqNum(), Utility.stringToByteArray(fileName + " " + Error.ERR_10));
					}
				}
				break;
			case TXNProtocol.WD:
				if(this.timeout.onRtn(from, pkt.getSeqNum())){
					contents = Utility.byteArrayToString(pkt.getPayload());
					i = contents.indexOf(' ');
					fileName = contents.substring(0, i);
					int lastSpace = i + 1;
					i = contents.indexOf(' ', lastSpace);
					int version = Integer.parseInt(contents.substring(lastSpace, i));
					contents = i == contents.length() - 1 ? "" : contents.substring(i + 1);
					
					f = this.getFileFromCache(fileName);
					c = (Command)f.execute(); //Get command that originally requested this Query
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
				}
				break;
			case TXNProtocol.ABORT:
				int txID = Integer.parseInt(Utility.byteArrayToString(pkt.getPayload()));
				this.timeout.onRtn(from, pkt.getSeqNum());
				if(txID == this.txn.id){
					this.abort(false);
				}
				break;
			case TXNProtocol.COMMIT:
				txID = Integer.parseInt(Utility.byteArrayToString(pkt.getPayload()));
				this.timeout.onRtn(from, pkt.getSeqNum());
				if(txID == this.txn.id){
					this.commitChangesLocally();
					this.commitConfirm();
				}
				break;
			case TXNProtocol.ERROR:
				contents = Utility.byteArrayToString(pkt.getPayload());
				i = contents.indexOf(' ');
				fileName = contents.substring(0, i);
				contents = contents.substring(i + 1);
				
				if(this.timeout.onRtn(from, pkt.getSeqNum())){
					f = this.getFileFromCache(fileName);
					c = (Command)f.execute();
					try{
						int code = Integer.parseInt(contents.trim());
						this.n.printError(c, code);
					}catch(Exception e){
						this.n.printError(contents);
					}
					executeCommandQueue(f);
				}
				break;
			case TXNProtocol.START:
				if(this.timeout.onRtn(from, pkt.getSeqNum())){
					this.txn.isStarted = true;
					this.n.printData("Success: Transaction #" + this.txn.id + " Started on Node " + this.n.addr);
				}
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
		
		this.n.printData("Success: Transaction #" + this.txn.id + " successfully committed on node " + this.n.addr);
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
				//Send txn to master node
				this.send(MASTER_NODE, TXNProtocol.COMMIT, new CommitPacket(this.txn).pack());
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
			try{
				int newTXNnum = this.lastTXNnum + RIONode.NUM_NODES;
				this.n.write(".txn_id", newTXNnum + "", false, true);
				
				//start a new transaction by creating a new transaction object
				this.txn = new Transaction( newTXNnum );
				this.send(MASTER_NODE, TXNProtocol.START, new byte[0]);
			}catch(Exception e){
				e.printStackTrace();
			}
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
