package transactions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


import edu.washington.cs.cse490h.lib.Utility;


public class Transaction implements Iterable<Command> {
	
	public final int id;
	private List<Command> log;
	private Map<File, List<Command>> fileLog;
	private int numQueued;
	public boolean willCommit;
	public boolean willAbort;
	
	public Transaction(int id){
		this.id = id;
		this.log = new ArrayList<Command>();
		this.fileLog = new HashMap<File, List<Command>>();
		this.numQueued = 0;
		this.willCommit = false;
		this.willAbort = false;
	}
	
	public void add(Command c){
		this.log.add(c);
		if(!this.fileLog.containsKey(c.getFile()))
			this.fileLog.put(c.getFile(), new ArrayList<Command>());
		this.fileLog.get(c.getFile()).add(c);
	}
	
	public List<Command> getCommands(){
		return this.log;
	}
	
	
	public List<Command> getCommands(File f){
		return this.fileLog.get(f);
	}
	
	public void setNumQueued( int num ) {
		this.numQueued = num;
	}
	
	public int getNumQueued() {
		return this.numQueued;
	}
	
	public void decrementNumQueued() {
		if( this.willCommit || this.willAbort) {
			this.numQueued = this.numQueued == 0 ? 0 : this.numQueued - 1;
		}
	}
	
	public boolean isDeleted(File f){
		List<Command> cmds = this.fileLog.get(f);
		return cmds != null && cmds.size() != 0 && cmds.get(cmds.size() - 1).getType() == Command.DELETE;
	}
	
	public Set<File> getFiles(){
		return this.fileLog.keySet();
	}
	
	public Iterator<Command> iterator(){
		return this.log.iterator();
	}
	
	public int getVersion(File f){
		int version = f.getVersion();
		for(Command c : this.fileLog.get(f)){
			if(c.getType() == Command.APPEND || c.getType() == Command.PUT)
				version++;
		}
		return version;
	}
	
	public byte[] getVersion(File f, String contents){
		int version = f.getVersion();
		for(Command c : this.getCommands(f)){
			if(c.getType() == Command.CREATE || c.getType() == Command.DELETE){
				contents = "";
				version++;
			}else if(c.getType() == Command.APPEND){
				version++;
				contents += c.getContents();
			}else if(c.getType() == Command.PUT){
				version++;
				contents = c.getContents();
			}
		}
		return Utility.stringToByteArray(f.getName() + " " + version + " " + this.id + " " + contents);
	}
	
	public String getVersionContents(File f, String contents){
		for(Command c : this.getCommands(f)){
			if(c.getType() == Command.CREATE || c.getType() == Command.DELETE){
				contents = "";
			}else if(c.getType() == Command.APPEND){
				contents += c.getContents();
			}else if(c.getType() == Command.PUT){
				contents = c.getContents();
			}
		}
		return contents;
	}
	
	public byte[] buildCommit(){
		String payload = "";
		
		for(Command c : this){
			payload += c.buildCommit() + "#";
		}
		
		return Utility.stringToByteArray(payload.isEmpty() ? "" : payload.substring(0, payload.length() - 1));
	}
	
	public static Transaction fromByteArray(int txnNum, Map<String, File> cache, byte[] arr){
		Transaction txn = new Transaction(txnNum);
		String payload = Utility.byteArrayToString(arr);
		if(payload.isEmpty())
			return txn;
		for(String command : payload.split("#"))
			txn.add(Command.fromByteArray(command, cache));
		
		return txn;
	}
	
	/**
	 * Used for paxos string representation
	 * @param str string built by Transaction.toString()
	 * @param cache
	 * @return Transaction object built from the given string
	 */
	public static Transaction fromString(String str, Map<String, File> cache){
		String[] parts = str.split("#");
		int id = Integer.parseInt(parts[0]);
		Transaction txn = new Transaction(id);
		
		if(parts.length == 1){
			//empty transaction, do nothing
		}else if(parts[1].equals("ABORT")){
			txn.willAbort = true;
		}else{
			for(int i = 1; i < parts.length; i++)
				if(!parts[i].isEmpty())
					txn.add(Command.fromByteArray(parts[i], cache));
		}
		return txn;
	}
	
	public static int getIdFromString(String str){
		return Integer.parseInt(str.split("#")[0]);
	}
	
	/**
	 * @return String representation of a Transaction, each command is separated by a hash sign
	 */
	public String toString(){
		String rtn = this.id + "#";
		
		if(this.willAbort){
			return rtn + "ABORT";
		}else{
			for(Command c : this){
				rtn += c.buildCommit() + "#";
			}
			
			return rtn.substring(0, rtn.length() - 1);
		}
	}
	
	public int size(){
		return this.log.size();
	}
	
	public boolean isEmpty(){
		return this.size() == 0;
	}
	
	public boolean hasFile(File f){
		return this.fileLog.containsKey(f);
	}
}
