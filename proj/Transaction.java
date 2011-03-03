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
	public boolean isStarted;
	public boolean willStart;
	public boolean willAbort;
	
	public Transaction(int id){
		this.id = id;
		this.log = new ArrayList<Command>();
		this.fileLog = new HashMap<File, List<Command>>();
		this.numQueued = 0;
		this.willCommit = false;
		this.willAbort = false;
		this.isStarted = false;
		this.willStart = true;
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
		if( this.willCommit ) {
			this.numQueued--;
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
	
	public byte[] buildCommit(){
		String payload = "";
		
		for(Command c : this){
			payload += c.buildCommit() + "#";
		}
		
		return Utility.stringToByteArray(payload.substring(0, payload.length() - 1));
	}
	
	public static Transaction fromByteArray(int txnNum, Map<String, File> cache, byte[] arr){
		Transaction txn = new Transaction(txnNum);
		String payload = Utility.byteArrayToString(arr);
		
		for(String command : payload.split("#")){
			txn.add(Command.fromByteArray(command, cache));
		}
		
		return txn;
	}
	
	public int size(){
		return this.log.size();
	}
	
	public boolean isEmpty(){
		return this.size() == 0;
	}
	
}
