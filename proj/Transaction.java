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
	
	public Transaction(int id){
		this.id = id;
		this.log = new ArrayList<Command>();
		this.fileLog = new HashMap<File, List<Command>>();
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
	
	public Set<File> getFiles(){
		return this.fileLog.keySet();
	}
	
	public Iterator<Command> iterator(){
		return this.log.iterator();
	}
	
	public byte[] buildCommit(){
		byte[] rtn = new byte[0];
		//TODO: build byte array of this.log
		return rtn;
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
			if(c.getType() == Command.APPEND){
				version++;
				contents += c.getContents();
			}else if(c.getType() == Command.PUT){
				version++;
				contents = c.getContents();
			}
		}
		return Utility.stringToByteArray(f.getName() + " " + version + " " + contents);
	}
	
}
