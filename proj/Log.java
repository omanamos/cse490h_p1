import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Log implements Iterable<MasterFile>{
	private List<MasterFile> files;
	private Map<MasterFile, Update> initialVersions;
	private Map<MasterFile, List<Command>> lookup;
	private Map<MasterFile, Boolean> reads;
	private Map<MasterFile, Boolean> writes;
	private Transaction txn;
	
	public Log(int client, Transaction txn){
		this.txn = txn;
		files = new ArrayList<MasterFile>();
		reads = new HashMap<MasterFile, Boolean>();
		writes = new HashMap<MasterFile, Boolean>();
		lookup = new HashMap<MasterFile, List<Command>>();
		initialVersions = new HashMap<MasterFile, Update>();
		
		for(Command c : txn){
			MasterFile f = (MasterFile)c.getFile();
			if(!lookup.containsKey(f))
				lookup.put(f, new ArrayList<Command>());
			if(c.getType() != Command.UPDATE)
				lookup.get(f).add(c);
			
			switch(c.getType()){
				case Command.CREATE://TODO: handle creates and deletes
				case Command.DELETE:
				case Command.GET:
					reads.put(f, true);
					break;
				case Command.APPEND:
					reads.put(f, true);
				case Command.PUT:
					writes.put(f, true);
					break;
				case Command.UPDATE:
					initialVersions.put(f, Update.fromPayload(c.getContents()));
					files.add(f);
					reads.put(f, false);
					writes.put(f, false);
					break;
			}
		}
	}
	
	
	public Transaction getTXN(){
		return this.txn;
	}
	
	public List<Command> getCommands(MasterFile f){
		return this.lookup.get(f);
	}
	
	public boolean hasReads(MasterFile f){
		return this.reads.get(f);
	}
	
	public boolean hasWrites(MasterFile f){
		return this.writes.get(f);
	}
	
	public Iterator<MasterFile> iterator(){
		return this.files.iterator();
	}
	
	public Update getInitialVersion(MasterFile f){
		return this.initialVersions.get(f);
	}
	
	public String buildWHLog(){
		String rtn = "";
		for(Command c : this.txn){
			rtn += c.toString() + "\n";
		}
		return rtn;
	}
}
