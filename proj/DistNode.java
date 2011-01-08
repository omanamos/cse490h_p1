import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class DistNode extends RIONode {
	
	private String sessionId;
	
	/**
	 * 
	 */
	private HashMap<Integer, Session> activeSessions;
	

	//TODO: Add Error reporting
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		String data = Utility.byteArrayToString(msg);
		switch(protocol){
		case Protocol.APPEND: case Protocol.PUT:
			String[] parts = data.split(" ");
			try {
				this.getWriter(parts[0], protocol == Protocol.APPEND).write(parts[1]);
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		case Protocol.CREATE:
			try {
				this.getWriter(data, false);
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		case Protocol.DELETE:
			try{
				this.getWriter(data, false).delete();
			}catch(IOException e){
				e.printStackTrace();
			}
			break;
		case Protocol.GET:
			try {
				printReader(this.getReader(data));
			} catch (FileNotFoundException e) {
				System.out.println("10 File does not exist");
			}
			break;
		default:
			return;
		}
	}
	
	@Override
	public void start() {
		
	}

	@Override
	public void onCommand(String command) {
		String[] parts = command.split(" ");
		int server = Integer.parseInt(parts[1]);
		
		if(parts[0].equals("create")){
			this.create(server, parts[2]);
		}else if(parts[0].equals("get")){
			this.get(server, parts[2]);
		}else if(parts[0].equals("put")){
			this.put(server, parts[2], parts[3]);
		}else if(parts[0].equals("append")){
			this.append(server, parts[2], parts[3]);
		}else if(parts[0].equals("delete")){
			this.delete(server, parts[2]);
		}else{
			System.out.println("Invalid command.");
		}
	}
	
	//TODO: check for max packet size
	private void create(int server, String filename){
		this.RIOLayer.RIOSend(server, Protocol.CREATE, Utility.stringToByteArray(filename));
	}
	
	private void get(int server, String filename){
		this.RIOLayer.RIOSend(server, Protocol.GET, Utility.stringToByteArray(filename));
	}
	
	private void put(int server, String filename, String contents){
		this.RIOLayer.RIOSend(server, Protocol.PUT, Utility.stringToByteArray(filename + " " + contents));
	}
	
	private void append(int server, String filename, String contents){
		this.RIOLayer.RIOSend(server, Protocol.APPEND, Utility.stringToByteArray(filename + " " + contents));
	}
	
	private void delete(int server, String filename){
		this.RIOLayer.RIOSend(server, Protocol.DELETE, Utility.stringToByteArray(filename));
	}
	
	private void printReader(PersistentStorageReader r){
		try{
			String line = r.readLine();
			while(line != null){
				System.out.println(line);
				line = r.readLine();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public class Session {
		private String sessionId;
		private int nodeId;
		private int lastSeqNum;
		
		public Session( int nodeId ) {
			this.sessionId =  UUID.randomUUID().toString();
			this.nodeId = nodeId;
			this.lastSeqNum = -1;
		}
		
		public String getSessionId() {
			return this.sessionId;
		}
		
		public int getNodeId() {
			return this.nodeId;
		}
		
		public int getLastSeqNum() {
			return this.lastSeqNum;
		}
	
		public void setLastSeqNum( int num ) {
			this.lastSeqNum = num;
		}
		
		
	}
}


