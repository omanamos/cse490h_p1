import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class DistNode extends RIONode {
	
	//TODO: Add Error reporting
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		String data = Utility.byteArrayToString(msg);
		
		if(protocol != Protocol.ACK && protocol != Protocol.ACK_SESSION){
			switch(protocol){
				case Protocol.APPEND: case Protocol.PUT:
					int indexOfSpace = data.indexOf(' ');
					String fileName = data.substring(0, indexOfSpace);
					String content = data.substring(indexOfSpace + 1);
					if(content.charAt(0) == '"')
						content = content.substring(1, content.length() - 1);
					
					try {
						this.getWriter(fileName, protocol == Protocol.APPEND).write(content);
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
			
		}else{
			switch( protocol ) {
			case Protocol.ACK: 
				break;
			case Protocol.ACK_SESSION:
				break;
			case Protocol.EXPIRED_SESSION:
				break;
			}
				
		}
	}
	
	@Override
	public void start() {
	}

	@Override
	public void onCommand(String command) {
		command = command.trim();
		if(!Pattern.matches("^((create|get|delete) [^ ]+ [^ ]+|(put|append) [^ ]+ [^ ]+ (\\\".+\\\"|[^ ]+))$", command)){
			System.out.println("Invalid command.");
			return;
		}
		
		int indexOfSpace = command.indexOf(' ');
		int lastSpace = indexOfSpace + 1;
		String code = command.substring(0, indexOfSpace);
		
		
		indexOfSpace = command.indexOf(' ', lastSpace);
		int server = Integer.parseInt(command.substring(lastSpace, indexOfSpace));
		lastSpace = indexOfSpace + 1;
		
		indexOfSpace = command.indexOf(' ', lastSpace);
		if(indexOfSpace < 0)
			indexOfSpace = command.length();
		String fileName = command.substring(lastSpace, indexOfSpace);
		lastSpace = indexOfSpace + 1;
		
		if(code.equals("create")){
			this.create(server, fileName);
		}else if(code.equals("get")){
			this.get(server, fileName);
		}else if(code.equals("put")){
			String content = command.substring(lastSpace);
			this.put(server, fileName, content);
		}else if(code.equals("append")){
			String content = command.substring(lastSpace);
			this.append(server, fileName, content);
		}else if(code.equals("delete")){
			this.delete(server, fileName);
		}
	}
	
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
	

}


