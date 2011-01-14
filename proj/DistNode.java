import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class DistNode extends RIONode {
	
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		String data = Utility.byteArrayToString(msg);
		
		switch(protocol){
			case Protocol.APPEND: case Protocol.PUT:
				int indexOfSpace = data.indexOf(' ');
				String fileName = data.substring(0, indexOfSpace);
				String content = data.substring(indexOfSpace + 1);
				if(content.charAt(0) == '"')
					content = content.substring(1, content.length() - 1);
				
				try {
					content = content.replaceAll("\\\\n", "\n");
					if(fileExists(fileName))
						if(protocol == Protocol.PUT)
							putFile(fileName, content);
						else{
							this.getWriter(fileName, true).write(content);
						}
					else
						printError(this.addr, from, protocol, fileName, Error.ERR_10);
				} catch (IOException e) {
					printError(this.addr, from, protocol, fileName, Error.ERR_10);
				}
				break;
			case Protocol.CREATE:
				try {
					if(!fileExists(data))
						this.getWriter(data, false);
					else
						printError(this.addr, from, protocol, data, Error.ERR_11);
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case Protocol.DELETE:
				try{
					if(fileExists(data))
						this.getWriter(data, true).delete();
					else
						throw new IOException();
				}catch(IOException e){
					printError(this.addr, from, protocol, data, Error.ERR_10);
				}
				break;
			case Protocol.GET:
				try {
					printReader(this.getReader(data));
				} catch (FileNotFoundException e) {
					printError(this.addr, from, protocol, data, Error.ERR_10);
				}
				break;
			default:
				return;
		}
	}
	
	private void putFile(String fileName, String content){
		try{
			PersistentStorageReader oldFile = this.getReader(fileName);
			this.getWriter(".temp", false).delete();
			PersistentStorageWriter temp = getWriter(".temp", true);
			
			temp.write("foo.txt");
			temp.newLine();
			
			copyFile(oldFile, temp);
			
			PersistentStorageWriter newFile = getWriter(fileName, false);
			newFile.write(content);
			temp.delete();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	private void writeFile(PersistentStorageWriter w, String contents) throws IOException{
		String[] lines = contents.split("\\n");
		for(String line : lines){
			w.write(line);
			w.newLine();
		}
	}
	
	private boolean fileExists(String fileName){
		try {
			this.getReader(fileName);
			return true;
		} catch (FileNotFoundException e) {
			return false;
		}
	}
	
	private void copyFile(PersistentStorageReader r, PersistentStorageWriter w) throws IOException{
		String file = "";
		String line = r.readLine(); 
		while(line != null){
			file += line + "\n";
		}
		w.write(file);
	}
	
	static void printError(int addr, int from, int protocol, String fileName, int code){
		System.out.println("Node " + from + ": Error: " + Protocol.protocolToString(protocol) + " on server " + 
							addr + " and file " + fileName + " returned error code " + Error.ERROR_STRINGS[code]);
	}
	
	@Override
	public void start() {
		if(fileExists(".temp")){
			try{
				PersistentStorageReader tempR = this.getReader(".temp");
				PersistentStorageWriter tempW = getWriter(".temp", false);
				if (tempR.ready())
					tempW.delete();
				else{
					String fileName = tempR.readLine();
					copyFile(tempR, this.getWriter(fileName, false));
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
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
		this.RIOLayer.sendRIO(server, Protocol.CREATE, Utility.stringToByteArray(filename));
	}
	
	private void get(int server, String filename){
		this.RIOLayer.sendRIO(server, Protocol.GET, Utility.stringToByteArray(filename));
	}
	
	private void put(int server, String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename + " " + contents);
		if(payload.length > RIOPacket.MAX_PAYLOAD_SIZE)
			printError(this.addr, server, Protocol.PUT, filename, Error.ERR_30);
		this.RIOLayer.sendRIO(server, Protocol.PUT, payload);
	}
	
	private void append(int server, String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename + " " + contents);
		if(payload.length > RIOPacket.MAX_PAYLOAD_SIZE)
			printError(this.addr, server, Protocol.APPEND, filename, Error.ERR_30);
		this.RIOLayer.sendRIO(server, Protocol.APPEND, payload);
	}
	
	private void delete(int server, String filename){
		this.RIOLayer.sendRIO(server, Protocol.DELETE, Utility.stringToByteArray(filename));
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


