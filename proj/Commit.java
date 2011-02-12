import java.util.ArrayList;
import java.util.List;


public class Commit {
	
	public Commit(List<Command> log){
		List<MasterFile> files = new ArrayList<MasterFile>();
		for(Command c : log){
			if(c.getType() == Command.UPDATE)
				files.add((MasterFile)c.getFile());
		}
		
	}
}
