package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class ReadPosts extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get [username]_wall",
												"txcommit"};
	
	private String posts;
	public ReadPosts(User u, DistNode n){
		super(COMMANDS, n, u);
	}
	
	@Override
	public void onCommandFinish(Command c) {
		
		switch(this.cmds.size()) {
			case 2:
				if( FacebookOperation.isUserLoggedIn(this.user, this.n.addr, c.getContents()) ) {
					//execute next command
					String wallCommand = FacebookOperation.replaceField(this.cmds.poll(), "username", this.user.getUsername());
					this.n.onFacebookCommand( wallCommand );
				} else {
					//abort
					this.notLoggedIn();
				}
				break;
			case 1:
				this.posts = c.getContents();
				this.n.onFacebookCommand( this.cmds.poll() );
				break;
		}
	}

	@Override
	public void onAbort(Transaction txn) {
		System.out.println("Node " + this.n.addr + ": Error: Cannot execute command: Please try again");
	}

	@Override
	public void onCommit(Transaction txn) {
		System.out.println("Node " + this.n.addr + ":\n" + this.posts );
		
	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.cmds.poll() );
	}

}
