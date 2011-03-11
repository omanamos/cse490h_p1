package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class ReadPosts extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get [username]_wall",
												"txcommit"};
	public ReadPosts(User u, DistNode n){
		
		super(COMMANDS, n, u);
		this.n.onFacebookCommand( this.cmds.poll() );
	}
	
	@Override
	public void onCommandFinish(Command c) {
		
		switch(this.cmds.size()) {
			case 2:
				if( FacebookOperation.isUserLoggedIn(this.user, c.getContents()) ) {
					//execute next command
					String wallCommand = FacebookOperation.replaceField(this.cmds.poll(), "username", this.user.getUsername());
					this.n.onFacebookCommand( wallCommand );
				} else {
					//abort
					this.n.notLoggedIn( this.user );
				}
				break;
			case 1:
				this.n.onFacebookCommand( this.cmds.poll() );
				break;
		}
	}

	@Override
	public void onAbort(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommit(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.cmds.poll() );
	}

}
