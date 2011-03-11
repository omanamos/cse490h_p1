package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class CreateUser extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get users", //check if user exists
												"append users \"[username] [password]\n\"",
												"create [username]_requests",
												"create [username]_friends",
												"create [username]_wall",
												"txcommit"};
	public CreateUser(User u, DistNode n){
		super(COMMANDS, n, u);
		this.n.onFacebookCommand( this.nextCommand() );
	}
	
	@Override
	public void onCommandFinish(Command c) {
		String newCommand;
		
		switch(this.cmds.size()) {
		case 5:
			if( !FacebookOperation.doesUserExist(this.user, c.getContents())) {
				String replaceUsername = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
				newCommand = FacebookOperation.replaceField(replaceUsername, "password", this.user.getPassword());
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("User already exists");
			}
			break;
		case 4:
			newCommand = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
			this.n.onFacebookCommand(newCommand);
			break;
		case 3:
			newCommand = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
			this.n.onFacebookCommand(newCommand);
			break;
		case 2:
			newCommand = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
			this.n.onFacebookCommand(newCommand);
			break;
		case 1:
			this.n.onFacebookCommand(this.nextCommand());
			break;
		
		}
	}

	@Override
	public void onAbort(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommit(Transaction txn) {
		System.out.println("User " + this.user.getUsername() + " created");
	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.nextCommand() );
	}

}
