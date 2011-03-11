package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class PostMessage extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get users", //check that requester exists -> if not, remove from [curUser]_requests file
												"get [username]_friends",
												"append [friend]_wall \"[message]\n\"", //execute for each line in [username]_friends file
												"txcommit"};
	public PostMessage(User u, String message, DistNode n){
		super(COMMANDS, n, u);
	}
	
	@Override
	public void onCommandFinish(Command c) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		
	}

}
