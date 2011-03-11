package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class AcceptFriend extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get users", //check that requester exists -> if not, remove from [curUser]_requests file
												"get [curUser]_requests", //check that requester actually requested to be friends
												"append [curUser]_friends \"[requester]\n\"",
												"append [requester]_friends \"[curUser]\n\"",
												"put [curUser]_requests \"[contents]\"",
												"txcommit"};
	public AcceptFriend(User curUser, User requester, DistNode n){
		super(COMMANDS, n, curUser);
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
