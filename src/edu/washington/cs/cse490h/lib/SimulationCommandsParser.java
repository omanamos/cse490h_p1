package edu.washington.cs.cse490h.lib;

/**
 * Parser for the Simulator commands.
 */
public class SimulationCommandsParser extends CommandsParser {
	protected Event parseNodeCmd(String[] cmd) {
		if(cmd.length < 2) {
			System.err.println("Command is too short: " + cmd);
			return null;
		}
		
		int nodeAddr = Integer.parseInt(cmd[0]);
		// the length of cmd is at least 2
		StringBuffer msg = new StringBuffer(cmd[1]);
		for(int i = 2; i < cmd.length; i++) {
			msg.append(" " + cmd[i]);
		}

		return Event.getCommand(nodeAddr, msg.toString());
	}
}
