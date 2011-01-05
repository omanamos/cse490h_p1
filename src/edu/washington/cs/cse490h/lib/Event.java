package edu.washington.cs.cse490h.lib;

import edu.washington.cs.cse490h.lib.Manager.Timeout;

/**
 * Class that represents the various types of events within the managers
 */
public class Event {
	protected int node;
	protected final EventType t;

	public static enum EventType {
		FAILURE, START, EXIT, COMMAND, ECHO, TIME, DELIVERY, TIMEOUT
	}

	protected String command;
	protected String[] msg;

	protected Packet p;
	protected Timeout to;

	private Event(EventType t) {
		this.t = t;
	}

	protected static Event getFailure(int node) {
		Event e = new Event(EventType.FAILURE);
		e.node = node;
		return e;
	}

	protected static Event getStart(int node) {
		Event e = new Event(EventType.START);
		e.node = node;
		return e;
	}

	protected static Event getExit() {
		Event e = new Event(EventType.EXIT);
		return e;
	}

	protected static Event getCommand(int node, String command) {
		Event e = new Event(EventType.COMMAND);
		e.node = node;
		e.command = command;
		return e;
	}

	protected static Event getEcho(String[] msg) {
		Event e = new Event(EventType.ECHO);
		e.msg = msg;
		return e;
	}

	protected static Event getTime() {
		Event e = new Event(EventType.TIME);
		return e;
	}

	protected static Event getDelivery(Packet p) {
		Event e = new Event(EventType.DELIVERY);
		e.node = p.getDest();
		e.p = p;
		return e;
	}

	protected static Event getTimeout(Timeout to) {
		Event e = new Event(EventType.TIMEOUT);
		e.node = to.node.addr;
		e.to = to;
		return e;
	}

	public String toString() {
		switch(t) {
		case FAILURE:
			return "FAILURE " + node;
		case START:
			return "START " + node;
		case EXIT:
			return "EXIT";
		case COMMAND:
			return "COMMAND " + node + " executes " + command;
		case ECHO:
			return "ECHO " + msg;
		case TIME:
			return "TIME " + msg;
		case DELIVERY:
			return "DELIVERY " + p;
		case TIMEOUT:
			return "TIMEOUT " + to;
		default:
			return "UNKNOWN EVENT TYPE " + t;
		}
	}
}