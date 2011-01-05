package edu.washington.cs.cse490h.lib;

import java.util.ArrayList;

public class VectorTime {
	private ArrayList<Integer> vector = null;
	public int vecLength = 0;

	/**
	 * Builds a blank VectorTime
	 * @param maxNodes the max vector length
	 */
	public VectorTime(int maxNodes) {
		this.vector = new ArrayList<Integer>(maxNodes);
		for (int i = 0; i < maxNodes; i++) {
			this.vector.add(0);
		}
		vecLength = maxNodes;
	}
	
	/**
	 * @return length of the vector time
	 */
	public int length() {
		return vecLength;
	}
	
	
	/**
	 * Returns the clock value at an index
	 */
	public int get(int index) {
		return this.vector.get(index);
	}

	/**
	 * Returns true if (this < t), otherwise returns false
	 * @param t the other vtime
	 * @return
	 */
	public boolean lessThan(VectorTime t) {
		assert(t.vecLength == this.vecLength);
		boolean foundStrictlyLess = false;
		for (int i = 0; i < vecLength; ++i) {
			if (vector.get(i) < t.get(i))
				foundStrictlyLess = true;
			else if (vector.get(i) > t.get(i))
				return false;
		}
		return foundStrictlyLess;
	}

	/**
	 * @return Whether or not this is a unit vector
	 */
	public boolean isOneTime() {
		boolean sawOne = false;
		for (int i = 0; i < vecLength; ++i) {
			if (sawOne && vector.get(i) == 1)
				return false;
			if (vector.get(i) == 1)
				sawOne = true;
			if (vector.get(i) > 0)
				return false;
		}
		return true;
	}

	/**
	 * @return Whether or not the vector is of length 1
	 */
	public boolean isSingular() {
		return vecLength == 1;
	}
	
	/**
	 * Increments vtime at an index
	 * @param index
	 */
	public void step(int index) {
		vector.set(index, vector.get(index) + 1);
	}
	
	/**
	 * Updates to be at least as large as another vtime. Used during message passing
	 * and other communication between nodes. Usually you would need to call this.step()
	 * after this method.
	 * @param t the other vtime
	 */
	public void updateTo(VectorTime t) {
		assert(t.vecLength == this.vecLength);
		for (int i = 0; i < vecLength; ++i) {
			if (vector.get(i) < t.get(i))
				vector.set(i, t.get(i));
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((vector == null) ? 0 : vector.hashCode());
		for (int i = 0; i < vecLength; ++i) {
			result += prime*result + vector.get(i).hashCode();
		}
		return result;
	}
	
	/**
	 * Returns a Synoptic-string representation for this vector, which
	 * looks like "1,2,3"
	 */
	public String toString() {
		String ret = "";
		for (int i = 0; i < vecLength; i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += vector.get(i).toString();
		}
		return ret;
	}
}
