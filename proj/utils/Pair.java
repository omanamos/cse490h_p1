package utils;

public class Pair<T1 extends Comparable<T1>,T2 extends Comparable<T2>> implements Comparable<Pair<T1,T2>>{
	public T1 t1;
	public T2 t2;
	
	public Pair(T1 t1, T2 t2){
		this.t1 = t1;
		this.t2 = t2;
	}

	public int compareTo(Pair<T1,T2> other) {
		int c1 = -this.t2.compareTo(other.t2);
		if(c1 == 0)
			return this.t1.compareTo(other.t1);
		return c1;
	}
}
