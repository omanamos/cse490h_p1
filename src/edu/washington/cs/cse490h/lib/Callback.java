package edu.washington.cs.cse490h.lib;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * <pre>   
 * Generic class to facilitate use of callbacks
 * Also provides a static helper function to get an object of Method that can be passed to the constructor of this class
 * Example:
 *         To get a Method object for the method public foo(String str) in class Test:
 *         String[] paramTypes = {"java.lang.String"};
 *         Method method = Callback.getMethod("foo", this, paramTypes);
 *         Callback cb = new Callback(method, this, "fooTest");
 *
 * The above code snippet assumes that it is written inside class Test, hence the use of this.
 * The method must have public visibility.
 * </pre>   
 */
public class Callback {
	private Method method;
	private Object obj;
	private Object[] params;

	/**
	 * Initializes member variables
	 * 
	 * @param method
	 *            The method to be invoked
	 * @param obj
	 *            The object on which the method is to be invoked
	 * @param params
	 *            An array of objects to be passed to the method as parameters
	 *            when it is invoked. Can be null if no parameters are to be
	 *            passed
	 */
	public Callback(Method method, Object obj, Object[] params) {
		this.method = method;
		this.obj = obj;
		this.params = params;
	}

	/**
	 * Sets the params to be passed to the method when it is invoked
	 * 
	 * @param params
	 *            The params to be passed to the method when it is invoked
	 */
	public void setParams(Object[] params) {
		this.params = params;
	}

	/**
	 * Invokes the callback
	 * 
	 * @throws IllegalAccessException
	 *             Thrown by invoke method in class Method
	 * @throws InvocationTargetException
	 *             Thrown by invoke method in class Method, if the underlying
	 *             method throws an exception
	 */
	public void invoke() throws IllegalAccessException, InvocationTargetException {
		method.invoke(obj, params);
	}

	/**
	 * Helper function to get a Method object which is needed to pass to the
	 * constructor of this class
	 * 
	 * @param methodName
	 *            The name of the method that we want a Method object for
	 * @param obj
	 *            The object that owns the method
	 * @return A Method object which can be passed to the constructor of this
	 *         class
	 * @param parameterTypes
	 *            Array of strings of parameter type names. Can be null if there
	 *            are no parameter types
	 * @throws ClassNotFoundException
	 *             This exception is thrown by Class.forName if the given class
	 *             name does not exist
	 * @throws NoSuchMethodException
	 *             Thrown by Class.getMethod if a matching method is not found
	 * @throws SecurityException
	 *             Thrown by Class.getMethod if access to the information is
	 *             denied
	 */
	public static Method getMethod(String methodName, Object obj, String[] parameterTypes) throws ClassNotFoundException,
			NoSuchMethodException, SecurityException {
		return obj.getClass().getMethod(methodName,	Callback.getParameterTypes(parameterTypes));
	}

	/**
	 * Helper to get the parameter types from an array of strings.
	 * 
	 * @param parameterTypes
	 *            An array of strings of the class names of the parameters
	 * @return An array of Classes
	 * @throws ClassNotFoundException
	 *             If any of the classes cannot be found
	 */
	private static Class<?>[] getParameterTypes(String[] parameterTypes) throws ClassNotFoundException {
		if ((parameterTypes == null) || (parameterTypes.length == 0)) {
			return null;
		}

		Class<?>[] paramTypes = new Class<?>[parameterTypes.length];
		for(int i = 0; i < paramTypes.length; i++) {
			paramTypes[i] = Class.forName(parameterTypes[i]);
		}
		return paramTypes;
	}
	
	public String toString() {
		return method.getName() + "(" + params + ")";
	}
	
	public String toSynopticString() {
		return method.getName();
	}
}
