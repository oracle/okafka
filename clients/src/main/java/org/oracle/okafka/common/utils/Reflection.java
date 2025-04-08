package org.oracle.okafka.common.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Reflection {
	public static <T> T createInstance(Class<T> clazz, Class<?>[] paramTypes, Object... args) {
		T instance = null;
		try {
			Constructor<T> constructor = clazz.getDeclaredConstructor(paramTypes);
			constructor.setAccessible(true);
			instance = constructor.newInstance(args);
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
		    throw new RuntimeException("Failed to create instance via reflection", e);
		}
		return instance;
	}
}
