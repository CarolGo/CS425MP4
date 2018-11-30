package cs425.crane;

import java.lang.reflect.Constructor;

public class JavaClassLoader extends ClassLoader {
    /**
     * Create a instance of a given class name
     * @param classBinName  class name to generate an instance
     * @return An instance of a given class name
     */
    public Object createInstance(String classBinName) {
        try {
            // Create a new JavaClassLoader
            ClassLoader classLoader = this.getClass().getClassLoader();

            // Load the target class using its binary name
            Class loadedMyClass = classLoader.loadClass(classBinName);

            System.out.println("Loaded class name: " + loadedMyClass.getName());

            // Create a new instance from the loaded class
            Constructor constructor = loadedMyClass.getConstructor();
            Object myClassObject = constructor.newInstance();

            return myClassObject;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
