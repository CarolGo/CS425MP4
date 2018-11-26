package cs425.crane.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

public class FunctionLoader {
    private static final Logger logger = LoggerFactory.getLogger(FunctionLoader.class);

    /**
     * Find and load the first found class file
     *
     * @param directory Path of the class file (or folders containing it)
     * @param classpath CP of the class
     * @return A Constructor of Mp4Function
     */
    public static Constructor<? extends Mp4Function> loadClass(String directory, String classpath) {
        File classDir = new File(directory);
        if (!classDir.isDirectory()) {
            try {
                // Found matching class, return
                return extractOneClass(classDir, classpath);
            } catch (IOException e) {
                logger.error("IO error when loading class", e);
            } catch (NoSuchMethodException | ClassNotFoundException e) {
                logger.error("Class not loadable", e);
            }
        } else {
            File[] tmp = classDir.listFiles();
            if (tmp != null) {
                for (File jar : tmp) {
                    try {
                        return extractOneClass(jar, classpath);
                    } catch (IOException e) {
                        logger.error("IO error when loading class", e);
                    } catch (ClassNotFoundException e) {
                        // Looking for other file
                        continue;
                    } catch (NoSuchMethodException e) {
                        logger.error("Class do not have a constructor");
                    }
                }
            } else {
                logger.error("Mysterious error causing <{}> fail to list files.", classDir.getAbsolutePath());
            }
        }
        // All failed, return null
        return null;
    }

    private static Constructor<? extends Mp4Function> extractOneClass(File file, String cp) throws NoSuchMethodException, ClassNotFoundException, IOException {
        URL[] f = {file.toURI().toURL()};
        Constructor<? extends Mp4Function> res;
        try (URLClassLoader cl = URLClassLoader.newInstance(f)) {
            Class<?> clazz = Class.forName(cp, true, cl);
            Class<? extends Mp4Function> newClass = clazz.asSubclass(Mp4Function.class);
            res = newClass.getConstructor();
        }
        return res;
    }

}
