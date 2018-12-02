package jnrfuse.utils;

import java.io.IOException;
import java.nio.file.Path;

import jnrfuse.FuseException;


public class MountUtils {
    /**
     * Perform/force a umount at the provided Path
     */
    public static void umount(Path mountPoint) {
        try {
            String mountPath = mountPoint.toAbsolutePath().toString();
            try {
                new ProcessBuilder("fusermount", "-u", "-z", mountPath).start();
            } catch (IOException e) {
                new ProcessBuilder("umount", mountPath).start();
            }
        } catch (IOException e) {
            throw new FuseException("Unable to umount FS", e);
        }
    }
}
