package jnrfuse.struct;


import org.junit.Test;

import jnrfuse.examples.MemoryFS;
import jnrfuse.struct.BaseFsTest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class MemoryFsTest extends BaseFsTest {
    @Test
    public void testReadWrite() throws Exception {
        MemoryFS stub = new MemoryFS();

        Path tmpDir = Files.createTempDirectory("hellofuse");
        blockingMount(stub, tmpDir);
        try {
            File fileOne = new File(tmpDir.toAbsolutePath() + "/file1");
            assertFalse("file mustn't exist", fileOne.exists());
            assertTrue("file hasn't been created", fileOne.createNewFile());
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileOne)))) {
                writer.print("test1");
                writer.print("test2");
            }

            String text = Files.lines(fileOne.toPath()).collect(Collectors.joining());
            assertEquals("file content doesn't match what was written", "test1test2", text);

            assertTrue("file can't be deleted", fileOne.delete());
            assertFalse("file mustn't exist", fileOne.exists());
        } finally {
            stub.umount();
        }
    }

}
