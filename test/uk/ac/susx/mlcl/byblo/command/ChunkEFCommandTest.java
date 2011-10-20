/*
 * Copyright (c) 2010, Hamish Morgan.
 * All Rights Reserved.
 */
package uk.ac.susx.mlcl.byblo.command;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.susx.mlcl.TestConstants;
import static org.junit.Assert.*;
import uk.ac.susx.mlcl.lib.io.FileFactory;
import uk.ac.susx.mlcl.lib.io.TempFileFactory;

/**
 *
 * @author hamish
 */
public class ChunkEFCommandTest {

    public ChunkEFCommandTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of setMaxChunkSize method, of class ChunkEFCommand.
     */
    @Test
    public void testSetMaxChunkSize() {
        System.out.println("setMaxChunkSize");
        int maxChunkSize = 1;
        ChunkEFCommand instance = new ChunkEFCommand();
        instance.setMaxChunkSize(maxChunkSize);
    }

    /**
     * Test of getCharset method, of class ChunkEFCommand.
     */
    @Test
    public void testGetCharset() {
        System.out.println("getCharset");
        ChunkEFCommand instance = new ChunkEFCommand(
                TestConstants.TEST_FRUIT_INPUT, TestConstants.DEFAULT_CHARSET);
        Charset expResult = TestConstants.DEFAULT_CHARSET;
        Charset result = instance.getCharset();
        assertEquals(expResult, result);
    }

    /**
     * Test of setCharset method, of class ChunkEFCommand.
     */
    @Test
    public void testSetCharset() {
        System.out.println("setCharset");
        Charset charset = Charset.forName("UTF-8");
        ChunkEFCommand instance = new ChunkEFCommand();
        instance.setCharset(charset);
    }

    /**
     * Test of getMaxChunkSize method, of class ChunkEFCommand.
     */
    @Test
    public void testGetMaxChunkSize() {
        System.out.println("getMaxChunkSize");
        ChunkEFCommand instance = new ChunkEFCommand();
        int expResult = ChunkEFCommand.DEFAULT_MAX_CHUNK_SIZE;
        int result = instance.getMaxChunkSize();
        assertEquals(expResult, result);
    }

    /**
     * Test of setSrcFile method, of class ChunkEFCommand.
     */
    @Test
    public void testSetSrcFile() {
        System.out.println("setSrcFile");
        File sourceFile = TestConstants.TEST_FRUIT_INPUT;
        ChunkEFCommand instance = new ChunkEFCommand();
        instance.setSrcFile(sourceFile);
    }

    /**
     * Test of getSrcFile method, of class ChunkEFCommand.
     */
    @Test
    public void testGetSrcFile() {
        System.out.println("getSrcFile");
        ChunkEFCommand instance = new ChunkEFCommand(
                TestConstants.TEST_FRUIT_INPUT, TestConstants.DEFAULT_CHARSET);
        File expResult = TestConstants.TEST_FRUIT_INPUT;
        File result = instance.getSrcFile();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDestFiles method, of class ChunkEFCommand.
     */
    @Test
    public void testGetDestFiles() {
        System.out.println("getDestFiles");
        ChunkEFCommand instance = new ChunkEFCommand();
        Collection<File> expResult = new ArrayList<File>();
        Collection<File> result = instance.getDestFiles();
        assertArrayEquals(expResult.toArray(), result.toArray());
    }

    /**
     * Test of getChunkFileFactory method, of class ChunkEFCommand.
     */
    @Test
    public void testGetChunkFileFactory() {
        System.out.println("getChunkFileFactory");
        ChunkEFCommand instance = new ChunkEFCommand();
        FileFactory expResult = new TempFileFactory();
        FileFactory result = instance.getChunkFileFactory();
        assertEquals(expResult, result);
    }

    /**
     * Test of setChunkFileFactory method, of class ChunkEFCommand.
     */
    @Test
    public void testSetChunkFileFactory() {
        System.out.println("setChunkFileFactory");
        FileFactory chunkFileFactory = new TempFileFactory();
        ChunkEFCommand instance = new ChunkEFCommand();
        instance.setChunkFileFactory(chunkFileFactory);
    }

//    /**
//     * Test of getDstFileQueue method, of class ChunkEFCommand.
//     */
//    @Test
//    public void testGetDstFileQueue() {
//        System.out.println("getDstFileQueue");
//        ChunkEFCommand instance = new ChunkEFCommand();
//        BlockingQueue expResult = null;
//        BlockingQueue result = instance.getDstFileQueue();
//        assertEquals(expResult, result);
//    }
//
//    /**
//     * Test of setDstFileQueue method, of class ChunkEFCommand.
//     */
//    @Test
//    public void testSetDstFileQueue() {
//        System.out.println("setDstFileQueue");
//        BlockingQueue<File> dstFileQueue = null;
//        ChunkEFCommand instance = new ChunkEFCommand();
//        instance.setDstFileQueue(dstFileQueue);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of runTask method, of class ChunkEFCommand.
     */
    @Test
    public void testRun() throws Exception {
        System.out.println("runTask");
        ChunkEFCommand instance = new ChunkEFCommand(
                TestConstants.TEST_FRUIT_INPUT, TestConstants.DEFAULT_CHARSET);
        instance.setMaxChunkSize(1000);
        instance.run();
    }
}
