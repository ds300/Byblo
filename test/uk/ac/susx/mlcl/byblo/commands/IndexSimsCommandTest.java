/*
 * Copyright (c) 2010, Hamish Morgan.
 * All Rights Reserved.
 */
package uk.ac.susx.mlcl.byblo.commands;

import java.io.File;
import org.junit.*;
import static uk.ac.susx.mlcl.TestConstants.*;
import uk.ac.susx.mlcl.byblo.io.TokenPairSource;

/**
 *
 * @author hamish
 */
public class IndexSimsCommandTest {

    public IndexSimsCommandTest() {
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

    @Test
    public void testRunOnFruitAPI_noskip_compact() throws Exception {
        testRunOnFruitAPI("compact-noskip-", false, false, true);
    }

    @Test
    public void testRunOnFruitAPI_skipboth_compact() throws Exception {
        testRunOnFruitAPI("compact-skipboth-", true, true, true);
    }

    @Test
    public void testRunOnFruitAPI_skipleft_compact() throws Exception {
        testRunOnFruitAPI("compact-skipleft-", true, false, true);
    }

    @Test
    public void testRunOnFruitAPI_skipright_compact() throws Exception {
        testRunOnFruitAPI("compact-skipright-", false, true, true);
    }

    @Test
    public void testRunOnFruitAPI_noskip_verbose() throws Exception {
        testRunOnFruitAPI("verbose-noskip-", false, false, false);
    }

    @Test
    public void testRunOnFruitAPI_skipboth_verbose() throws Exception {
        testRunOnFruitAPI("verbose-skipboth-", true, true, false);
    }

    @Test
    public void testRunOnFruitAPI_skipleft_verbose() throws Exception {
        testRunOnFruitAPI("verbose-skipleft-", true, false, false);
    }

    @Test
    public void testRunOnFruitAPI_skipright_verbose() throws Exception {
        testRunOnFruitAPI("verbose-skipright-", false, true, false);
    }

    public void testRunOnFruitAPI(
            String prefix, boolean skip1, boolean skip2, boolean compact) throws Exception {
        System.out.println("Testing " + IndexSimsCommandTest.class.getName()
                + " on " + TEST_FRUIT_SIMS_100NN);

        final String name = TEST_FRUIT_SIMS_100NN.getName();
        final File out = new File(TEST_OUTPUT_DIR, prefix + name + ".indexed");
        File out2 = suffix(out, ".unindexed");
        final File idx = new File(TEST_OUTPUT_DIR, name + ".entry-index");

        deleteIfExist(out, idx);

        indexSims(TEST_FRUIT_SIMS_100NN, out, idx, skip1, skip2,
                compact);

        unindexSims(out, out2, idx, skip1, skip2, compact);

        TokenPairSource.equal(out, out2, DEFAULT_CHARSET);

    }

    private static void indexSims(File from, File to, File index,
                                boolean skip1, boolean skip2, boolean compact)
            throws Exception {
        assertValidInputFiles(from);
        assertValidOutpuFiles(to, index);

        IndexSimsCommand unindex = new IndexSimsCommand();
        unindex.getFilesDeligate().setCharset(DEFAULT_CHARSET);
        unindex.getFilesDeligate().setSourceFile(from);
        unindex.getFilesDeligate().setDestinationFile(to);
        unindex.getFilesDeligate().setCompactFormatDisabled(!compact);
        unindex.getIndexDeligate().setIndexFile(index);
        unindex.getIndexDeligate().setSkipIndexed1(skip1);
        unindex.getIndexDeligate().setSkipIndexed2(skip2);
        unindex.runCommand();

        assertValidInputFiles(to, index);
        assertSizeGT(from, to);
    }

    private static void unindexSims(File from, File to, File index,
                                  boolean skip1, boolean skip2, boolean compact)
            throws Exception {
        assertValidInputFiles(from, index);
        assertValidOutpuFiles(to);

        UnindexSimsCommand unindex = new UnindexSimsCommand();
        unindex.getFilesDeligate().setCharset(DEFAULT_CHARSET);
        unindex.getFilesDeligate().setSourceFile(from);
        unindex.getFilesDeligate().setDestinationFile(to);
        unindex.getFilesDeligate().setCompactFormatDisabled(!compact);
        unindex.getIndexDeligate().setIndexFile(index);
        unindex.getIndexDeligate().setSkipIndexed1(skip1);
        unindex.getIndexDeligate().setSkipIndexed2(skip2);
        unindex.runCommand();

        assertValidInputFiles(to);
        assertSizeGT(to, from);
    }
}