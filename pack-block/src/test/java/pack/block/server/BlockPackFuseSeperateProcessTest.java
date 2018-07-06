package pack.block.server;

public class BlockPackFuseSeperateProcessTest {
  //
  // private static final String TEST_BLOCK_PACK_FUSE_SEPERATE_PROCESS =
  // "testBlockPackFuseSeperateProcess";
  //
  // private static final Logger LOGGER =
  // LoggerFactory.getLogger(BlockPackFuseSeperateProcessTest.class);
  //
  // private static final String USER_NAME = "user.name";
  // private static final String TEST_BLOCK_PACK_FUSE_FILE_SYSTEM =
  // "testBlockPackFuseFileSystem";
  // private static final String RW = "rw";
  // private static final String HDFS = "hdfs";
  // private static final String ZK = "zk";
  // private static final int MAX_FILE_SIZE = 100_000;
  // private static final int MAX_PASSES = 1000;
  // private static final int MIN_PASSES = 100;
  // private static final int MAX_BUFFER_SIZE = 16000;
  // private static final int MIN_BUFFER_SIZE = 1000;
  // private static MiniDFSCluster cluster;
  // private static FileSystem fileSystem;
  // private static File root = new File("./target/tmp/" +
  // BlockPackFuseSeperateProcessTest.class.getName());
  // private static ZkMiniCluster zkMiniCluster;
  // private static String zkConnection;
  // private static int zkTimeout;
  // private static long seed;
  //
  // @BeforeClass
  // public static void setup() throws IOException {
  // String user = System.getProperty(USER_NAME);
  // Utils.exec(LOGGER, "sudo", "chown", "-R", user + ":" + user,
  // root.getAbsolutePath());
  // Utils.rmr(root);
  // File storePathDir = Utils.mkdir(new File(root, HDFS));
  // Configuration configuration = new Configuration();
  // String storePath = storePathDir.getAbsolutePath();
  // configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
  // cluster = new MiniDFSCluster.Builder(configuration).build();
  // fileSystem = cluster.getFileSystem();
  //
  // File zk = Utils.mkdir(new File(root, ZK));
  // zkMiniCluster = new ZkMiniCluster();
  // zkMiniCluster.startZooKeeper(zk.getAbsolutePath(), true);
  // zkConnection = zkMiniCluster.getZkConnectionString();
  // zkTimeout = 10000;
  // seed = new Random().nextLong();
  // }
  //
  // @AfterClass
  // public static void teardown() {
  // cluster.shutdown();
  // zkMiniCluster.shutdownZooKeeper();
  // }
  //
  // // @Test
  // public void testBlockPackFuseSeperateProcess() throws Exception {
  // Path volumePath = new Path("/BlockPackFuseTest/" +
  // TEST_BLOCK_PACK_FUSE_SEPERATE_PROCESS);
  // fileSystem.delete(volumePath, true);
  // HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
  // .length(100L * 1024L * 1024L * 1024L)
  // .build();
  //
  // HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
  //
  // File testRoot = new File(root, TEST_BLOCK_PACK_FUSE_SEPERATE_PROCESS);
  //
  // File conf = new File(testRoot, "conf");
  // conf.mkdirs();
  // writeConfiguration(fileSystem.getConf(), new File(conf, "hdfs-site.xml"));
  // writeConfiguration(fileSystem.getConf(), new File(conf, "core-site.xml"));
  //
  // String hdfVolumePath = "/BlockPackFuseTest/" +
  // TEST_BLOCK_PACK_FUSE_SEPERATE_PROCESS;
  // String volumeName = TEST_BLOCK_PACK_FUSE_SEPERATE_PROCESS;
  // String fuseMountLocation = mkdirs(new File(testRoot,
  // "fuse")).getAbsolutePath();
  // String fsMountLocation = mkdirs(new File(testRoot,
  // "fs")).getAbsolutePath();
  // String fsMetricsLocation = mkdirs(new File(testRoot,
  // "metrics")).getAbsolutePath();
  // String fsLocalCache = mkdirs(new File(testRoot,
  // "cache")).getAbsolutePath();
  // String logOutput = mkdirs(new File(testRoot, "logging")).getAbsolutePath();
  // String unixSock = new File(testRoot, "sock").getAbsolutePath()
  // .replace("/./", "/");
  // String libDir = mkdirs(new File(testRoot, "lib")).getAbsolutePath();
  // int numberOfMountSnapshots = 5;
  // long volumeMissingPollingPeriod = TimeUnit.SECONDS.toMillis(4);
  // int volumeMissingCountBeforeAutoShutdown = 5;
  // boolean countDockerDownAsMissing = true;
  //
  // Process process = BlockPackFuseProcessBuilder.startProcess(false,
  // fuseMountLocation, fsMountLocation,
  // fsMetricsLocation, fsLocalCache, hdfVolumePath, zkConnection, zkTimeout,
  // volumeName, logOutput, unixSock,
  // libDir, numberOfMountSnapshots, volumeMissingPollingPeriod,
  // volumeMissingCountBeforeAutoShutdown,
  // countDockerDownAsMissing, ImmutableList.of(conf.getAbsolutePath()));
  //
  // waitForProcessToExit(process);
  //
  // File compactorDir = new File(testRoot, "compactor");
  // List<Path> pathList = new ArrayList<>();
  // pathList.add(new Path("/BlockPackFuseTest"));
  // AtomicBoolean running = new AtomicBoolean(true);
  // try (PackCompactorServer packCompactorServer = new
  // PackCompactorServer(compactorDir, fileSystem, pathList,
  // zkConnection, zkTimeout)) {
  // Thread thread = new Thread(() -> {
  // while (running.get()) {
  // try {
  // packCompactorServer.executeCompaction();
  // Thread.sleep(TimeUnit.SECONDS.toMillis(10));
  // } catch (Exception e) {
  // LOGGER.error("Unknown error", e);
  // }
  // }
  // });
  // thread.start();
  //
  // File unixSockFile = new File(unixSock);
  // BlockPackStorage.waitForMount(new File(fsMountLocation), unixSockFile);
  //
  // // BlockPackAdminClient client =
  // // BlockPackAdminClient.create(unixSockFile);
  //
  // System.out.println("Mounted.....");
  //
  // BlockPackStorage.umountVolume(unixSockFile);
  //
  // // int numberOfFiles = 100;
  // // int threads = 20;
  // //
  // // ExecutorService pool = Executors.newCachedThreadPool();
  // //
  // // String user = System.getProperty(USER_NAME);
  // //
  // // ImmutableMap.Builder<File, Future<Map<String, String>>> builder =
  // // ImmutableMap.builder();
  // // ImmutableMap<File, Future<Map<String, String>>> map;
  // //
  // // Utils.exec(LOGGER, "sudo", "chown", "-R", user + ":" + user,
  // // fsMountLocation);
  // // File baseDir = new File(fsMountLocation);
  // // for (int pass = 0; pass < threads; pass++) {
  // // File testDir = new File(baseDir, Integer.toString(pass));
  // // Future<Map<String, String>> future = pool.submit(() ->
  // // testFileSystem(testDir, numberOfFiles));
  // // builder.put(testDir, future);
  // // }
  // // pool.shutdown();
  // // pool.awaitTermination(1, TimeUnit.MINUTES);
  // // map = builder.build();
  // // for (Entry<File, Future<Map<String, String>>> e : map.entrySet()) {
  // // File testDir = e.getKey();
  // // Future<Map<String, String>> future = e.getValue();
  // // validateHashes(testDir, future.get());
  // // }
  //
  // // try (BlockPackFuse blockPackFuse = new BlockPackFuse(fuseConfig)) {
  // // blockPackFuse.mount(false);
  // // for (Entry<File, Future<Map<String, String>>> e : map.entrySet()) {
  // // File testDir = e.getKey();
  // // Future<Map<String, String>> future = e.getValue();
  // // validateHashes(testDir, future.get());
  // // }
  // // }
  //
  // running.set(false);
  // thread.interrupt();
  // thread.join();
  // }
  // }
  //
  // private void waitForProcessToExit(Process process) throws
  // InterruptedException, IOException {
  // int exit = process.waitFor();
  // if (exit == 0) {
  //
  // String stdout = IOUtils.toString(process.getInputStream(), "UTF-8");
  // System.out.println("=================");
  // System.out.println(stdout);
  //
  // String stderr = IOUtils.toString(process.getErrorStream(), "UTF-8");
  // System.err.println("=================");
  // System.err.println(stderr);
  //
  // return;
  // } else {
  //
  // String stdout = IOUtils.toString(process.getInputStream(), "UTF-8");
  // System.out.println("=================");
  // System.out.println(stdout);
  //
  // String stderr = IOUtils.toString(process.getErrorStream(), "UTF-8");
  // System.err.println("=================");
  // System.err.println(stderr);
  //
  // throw new RuntimeException();
  // }
  //
  // }
  //
  // private File mkdirs(File file) {
  // file.mkdirs();
  // return file;
  // }
  //
  // private void validateHashes(File testDir, Map<String, String> hashes)
  // throws IOException {
  // for (Entry<String, String> entry : hashes.entrySet()) {
  // String hash = entry.getValue();
  // File file = new File(testDir, entry.getKey());
  // assertEquals(hash, Md5Utils.md5AsBase64(file));
  // }
  // }
  //
  // private Map<String, String> testFileSystem(File baseDir, int numberOfFiles)
  // throws IOException {
  // baseDir.mkdirs();
  // Map<String, String> fileHashes = new ConcurrentHashMap<>();
  // Random random = new Random(seed);
  // for (int i = 0; i < numberOfFiles; i++) {
  // String fileName = Long.toString(random.nextLong());
  // long fileLength = random.nextInt(MAX_FILE_SIZE);
  // String md5 = generateFile(random, baseDir, fileName, (int) fileLength);
  // fileHashes.put(fileName, md5);
  // }
  // return fileHashes;
  // }
  //
  // private String generateFile(Random random, File baseDir, String fileName,
  // int fileLength) throws IOException {
  // File file = new File(baseDir, fileName);
  // long passSeed = random.nextLong();
  // int passes = random.nextInt(MAX_PASSES - MIN_PASSES) + MIN_PASSES;
  // int maxBuf = random.nextInt(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE) +
  // MIN_BUFFER_SIZE;
  // try (RandomAccessFile rand = new RandomAccessFile(file, RW)) {
  // rand.setLength(fileLength);
  // writeRandomFile(rand, fileLength, passSeed, passes, maxBuf);
  // }
  // return Md5Utils.md5AsBase64(file);
  // }
  //
  // private void writeRandomFile(RandomAccessFile rand, int fileLength, long
  // passSeed, int passes, int maxBuf)
  // throws IOException {
  // Random random = new Random(passSeed);
  // for (int pass = 0; pass < passes; pass++) {
  // long pos = random.nextInt(fileLength);
  // int length = random.nextInt(maxBuf);
  // int len = (int) Math.min(length, fileLength - pos);
  // byte[] buf = new byte[len];
  // random.nextBytes(buf);
  // rand.seek(pos);
  // rand.write(buf);
  // }
  // }
  //
  // private static void writeConfiguration(Configuration conf, File file)
  // throws IOException {
  // try (OutputStream outputStream = new FileOutputStream(file)) {
  // conf.writeXml(outputStream);
  // }
  // }
}
