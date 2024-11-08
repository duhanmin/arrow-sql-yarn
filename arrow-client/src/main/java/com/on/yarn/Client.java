package com.on.yarn;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.on.yarn.base.LocalYarnManipulator;
import com.on.yarn.base.YarnManipulator;
import com.on.yarn.constant.Constants;
import com.on.yarn.job.YarnJob;
import com.on.yarn.util.Log4jPropertyHelper;
import com.on.yarn.util.YarnHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import static com.on.yarn.constant.Constants.OSS;
import static com.on.yarn.constant.Constants.S_3;
import static com.on.yarn.constant.Constants.S_3_A;
import static com.on.yarn.constant.Constants.S_3_N;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    // Configuration
    private final Configuration conf;
    private final YarnClient yarnClient;
    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 10;
    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;
    // Application master jar file
    private String appMasterJar = "";
    // Main class to invoke application master
    private final String appMasterMainClass;
    private String[] shellArgs = new String[]{};
    private String[] javaOpts = new String[]{};
    // Env variables to be setup for the shell command
    private final Map<String, String> shellEnv = new HashMap<>();
    // Shell Command Container priority
    private final int shellCmdPriority = 0;
    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 10;
    // Amt. of virtual cores to request for container in which shell script will be executed
    private int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;
    // log4j.properties file
    // if available, add to local resources and set into classpath
    private String log4jPropFile = "";
    //The submitter saves the running status
    private String checkpoint = "";
    // Start time for client
    private final long clientStartTime = System.currentTimeMillis();
    // Timeout threshold for client. Kill app after time interval expires.
    // -1 means no timeout so that the application will not be killed after timeout,
    // in other words, long time running job will be kept running.
    private long clientTimeout = -1;
    // flag to indicate whether to keep containers across application attempts.
    private boolean keepContainers = false;
    // Debug flag
    boolean debugFlag = false;
    // Command line options
    private final Options opts;
    private int memoryOverhead = 50;
    private boolean reflect_run = true;
    private FileSystem fs;
    private String jobSql = "";
    private String user;
    private static YarnManipulator yarnManipulator;
    private List<String> parameter = CollUtil.newArrayList();
    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        yarnManipulator = new LocalYarnManipulator();
        try {
            Client client = new Client();
            yarnManipulator.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            ApplicationId app = client.run();
            result = client.monitorApplication(app);
        } catch (Throwable t) {
            yarnManipulator.error("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            yarnManipulator.info("Application completed successfully");
            System.exit(0);
        }
        yarnManipulator.error("Application failed");
        System.exit(2);
    }

    public Client(YarnManipulator yarnManipulator) throws Exception {
        this(new YarnConfiguration());
        this.yarnManipulator = yarnManipulator;
    }

    /**
     */
    public Client(Configuration conf) {
        this("com.on.yarn.ApplicationMaster", conf);
    }

    Client(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        // set am retry to a lot of times
        conf.set("yarn.resourcemanager.am.max-attempts", "3");
        conf.set("mapreduce.am.max-attempts", "3");
        conf.set("yarn.client.failover-sleep-base-ms","1000");
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("memory_overhead", true, "Amount of memory overhead in MB for application master and container");
        opts.addOption("jar_path", true, "Jar file containing the application master in local file system");
        opts.addOption("job", true, "Jar file containing the application master in HDFS");
        opts.addOption("proxy_user", true, "proxy_user");
        opts.addOption("shell_args", true, "Command line args for the shell script."
                + "Multiple args can be separated by empty space.");
        opts.addOption("java_opts", true, "Java opts for container");
        opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES);
        opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("keep_containers_across_application_attempts", false,
                "Flag to indicate whether to keep containers across application attempts."
                        + " If the flag is true, running containers will not be killed when"
                        + " application attempt fails and these containers will be retrieved by"
                        + " the new application attempt ");
        opts.addOption("checkpoint", true, "The submitter saves the running status");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    /**
     */
    public Client() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     *
     * @param args Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("log_properties")) {
            String log4jPath = cliParser.getOptionValue("log_properties");
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
            } catch (Exception e) {
                yarnManipulator.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        if (cliParser.hasOption("keep_containers_across_application_attempts")) {
            yarnManipulator.info("keep_containers_across_application_attempts");
            keepContainers = true;
        }

        appName = cliParser.getOptionValue("appname", "AppOnYarnDemo");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "128"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

        if (amMemory == 1024) amMemory = 825;
        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("jar_path")) {
            throw new IllegalArgumentException("No jar_path file path specified for application master");
        }
        appMasterJar = cliParser.getOptionValue("jar_path");

        if (!cliParser.hasOption("job")) {
            throw new IllegalArgumentException("No job file path specified for application master");
        }
        jobSql = FileUtil.readUtf8String(cliParser.getOptionValue("job"));

        user = cliParser.getOptionValue("proxy_user");

        if (StringUtils.isNotBlank(user)) {
            String mkdirStr = "hdfs dfs -mkdir /user/{}";
            String chownStr = "hdfs dfs -chown -R {} /user/{}";
            Constants.exec(StrUtil.format(mkdirStr, user));
            Constants.exec(StrUtil.format(chownStr,user, user));
            System.setProperty("HADOOP_USER_NAME", user);
        }

        if (cliParser.hasOption("shell_args")) {
            shellArgs = cliParser.getOptionValues("shell_args");
        }
        if (cliParser.hasOption("java_opts")) {
            javaOpts = cliParser.getOptionValues("java_opts");
        }

        if (cliParser.hasOption("shell_env")) {
            String[] envs = cliParser.getOptionValues("shell_env");
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        memoryOverhead = Integer.parseInt(cliParser.getOptionValue("memory_overhead", "2"));
        reflect_run = Boolean.parseBoolean(cliParser.getOptionValue("reflect_run", "true"));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

        clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "-1"));
        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        if (cliParser.hasOption("p")) {
            String p = cliParser.getOptionValue("p");
            if (StrUtil.isNotBlank(p)){
                for (String str : StrUtil.split(p, ",")) {
                    if(StrUtil.isNotBlank(str)){
                        parameter.add(str);
                    }
                }
            }
        }

        log4jPropFile = cliParser.getOptionValue("log_properties", "");
        checkpoint = cliParser.getOptionValue("checkpoint", "");

        return true;
    }

    public void stop() {
        if (null != yarnClient) {
            try {
                yarnClient.stop();
            } catch (Exception e) {
            }
        }
        if (null != fs) {
            IoUtil.close(fs);
        }
    }

    /**
     * Main run function for the client
     *
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public ApplicationId run() throws IOException, YarnException, URISyntaxException {
        yarnManipulator.info("Running Client");
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        yarnManipulator.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                NodeState.RUNNING);
        yarnManipulator.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            yarnManipulator.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        List<String> allQueues = yarnClient.getAllQueues().stream().map(QueueInfo::getQueueName).collect(Collectors.toList());
        yarnManipulator.info("Got all queue:" + allQueues);
        if (!allQueues.contains(this.amQueue)) {
            yarnManipulator.warn("Queue not found, using default queue");
            this.amQueue = "default";
        }
        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        yarnManipulator.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                yarnManipulator.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        // TODO get min/max resource capabilities from RM and change memory ask if needed
        // If we do not have min/max, we may not be able to correctly request
        // the required resources from the RM for the app master
        // Memory ask has to be a multiple of min and less than max.
        // Dump out information about cluster capability as seen by the resource manager
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        yarnManipulator.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory + memoryOverhead > maxMem) {
            yarnManipulator.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem - memoryOverhead;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        yarnManipulator.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            yarnManipulator.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationType("arrow-sql");
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<>();

        yarnManipulator.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        fs = YarnHelper.getFileSystem(conf, appMasterJar);

        Path dst;
        if (StrUtil.startWithAny(appMasterJar, "hdfs", S_3_A, S_3_N, S_3, OSS)) {
            String path = new Path(appMasterJar).toUri().getPath();
            dst = fs.makeQualified(new Path(path));
        } else {
            dst = YarnHelper.upLoad(fs, appMasterJar);
        }
        YarnHelper.addlocalResources(fs, Constants.APP_MASTER_JAR_PATH, localResources, dst);
        YarnHelper.addFrameworkToDistributedCache(fs, dst.toUri().toString(), localResources, conf);

        // Set the log4j properties if needed
        if (!log4jPropFile.isEmpty()) {
            addToLocalResources(fs, log4jPropFile, Constants.LOG_4_J_PATH, appId.toString(), localResources, null);
        }

        if (shellArgs.length > 0) {
            addToLocalResources(fs, null, Constants.SHELL_ARGS_PATH, appId.toString(),localResources, StringUtils.join(shellArgs, " "));
        }

        if (javaOpts.length > 0) {
            addToLocalResources(fs, null, Constants.JAVA_OPTS_PATH, appId.toString(), localResources, StringUtils.join(javaOpts, " "));
        }

        // Set the necessary security tokens as needed
        //amContainer.setContainerTokens(containerToken);

        // Set the env variables to be setup in the env where the application master will be run
        yarnManipulator.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<>();

        env.put("CLASSPATH", YarnHelper.buildClassPathEnv(conf));
        env.put(Constants.JAR_FILE_PATH, dst.toUri().toString());

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./logback.xml");

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./parquet-logging.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("LANG", "zh_CN.UTF-8");
        env.put("CLASSPATH", classPathEnv.toString());

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<>(30);

        // Set java executable command
        yarnManipulator.info("Setting up app master command");
        vargs.add("java");
        vargs.add("-server");
        // Set Xmx based on am memory size
        vargs.add("-Xms" + 96 + "m");
        if (reflect_run){
            vargs.add("-Xmx" + amMemory + "m");
        }
        vargs.add("-Dreflect=" + reflect_run);
        vargs.add("-Dloglevel=info");
        vargs.add("-Djava.security.egd=file:///dev/urandom");
        vargs.add("-Duser.language=zh");
        vargs.add("-Dfile.encoding=utf-8");
        vargs.add("-D"+ Constants.JOB_SQL + "=" + Base64.encode(jobSql));

        if (CollUtil.isNotEmpty(parameter)){
            for (String p : parameter) {
                vargs.add("-D" + p);
            }
        }

        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("--container_memory " + containerMemory);
        vargs.add("--container_vcores " + containerVirtualCores);
        vargs.add("--memory_overhead " + memoryOverhead);
        vargs.add("--num_containers " + numContainers);
        vargs.add("--priority " + shellCmdPriority);
        vargs.add("--master_memory " + amMemory);
        if (StrUtil.isNotBlank(checkpoint)) {
            vargs.add("--checkpoint " + checkpoint);
        }
        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        yarnManipulator.info("Completed setting up app master command " + command);
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Resource.newInstance(amMemory + memoryOverhead, amVCores);
        appContext.setResource(capability);

        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final Token<?>[] tokens =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    yarnManipulator.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        // TODO - what is the range for priority? how to decide?
        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on success
        // or an exception thrown to denote some form of a failure
        yarnManipulator.info("Submitting application to ASM");

        yarnClient.submitApplication(appContext);

        // TODO
        // Try submitting the same request again
        // app submission failure?

        // Monitor the application
        return appId;

    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    public boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 3 second.
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                yarnManipulator.error("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            if (StrUtil.isNotBlank(checkpoint)) {
                YarnJob yarnJob = new YarnJob(appId.toString(), report.getYarnApplicationState().toString());
                Path path = new Path(checkpoint + "/" + yarnJob.getAppId());
                try (FSDataOutputStream fsDataOutputStream = fs.create(path, true)) {
                    fsDataOutputStream.writeBytes(yarnJob.toString());
                    fsDataOutputStream.flush();
                }
            }

            yarnManipulator.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    yarnManipulator.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    yarnManipulator.info("Application did finished unsuccessfully."
                            + " YarnState=" + state + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                yarnManipulator.info("Application did not finish."
                        + " YarnState=" + state + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            if (clientTimeout > 0 && System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                yarnManipulator.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;
            }
        }

    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    public void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
        // the same time.
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
    }

    private Path addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        return YarnHelper.addlocalResources(fs, fileSrcPath, fileDstPath, localResources, resources, suffix);
    }
}
