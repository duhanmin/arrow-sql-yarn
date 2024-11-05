package com.on.yarn;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.annotations.VisibleForTesting;
import com.on.yarn.admin.RoutSQL;
import com.on.yarn.constant.Constants;
import com.on.yarn.arrow.Executor;
import com.on.yarn.job.YarnJob;
import com.on.yarn.util.Log4jPropertyHelper;
import com.on.yarn.util.YarnHelper;
import lombok.Data;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    // Configuration
    private Configuration conf;
    private FileSystem fs;
    // Handle to communicate with the Resource Manager
    private AMRMClientAsync amRMClient;
    // In both secure and non-secure modes, this points to the job-submitter.
    private UserGroupInformation appSubmitterUgi;
    // Application Attempt Id ( combination of attemptId and fail count )
    @VisibleForTesting
    protected ApplicationAttemptId appAttemptID;
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";
    // App Master configuration
    // No. of containers to run shell command on
    @VisibleForTesting
    protected int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 10;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;
    //The submitter saves the running status
    private String checkpoint = "";
    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    @VisibleForTesting
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    @VisibleForTesting
    protected AtomicInteger numRequestedContainers = new AtomicInteger();
    // Args to be passed to the shell command
    private String shellArgs = "";
    private String javaOpts = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<>();
    private static volatile boolean done;
    private static volatile boolean doneJob = false;
    private ByteBuffer allTokens;
    private ConcurrentHashMap<ContainerId, Container> runningContainers = new ConcurrentHashMap<>();
    // Container memory overhead in MB
    private int memoryOverhead = 10;
    private static int amMemory = 128;
    private static Executor donexecutor = null;

    public static void main(String[] args) {
        boolean result = false;
        ApplicationMaster appMaster = null;
        try {
            appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            LOG.info("ApplicationMaster finish...");
            donexecutor = RoutSQL.getExecutor();
            appMaster.checkpoint();
            long startTime = System.currentTimeMillis();
            donexecutor.run();
            LOG.info("sql运行时间: " + (System.currentTimeMillis() - startTime) + " 毫秒");
            appMaster.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "log:" + donexecutor.getLog());
            done = true;
            doneJob = true;
            result = appMaster.finish();
            LOG.info("ApplicationMaster finish");
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            if (appMaster != null) {
                appMaster.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "log:" + donexecutor.getLog());
            }
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        } finally {
            if (null != appMaster) appMaster.stop();
            if (result) {
                LOG.info("Application Master completed successfully. exiting");
                System.exit(0);
            } else {
                LOG.info("Application Master failed. exiting");
                System.exit(2);
            }
        }
    }

    public void stop() {
        if (null != amRMClient) {
            try {
                amRMClient.stop();
            } catch (Exception e) {
            }
        }
        IoUtil.close(fs);
        IoUtil.close(amRMClient);
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private void dumpOutDebugInfo() {
        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        BufferedReader buf = null;
        try {
            String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir")
                    : Shell.execCommand("ls", "-al");
            buf = new BufferedReader(new StringReader(lines));
            String line;
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.cleanup(LOG, buf);
        }
    }

    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
        conf.set("yarn.resourcemanager.am.max-attempts", "3");
        conf.set("mapreduce.am.max-attempts", "3");
        conf.set("yarn.client.failover-sleep-base-ms", "1000");
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("master_memory", true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("memory_overhead", true,
                "Amount of memory overhead in MB for container");
        opts.addOption("java_opts", true, "Java opts for container");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("checkpoint", true, "The submitter saves the running status");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        //Check whether customer log4j.properties file exists
        if (FileUtil.exist(Constants.LOG_4_J_PATH)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, Constants.LOG_4_J_PATH);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "128"));
        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (FileUtil.exist(Constants.SHELL_ARGS_PATH)) {
            shellArgs = FileUtil.readUtf8String(Constants.SHELL_ARGS_PATH);
        }

        if (FileUtil.exist(Constants.JAVA_OPTS_PATH)) {
            javaOpts = FileUtil.readUtf8String(Constants.JAVA_OPTS_PATH);
        }

        if (cliParser.hasOption("shell_env")) {
            String[] shellEnvs = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
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
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        memoryOverhead = Integer.parseInt(cliParser.getOptionValue("memory_overhead", "1"));

        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        checkpoint = cliParser.getOptionValue("checkpoint", "");

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public void run() throws Throwable {
        LOG.info("Starting ApplicationMaster");
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();
        amRMClient.registerApplicationMaster(NetUtils.getHostname(), appMasterRpcPort, appMasterTrackingUrl);
        if (StrUtil.isNotBlank(checkpoint)) {
            fs = YarnHelper.getFileSystem(conf, checkpoint);
        }
    }

    @VisibleForTesting
    protected boolean finish() {
        // wait for completion.
        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        // TODO: 2023/2/15 试试这里,不管node线程,直接结束
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            if (doneJob) {
                appStatus = FinalApplicationStatus.SUCCEEDED;
            } else {
                appStatus = FinalApplicationStatus.FAILED;
                appMessage = "Diagnostics." + ", total=" + numTotalContainers
                        + ", completed=" + numCompletedContainers.get() + ", allocated="
                        + numAllocatedContainers.get() + ", failed="
                        + numFailedContainers.get();
                success = false;
            }
        }

        unregisterApplicationMaster(appStatus, appMessage);
        amRMClient.stop();

        return success;
    }

    public void unregisterApplicationMaster(FinalApplicationStatus appStatus, String appMessage) {
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }
    }

    /**
     * RMCallbackHandler
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());
                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();

                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
                }
                runningContainers.remove(containerStatus.getContainerId());
            }

            // ask for more containers if any failed
            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClient.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            return (float) numCompletedContainers.get()
                    / numTotalContainers;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amRMClient.stop();
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        // TODO - what is the range for priority? how to decide?
        Priority pri = Priority.newInstance(requestPriority);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(containerMemory + memoryOverhead,
                containerVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        LOG.info("Requested container ask: " + request);
        return request;
    }

    private void checkpoint() {
        if (StrUtil.isNotBlank(checkpoint)) {
            String app = appAttemptID.getApplicationId().toString();
            long tmp = System.currentTimeMillis();
            ThreadUtil.execAsync(() -> {
                while (true) {
                    try {
                        ThreadUtil.sleep(3000);
                        Path filePath = new Path(checkpoint + "/" + app);
                        long cost = System.currentTimeMillis() - tmp;
                        if (fs.exists(filePath)) {
                            String appInfo = IoUtil.read(fs.open(filePath), StandardCharsets.UTF_8);
                            YarnJob yarnJob = JSONUtil.toBean(appInfo, YarnJob.class);
                            cost = System.currentTimeMillis() - yarnJob.getDate();
                        } else {
                            LOG.info("not found checkpoint file.");
                        }
                        if (cost > 15000) {
                            stop();
                            LOG.error("checkpoint > 30s");
                            Runtime.getRuntime().halt(1);
                        }
                    } catch (IOException e) {
                        LOG.error("checkpoint error", e);
                    }
                }
            }, true);
        }
    }

    private void kill() {

    }
}
