package com.on.yarn.util;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.on.yarn.constant.Constants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static com.on.yarn.constant.Constants.OSS;
import static com.on.yarn.constant.Constants.S_3;
import static com.on.yarn.constant.Constants.S_3_A;
import static com.on.yarn.constant.Constants.S_3_N;

/**
 * YarnHelper
 */
public class YarnHelper {

    public static String buildClassPathEnv(Configuration conf) {
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/log4j.properties")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CLIENT_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$JAVA_HOME/lib/tools.jar")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/conf/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        return classPathEnv.toString();
    }

    public static void addFrameworkToDistributedCache(FileSystem fs, String javaPathInHdfs, Map<String, LocalResource> localResources, Configuration conf) throws IOException {
        URI uri;
        try {
            uri = new URI(javaPathInHdfs);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to parse '" + javaPathInHdfs + "' as a URI.");
        }
        Path frameworkPath = fs.makeQualified(new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));
        FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), conf);
        frameworkPath = fc.resolvePath(frameworkPath);
        uri = frameworkPath.toUri();
        try {
            uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, Constants.JAR_FILE_LINKEDNAME);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        FileStatus scFileStatus = fs.getFileStatus(frameworkPath);
        LocalResource scRsrc = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(uri),
                        LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(Constants.JAR_FILE_LINKEDNAME, scRsrc);
    }

    public static FileSystem getFileSystem(Configuration conf, String path) throws IOException, URISyntaxException {
        FileSystem fs;
        if (StrUtil.startWithAny(path, S_3_A, S_3_N, S_3)) {
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            conf.set("fs.s3a.impl.disable.cache", "true");
            fs = FileSystem.get(new URI(path), conf);
        } else if (StrUtil.startWithAny(path, OSS)) {
            conf.set("fs.oss.impl.disable.cache", "true");
            fs = FileSystem.get(new URI(path), conf);
        } else {
            conf.set("fs.hdfs.impl.disable.cache", "true");
            fs = FileSystem.get(conf);
        }
        return fs;
    }

    public static Path addToLocalResources(FileSystem fs, String path, String name, Map<String, LocalResource> localResources) throws IOException {
        Path dst = fs.makeQualified(new Path(path));
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(name, scRsrc);
        return dst;
    }

    public static void addlocalResources(FileSystem fs, String fileDstPath, Map<String, LocalResource> localResources, Path dst) throws IOException {
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    public static Path addlocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, Map<String, LocalResource> localResources, String resources, String suffix) throws IOException {
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        addlocalResources(fs, fileDstPath, localResources, dst);
        return dst;
    }

    public static Path upLoad(FileSystem fs, String path) throws IOException {
        String name = FileUtil.getName(path);
        Path yarnJar = new Path(fs.getHomeDirectory(), "yarn_jar/" + name);
        if (!fs.exists(yarnJar)) {
            fs.copyFromLocalFile(new Path(path), yarnJar);
        }
        return yarnJar;
    }

    public static boolean killAppId(String appId) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        try {
            yarnClient.init(new YarnConfiguration());
            yarnClient.start();
            ApplicationId applicationId = ApplicationId.fromString(appId);
            yarnClient.killApplication(applicationId);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(yarnClient);
        }
        return false;
    }
}
