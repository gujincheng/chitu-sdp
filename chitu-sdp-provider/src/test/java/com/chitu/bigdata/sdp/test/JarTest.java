package com.chitu.bigdata.sdp.test;


import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import org.apache.commons.io.IOUtils;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JarTest {


    public static void main(String[] args) throws IOException, XmlPullParserException {



        // 删除本地文件或目录
        boolean delFlag = FileUtil.del("/app/bigdata-sdp/tmp/jar/commons-lang-2.5.jar");
        // 删除失败
        if (!delFlag) {
            System.out.println("删除本地文件失败");
        }








        AtomicBoolean versionVerifyFlag = new AtomicBoolean(true);

        // 将hdfs jar文件下载到本地

        // 获取符合规则的文件名
        JarFile jarFile = new JarFile("D:\\idea_work\\bigdata-flink-dstream\\target\\bigdata-flink-dstream-1.0-SNAPSHOT.jar");
        List<String> fileNamelist = new ArrayList<>();
        Enumeration<JarEntry> en = jarFile.entries();
        while (en.hasMoreElements()) {
            JarEntry entry = en.nextElement();
            String fileName = entry.getName();
            if (fileName.startsWith("META-INF/maven") && fileName.endsWith("pom.xml")) {
                fileNamelist.add(fileName);
            }
        }

        // 遍历校验
        for (String fileName : fileNamelist) {
            JarEntry jarEntry = jarFile.getJarEntry(fileName);
            InputStream inputStream = jarFile.getInputStream(jarEntry);
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(inputStream);
            Properties properties = model.getProperties();
            for (Dependency dependency : model.getDependencies()) {
                if ("org.apache.flink".equals(dependency.getGroupId())) {
                    // 获取版本
                    Matcher m = Pattern.compile("\\$\\{(.*)}").matcher(dependency.getVersion());
                    String version = null;
                    if (m.find()) {
                        String propertyKey = m.group(1);
                        version = properties.getProperty(propertyKey);
                    } else {
                        version = dependency.getVersion();
                    }
                    // 校验是否一致
                    if (!version.equals("1.14.3")) {
                        versionVerifyFlag.set(false);
                        break;
                    }
                }
            }
            if (!versionVerifyFlag.get()) {
                break;
            }
        }

        System.out.println();


        JarFile jarFile1 = new JarFile("D:\\idea_work\\bigdata-flink-udf\\target\\bigdata-flink-udf.jar");
        //JarFile jarFile = new JarFile("D:\\idea_work\\bigdata-flink-dstream\\target\\bigdata-flink-dstream-1.0-SNAPSHOT.jar");
        JarEntry jarEntry = jarFile.getJarEntry("META-INF/maven/com.chitu.bigdata.flink.udf/bigdata-flink-udf/pom.xml");
        InputStream inputStream = jarFile.getInputStream(jarEntry);
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(inputStream);
        Properties properties = model.getProperties();
        AtomicBoolean flag1 = new AtomicBoolean(true);
        for (Dependency dependency : model.getDependencies()) {
            if ("org.apache.flink".equals(dependency.getGroupId())) {
                // 获取版本
                Matcher m = Pattern.compile("\\$\\{(.*)}").matcher(dependency.getVersion());
                String version = null;
                if (m.find()) {
                    String propertyKey = m.group(1);
                    version = properties.getProperty(propertyKey);
                } else {
                    version = dependency.getVersion();
                }
                // 校验是否一致
                if (!version.equals("1.15.2")) {
                    versionVerifyFlag.set(false);
                    break;
                }
            }
        }

        String ss = "${hive.version}";

        // 创建 Pattern 对象
        Pattern r = Pattern.compile("\\$\\{(.+?)\\}");

        r = Pattern.compile("\\$\\{(.*)}");

        // 现在创建 matcher 对象
        Matcher m = r.matcher(ss);

        if (m.find()) {
            System.out.println("Found value: " + m.group(0));
            System.out.println("Found value: " + m.group(1));
        }

        String s = ReUtil.get("\\$\\{(.+?)\\}", ss, 1);
        System.out.println(IOUtils.toString(inputStream, "utf-8"));
        inputStream.close();

        System.out.println();


    }
}


class FileName {
    private String name;
    private String size;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

}
