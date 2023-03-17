

package com.chitu.bigdata.sdp.test;

import java.io.*;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * TODO 类功能描述
 *
 * @author chenyun
 * @version 1.0
 * @date 2022/7/2 20:14
 */
public class JarFileReader {

    private JarInputStream jarInput;

    private HashMap<String, ByteArrayOutputStream> entriesStreamMap;

    public JarFileReader(InputStream in) throws IOException {
        jarInput = new JarInputStream(in);
        entriesStreamMap = new HashMap<String, ByteArrayOutputStream>();
    }

    public void readEntries() throws IOException {
        JarEntry entry = jarInput.getNextJarEntry();
        String manifestEntry = null;
        while (entry != null) {
            System.out.println("Entry Name = " + entry.getName());
            manifestEntry = entry.getName();
            copyInputStream(jarInput, entry.getName());
            entry = jarInput.getNextJarEntry();
        }
        System.out.println("Now!! + get jar entry inputstream again...");
        InputStream inputStream = getCopy(manifestEntry);
        System.out.println(inputStream.read());
    }

    public void copyInputStream(InputStream in, String entryName) throws IOException {
        if (!entriesStreamMap.containsKey(entryName)) {
            ByteArrayOutputStream _copy = new ByteArrayOutputStream();
            int read = 0;
            int chunk = 0;
            byte[] data = new byte[256];
            while (-1 != (chunk = in.read(data))) {
                read += data.length;
                _copy.write(data, 0, chunk);
            }
            entriesStreamMap.put(entryName, _copy);
        }
    }

    public InputStream getCopy(String entryName) {
        ByteArrayOutputStream _copy = entriesStreamMap.get(entryName);
        return (InputStream) new ByteArrayInputStream(_copy.toByteArray());
    }

    public static void abc(String jarPath) {
        File jarFile = new File(jarPath);
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(jarFile));
            JarFileReader reader = new JarFileReader(in);
            reader.readEntries();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
