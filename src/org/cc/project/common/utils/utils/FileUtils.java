package org.cc.project.common.utils.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Hobbes on 26/07/17.
 */
public class FileUtils {

    public static boolean createJobOutputPath(Job job, String path, boolean overwrite) {
        return createJobOutputPath(job, new Path(path), overwrite);
    }

    public static boolean createJobOutputPath(Job job, Path path, boolean overwrite) {
        if (null == job || null == path) {
            throw new IllegalArgumentException("Invalid args");
        }
        try {
            if (overwrite) {
                FileSystem fs = FileSystem.get(job.getConfiguration());
                if (fs.exists(path)) {
                    fs.delete(path, true);
                }
            }
            FileOutputFormat.setOutputPath(job, path);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static int getAsciiValue(String str) {
        int asciiVal = 0;
        if (StringUtils.isBlank(str)) {
            return 0;
        }
        char[] cArr = str.toCharArray();
        for (int i = 0; i < cArr.length; i++) {
            asciiVal += cArr[i];
        }
        return asciiVal;
    }

    public static void main(String[] s) throws Exception {
        String first = "ABE,DEF,302";
        String second = "ACF,DHF,02";
        System.out.println(Math.abs(first.split(",")[0].hashCode()) * 127 % 3);
        System.out.println(Math.abs(second.split(",")[0].hashCode()) * 127 % 3);
        System.out.println(Math.abs(first.split(",")[0].hashCode()/10000) * 3 % 4);
        System.out.println(Math.abs(second.split(",")[0].hashCode()/10000) * 3 % 4);

        System.out.println(1998 * 7 % 4);
        System.out.println(1999 * 7 % 4);

        System.out.println(getAsciiValue(first.split(",")[0])*127);
        System.out.println(getAsciiValue(second.split(",")[0])*127);


    }

}
