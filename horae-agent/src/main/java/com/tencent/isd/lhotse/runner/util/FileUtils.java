package com.tencent.isd.lhotse.runner.util;

import org.apache.commons.lang.StringUtils;


public class FileUtils {
    public static String replaceHomePath(String path) {
        if (StringUtils.isBlank(path))
            return path;

        if (path.contains("~")) {
            String usrHome = System.getProperty("user.home");
            return StringUtils.replace(path, "~", usrHome);
        }
        else
            return path;
    }


    public static void main(String[] args) {
        System.out.println(replaceHomePath("~/data/good/"));
    }
}
