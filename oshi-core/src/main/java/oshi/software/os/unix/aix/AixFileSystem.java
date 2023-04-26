/*
 * Copyright 2020-2022 The OSHI Project Contributors
 * SPDX-License-Identifier: MIT
 */
package oshi.software.os.unix.aix;

import java.io.File;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import oshi.annotation.concurrent.ThreadSafe;
import oshi.software.common.AbstractFileSystem;
import oshi.software.os.OSFileStore;
import oshi.util.ExecutingCommand;
import oshi.util.FileSystemUtil;
import oshi.util.FileUtil;
import oshi.util.ParseUtil;

/**
 * The AIX File System contains {@link oshi.software.os.OSFileStore}s which are a storage pool, device, partition,
 * volume, concrete file system or other implementation specific means of file storage.
 */
@ThreadSafe
public class AixFileSystem extends AbstractFileSystem {

    public static final String OSHI_AIX_FS_PATH_EXCLUDES = "oshi.os.aix.filesystem.path.excludes";
    public static final String OSHI_AIX_FS_PATH_INCLUDES = "oshi.os.aix.filesystem.path.includes";
    public static final String OSHI_AIX_FS_VOLUME_EXCLUDES = "oshi.os.aix.filesystem.volume.excludes";
    public static final String OSHI_AIX_FS_VOLUME_INCLUDES = "oshi.os.aix.filesystem.volume.includes";

    private static final List<PathMatcher> FS_PATH_EXCLUDES = FileSystemUtil
            .loadAndParseFileSystemConfig(OSHI_AIX_FS_PATH_EXCLUDES);
    private static final List<PathMatcher> FS_PATH_INCLUDES = FileSystemUtil
            .loadAndParseFileSystemConfig(OSHI_AIX_FS_PATH_INCLUDES);
    private static final List<PathMatcher> FS_VOLUME_EXCLUDES = FileSystemUtil
            .loadAndParseFileSystemConfig(OSHI_AIX_FS_VOLUME_EXCLUDES);
    private static final List<PathMatcher> FS_VOLUME_INCLUDES = FileSystemUtil
            .loadAndParseFileSystemConfig(OSHI_AIX_FS_VOLUME_INCLUDES);

    @Override
    public List<OSFileStore> getFileStores(boolean localOnly) {
        return getFileStoreMatching(null, localOnly);
    }

    static final Pattern FS_PATTERN = Pattern.compile("^(?:[\\w\\.]+:)?\\/");

    // Called by AixOSFileStore
    static List<OSFileStore> getFileStoreMatching(String nameToMatch) {
        return getFileStoreMatching(nameToMatch, false);
    }

    private static List<OSFileStore> getFileStoreMatching(String nameToMatch, boolean localOnly) {
        List<OSFileStore> fsList = new ArrayList<>();

        // Get inode usage data
        Map<String, Long> inodeFreeMap = new HashMap<>();
        Map<String, Long> inodeTotalMap = new HashMap<>();
        // jna doesn't provide LibC, so we go for shell commands...
        String command = "df -F %l %n" + (localOnly ? " -T local" : "");
        List<String> dfResult = ExecutingCommand.runNative(command);

        for (String line : ExecutingCommand.runNative(command)) {
            /*- Sample Output:
             root@sovma473:/$ df -F %l %n -T local
            Filesystem    512-blocks     Iused    Ifree
            /dev/hd4         1441792    18137    58071
            /dev/hd2         5636096    44046   102455
            /dev/hd9var      1179648     9919    28280
            /dev/hd3          393216      179    39126
            /dev/hd1          131072       62     1003
            /dev/hd11admin     262144        7    29131
            /dev/hd10opt      917504      412    89642
            /dev/livedump     524288        4    58200
            /dev/resgrp473lv   83886080    28800  8358964

            root@sovma473:/$ df -F %l %n
            Filesystem    512-blocks     Iused    Ifree
            /dev/hd4         1441792    18137    58069
            /dev/hd2         5636096    44046   102455
            /dev/hd9var      1179648     9919    28280
            /dev/hd3          393216      179    39126
            /dev/hd1          131072       62     1003
            /dev/hd11admin     262144        7    29131
            /proc                  -        -        -
            /dev/hd10opt      917504      412    89642
            /dev/livedump     524288        4    58200
            /dev/resgrp473lv   83886080    28799  8358449
            192.168.253.80:/usr/sys/inst.images/toolbox_20110809   461373440    84670  5662375
            192.168.253.80:/usr/sys/inst.images/toolbox_20131113   461373440    84670  5662375
            192.168.253.80:/usr/sys/inst.images/toolbox_20151220   461373440    84670  5662375
            192.168.253.80:/usr/sys/inst.images/mozilla_3513   461373440    84670  5662375
             */
            if (FS_PATTERN.matcher(line).find()) {
                String[] split = ParseUtil.whitespaces.split(line);
                if (split.length >= 3) {
                    // get the last 2 columns, that's where we told df to provide used and free values
                    long used = ParseUtil.parseLongOrDefault(split[split.length - 2], 0L);
                    long free = ParseUtil.parseLongOrDefault(split[split.length - 1], 0L);
                    inodeTotalMap.put(split[0], used + free);
                    inodeFreeMap.put(split[0], free);
                }
            }
        }

        // Get mount table
        for (String fs : ExecutingCommand.runNative("mount")) { // NOSONAR squid:S135
            /*- Sample Output:
             *   node       mounted        mounted over    vfs       date        options
            * -------- ---------------  ---------------  ------ ------------ ---------------
            *          /dev/hd4         /                jfs2   Jun 16 09:12 rw,log=/dev/hd8
            *          /dev/hd2         /usr             jfs2   Jun 16 09:12 rw,log=/dev/hd8
            *          /dev/hd9var      /var             jfs2   Jun 16 09:12 rw,log=/dev/hd8
            *          /dev/hd3         /tmp             jfs2   Jun 16 09:12 rw,log=/dev/hd8
            *          /dev/hd11admin   /admin           jfs2   Jun 16 09:13 rw,log=/dev/hd8
            *          /proc            /proc            procfs Jun 16 09:13 rw
            *          /dev/hd10opt     /opt             jfs2   Jun 16 09:13 rw,log=/dev/hd8
            *          /dev/livedump    /var/adm/ras/livedump jfs2   Jun 16 09:13 rw,log=/dev/hd8
            * foo      /dev/fslv00      /home            jfs2   Jun 16 09:13 rw,log=/dev/loglv00
             */
            // Lines begin with optional node, which we don't use. To force sensible split
            // behavior, append any character at the beginning of the string
            String[] split = ParseUtil.whitespaces.split("x" + fs);
            if (split.length > 7) {
                // 1st field is volume name [0-index]
                // 2nd field is mount point
                // 3rd field is fs type
                // 4th-6th fields are date, ignored
                // 7th field is options
                String volume = split[1];
                String path = split[2];
                String type = split[3];
                String options = split[4];

                // Skip non-local drives if requested, and exclude pseudo file systems
                if ((localOnly && NETWORK_FS_TYPES.contains(type)) || !path.equals("/")
                        && (PSEUDO_FS_TYPES.contains(type) || FileSystemUtil.isFileStoreExcluded(path, volume,
                                FS_PATH_INCLUDES, FS_PATH_EXCLUDES, FS_VOLUME_INCLUDES, FS_VOLUME_EXCLUDES))) {
                    continue;
                }

                String name = path.substring(path.lastIndexOf('/') + 1);
                // Special case for /, pull last element of volume instead
                if (name.isEmpty()) {
                    name = volume.substring(volume.lastIndexOf('/') + 1);
                }

                if (nameToMatch != null && !nameToMatch.equals(name)) {
                    continue;
                }
                File f = new File(path);
                if (!f.exists() || f.getTotalSpace() < 0) {
                    continue;
                }
                long totalSpace = f.getTotalSpace();
                long usableSpace = f.getUsableSpace();
                long freeSpace = f.getFreeSpace();

                String description;
                if (volume.startsWith("/dev") || path.equals("/")) {
                    description = "Local Disk";
                } else if (volume.equals("tmpfs")) {
                    description = "Ram Disk";
                } else if (NETWORK_FS_TYPES.contains(type)) {
                    description = "Network Disk";
                } else {
                    description = "Mount Point";
                }

                fsList.add(new AixOSFileStore(name, volume, name, path, options, "", "", description, type, freeSpace,
                        usableSpace, totalSpace, inodeFreeMap.getOrDefault(volume, 0L),
                        inodeTotalMap.getOrDefault(volume, 0L)));
            }
        }
        return fsList;
    }

    @Override
    public long getOpenFileDescriptors() {
        boolean header = false;
        long openfiles = 0L;
        for (String f : ExecutingCommand.runNative("lsof -nl")) {
            if (!header) {
                header = f.startsWith("COMMAND");
            } else {
                openfiles++;
            }
        }
        return openfiles;
    }

    @Override
    public long getMaxFileDescriptors() {
        return ParseUtil.parseLongOrDefault(ExecutingCommand.getFirstAnswer("ulimit -n"), 0L);
    }

    @Override
    public long getMaxFileDescriptorsPerProcess() {
        final List<String> lines = FileUtil.readFile("/etc/security/limits");
        for (final String line : lines) {
            if (line.trim().startsWith("nofiles")) {
                return ParseUtil.parseLastLong(line, Long.MAX_VALUE);
            }
        }
        return Long.MAX_VALUE;
    }
}
