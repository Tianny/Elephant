import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * HDFS 文件系统操作工具类
 */

public class HDFSUtils {
    /*
    * 初始化 HDFS Configuration
    */
    public static Configuration initConfiguration(String confPath) {
        Configuration configuration = new Configuration();
//        System.out.println(confPath + File.separator + "core-site.xml");
        configuration.addResource(new Path(confPath + File.separator + "core-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "hdfs-site.xml"));
        return configuration;
    }

    /*
    * 向 HDFS 指定目录创建一个文件
    * @param fs HDFS 文件系统
    * @param dst 目标文件路径
    * @param contents 文件内容
    */
    public static void createFile(FileSystem fs, String dst, String contents) {
        try {
            Path path = new Path(dst);
            FSDataOutputStream fsDataOutputStream = fs.create(path);
            fsDataOutputStream.write(contents.getBytes());
            fsDataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    * 上传文件至 HDFS
    * @param fs HDFS 文件系统
    * @param src 源文件路径
    * @param dst 目标文件路径
    * */
    public static void uploadFile(FileSystem fs, String src, String dst) {
        try {
            Path srcPath = new Path(src); // 原路径
            Path dstPath = new Path(dst); // 目标路径
            fs.copyFromLocalFile(false, srcPath, dstPath);
            // 打印文件路径
            System.out.println("----list files-----");
            FileStatus[] fileStatuses = fs.listStatus(dstPath);
            for (FileStatus file : fileStatuses) {
                System.out.println(file.getPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    * 文件重命名
    * @param fs
    * @param oldName
    * @param newName
     */

    public static void rename(FileSystem fs, String oldName, String newName) {
        try {
            Path oldPath = new Path(oldName);
            Path newPath = new Path(newName);
            boolean isOk = fs.rename(oldPath, newPath);
            if (isOk) {
                System.out.print("rename ok");
            } else {
                System.out.print("rename failure");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    * 删除文件
     */

    public static void delete(FileSystem fs, String filePath) {
        try {
            Path path = new Path(filePath);
            boolean isOk = fs.deleteOnExit(path);
            if (isOk) {
                System.out.println("delete ok");
            } else {
                System.out.print("delete failure");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    创建 HDFS 目录
     */
    public static void mkdir(FileSystem fs, String path) {
        try {
            Path srcPath = new Path(path);
            if (fs.exists(srcPath)) {
                System.out.println("目录已存在");
                return;
            }

            boolean isOk = fs.mkdirs(srcPath);
            if (isOk) {
                System.out.println("create dir ok");
            } else {
                System.out.print("create dir failure");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    读取 HDFS 文件
     */
    public static void readFile(FileSystem fs, String filePath) {
        try {
            Path srcPath = new Path(filePath);
            InputStream in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); // 复制输出到标准输出
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
