import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/*
 * 访问 Kerberos 环境下的 HDFS
 */

public class KBHDFSTest {
    private static String confPath = System.getProperty("user.dir") + File.separator + "elephant-hdfs" + File.separator + "conf";

    /*
    * 初始化 Kerberos 环境
     */
    public static void initKerberosEnv(Configuration conf) {
        System.setProperty("java.security.krb5.conf", confPath + File.separator + "krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdfs/kt1@DEV.DXY.CN", confPath + File.separator + "hdfs.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 初始化 HDFS Configuration 配置
        Configuration configuration = HDFSUtils.initConfiguration(confPath);
        initKerberosEnv(configuration);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            HDFSUtils.mkdir(fileSystem, "/test");
            HDFSUtils.uploadFile(fileSystem, confPath + File.separator + "krb5.conf", "/test");
            HDFSUtils.rename(fileSystem, "/test/krb5.conf", "/test/krb51.conf");
            HDFSUtils.readFile(fileSystem, "/test/krb51.conf");
            HDFSUtils.delete(fileSystem, "/test/krb51.conf");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
