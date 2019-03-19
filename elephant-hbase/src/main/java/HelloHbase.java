import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;

public class HelloHbase {

    /*
     * 初始化 Kerberos 认证
     */
    public static void initKerberosEnv(Configuration conf) {
        System.setProperty("java.security.krb5.conf", HelloHbase.class.getClassLoader().getResource("krb5.conf").getPath());
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hbase/kt1@DEV.DXY.CN", HelloHbase.class.getClassLoader().getResource("hbase.keytab").getPath());
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        // 获取配置文件
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        // Kerberos 认证
        initKerberosEnv(config);


        //创建连接
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            // 定义表名
            TableName tableName = TableName.valueOf("mytable");

            // 定义表
            HTableDescriptor table = new HTableDescriptor(tableName);

            // 定义列族
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            table.addFamily(new HColumnDescriptor(mycf));

            // 执行创建动作
            admin.createTable(table);
        }
    }
}
