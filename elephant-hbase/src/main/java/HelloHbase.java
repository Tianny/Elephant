import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
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

    /**
     * 检查 mytable 表是否存在，如果存在就删掉旧表重新建立
     * @param admin
     * @param table
     * @throws IOException
     */
    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    /**
     * 建表
     * @param config
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void createSchemaTables(Configuration config) throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("mytable"));
            table.addFamily(new HColumnDescriptor("mycf").setCompressionType(Compression.Algorithm.NONE));
            System.out.println("Creating table.");
            // 新建表
            createOrOverwrite(admin, table);
            System.out.println("Done.");
        }
    }

    /**
     * 修改
     * @param config
     * @throws IOException
     */
    public static void modifySchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("mytable");

            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist");
                System.exit(-1);
            }

            // 往 mytable 里添加 newcf 列族
            HColumnDescriptor newColumn = new HColumnDescriptor("newcf");
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // 获取表定义
            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // 更新 mycf 列族
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            mycf.setCompactionCompressionType(Compression.Algorithm.GZ);
            mycf.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(mycf);
            admin.modifyTable(tableName, table);
        }
    }

    /**
     * 删表
     * @param config
     * @throws IOException
     */

    public static void deleteSchema(Configuration config) throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("mytable");

            // 停用 mytable
            admin.disableTable(tableName);

            // 删除 mycf 列族
            admin.deleteColumn(tableName, "mycf".getBytes("UTF-8"));

            // 删除 mytable 表
            admin.deleteTable(tableName);
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

        // 建表
        createSchemaTables(config);

        // 该表
        modifySchema(config);

        // 删表
        deleteSchema(config);
    }
}
