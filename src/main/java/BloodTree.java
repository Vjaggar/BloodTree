

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BloodTree {

    public static void main(String[] args) {

        // 接收输入的文件
        // 去掉文件中"--"后面的内容
        // 将文件按照";"切割成块
        // 循环这些文件,将每块的第一个表和后面的所有表(根据提供的"库名."匹配),形成一一对应关系
        // 将这些一一对应的关系装载到MySQL表中
        // 去除掉子表为空的数据
        // mvn install:install-file -Dfile=mysql-connector-java-5.1.39-bin.jar -DgroupId=org.oracle.mysql -DartifactId=oracle -Dversion=5.1.39 -Dpackaging=jar
//        create table bloodtree_in(tab VARCHAR(200),section int,father VARCHAR(200),son VARCHAR(200)) default charset=utf8 default collate=utf8_bin;
//        create table bloodtree_out(tab VARCHAR(200),father VARCHAR(200),son VARCHAR(200));

//        getTable("F:\\test\\xx.hql","MID");


        getAllTable("F:\\test\\xx.hql");




    }



    public static List<String> getAllTable(String sourceFile) {
        findTable(sourceFile);
        List<String> tables = select("SELECT TAB FROM(\n" +
                "SELECT father AS TAB FROM test.bloodtree_in \n" +
                "UNION ALL\n" +
                "SELECT SON AS TAB FROM test.bloodtree_in\n" +
                ") X\n" +
                "WHERE TAB<>''\n" +
                "GROUP BY TAB;");
        return tables;
    }





    public static List<String> getTable(String sourceFile,String flag) {
        List<String> tables = new ArrayList<String>();

        findTable(sourceFile);

        if (flag.equals("SOURCE")) {

            tables = select("SELECT a.father,substring_index(father,'.',1) as db,substring_index(father,'.',-1) as tab from\n" +
                    "(SELECT father FROM TEST.bloodtree_out group by father) a\n" +
                    "LEFT JOIN\n" +
                    "(SELECT son FROM TEST.bloodtree_out group by son) b\n" +
                    "on a.father=b.son\n" +
                    "where b.son is null\n" +
                    "order by a.father\n" +
                    ";");

        } else if (flag.equals("RESULT")) {
            tables = select("SELECT a.son,substring_index(father,'.',1) as db,substring_index(father,'.',-1) as tab from\n" +
                    "(SELECT son FROM TEST.bloodtree_out group by son) a\n" +
                    "LEFT JOIN\n" +
                    "(SELECT father FROM TEST.bloodtree_out group by father) b\n" +
                    "on a.son=b.father\n" +
                    "where b.father is null\n" +
                    "order by a.son\n" +
                    ";");

        } else  if (flag.equals("MID")) {
            tables = select("SELECT tmp,db,tab FROM(\n" +
                    "SELECT a.father as tmp,substring_index(father,'.',1) as db,substring_index(father,'.',-1) as tab from\n" +
                    "(SELECT father FROM TEST.bloodtree_out group by father) a\n" +
                    "LEFT JOIN\n" +
                    "(SELECT son FROM TEST.bloodtree_out group by son) b\n" +
                    "on a.father=b.son\n" +
                    "where b.son is not null\n" +
                    "UNION\n" +
                    "SELECT a.son as tmp,substring_index(father,'.',1) as db,substring_index(father,'.',-1) as tab from\n" +
                    "(SELECT son FROM TEST.bloodtree_out group by son) a\n" +
                    "LEFT JOIN\n" +
                    "(SELECT father FROM TEST.bloodtree_out group by father) b\n" +
                    "on a.son=b.father\n" +
                    "where b.father is not null\n" +
                    ") x\n" +
                    "order by tmp,db,tab\n" +
                    ";");

        }

        return tables;
    }

    public static void findTable(String sourceFile) {
        BufferedReader br = null;
        String temp;
        String son = null;
        int i = 0;
        int y = 1;

        executeSql("DELETE FROM TEST.bloodtree_in WHERE tab='9527';");
        try {
            // 指定生成的文件为UTF-8格式
            br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "utf-8"));
            temp = br.readLine();
            while (temp != null) {
                String splitTemp = temp;
                String match;
                String cutTmp = temp;
                String[] strArray = cutTmp.split("--");

//                System.out.println("strArray:" + Arrays.toString(strArray));
                if (strArray.length != 0) {
                    splitTemp = strArray[0];
//                    System.out.println("分割后的:" + splitTemp);
                }
                // 根据已知库名正则匹配查找表
                Pattern p = Pattern.compile("(BAS|BDA_GER_TMP|BDSP_COMM|BWT|CFG|CS_ETE|CUST_VIEW|DEFAULT|" +
                        "INF|INF10000|INF31W|INF3BSN|INF3CHN|INF3CPC|INF3CRM|INF3DAP|INF3OTH|INF3RM|INF3SETT|" +
                        "INF3SPS|INF3TPSS|INF3WG|INF_BSP|INF_BSS|INF_MSS|INF_NET|INF_OSS|INFDAP|INFEDC|INT|INT3ASS|" +
                        "INT3LAB|INT3MID|INT_NET|LAB|LSQ_HS|LXY_HS|TEST|TMP|TST|USER_BO|USER_DTD|USER_KHTYXT|USER_MRXT|" +
                        "USER_SJ|USER_SJGL|USER_SJJS|USER_WGYX|USER_ZHCW|USER_ZHJSXT|USER_ZHRL|USER_ZJJH|WID|ZNYX_APPED)\\.\\S+",Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(splitTemp);
                while (m.find()) {
                    match = m.group().replace(";", "");
                    if (match != null) {
//                        System.out.println("match: " + i + " " + match);
                        if (i == 0) {
                            son = match;
                            match = "";
                        }

                        executeSql("INSERT INTO TEST.bloodtree_in(tab,section,father,son) " +
                                "values('9527'," + y + ",'" + match + "','" + son + "');");
//                      System.out.println("INSERT INTO TEST.bloodtree_in(tab,section,father,son)
//                               values('9527'," + y + ",'" + fatherTable + "','" + match + "');");
                    }
                    i++;
                }

                // 判断当前行是否存在有效的";"
                Pattern p1 = Pattern.compile(";");
                Matcher m1 = p1.matcher(splitTemp);
                while (m1.find()) {
                    match = m1.group();
                    if (match != null) {
//                        System.out.println("match: " + i + " " + match);
//                        System.out.println("第" + y + "段结束: " + splitTemp);
                        i = 0;
                        y++;
                    }
                }


                temp = br.readLine();
            }
        } catch (IOException e) {
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        executeSql("DELETE FROM TEST.bloodtree_out WHERE tab='9527';");
        executeSql("INSERT INTO TEST.bloodtree_out(tab,father,son) SELECT tab,father,son FROM( SELECT upper(tab) AS tab,upper(father) AS father,upper(son) AS son FROM TEST.bloodtree_in WHERE father<>'') x GROUP BY tab,father,son ORDER BY tab,son,father;");
    }


    // 数据库连接
    public static Connection connectionMysql() {
        String user = "test";
        String pass = "test";
        String driver = "com.mysql.jdbc.Driver";// 驱动程序类名
        String url = "jdbc:mysql://localhost:3306/test?" // 数据库URL
                + "useUnicode=true&characterEncoding=UTF8&useSSL=false";// 防止乱码
        Connection conn = null;
        // 数据库连接
        try {
            Class.forName(driver);// 注册(加载)驱动程序
            conn = DriverManager.getConnection(url, user, pass);// 获取数据库连接
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    // 更新操作
    public static void executeSql(String sql) {
        Connection nn = connectionMysql();
        // 执行操作
        try {
            // 插入数据的sql语句
            Statement stmt1 = nn.createStatement();    // 创建用于执行静态sql语句的Statement对象
            stmt1.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数
            nn.close();   //关闭数据库连接
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 释放数据库连接
        try {
            if (nn != null)
                nn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 查询操作
    public static List<String> select(String sql) {
        Connection nn = connectionMysql();
        List<String> tables = new ArrayList<String>();

// 执行操作
        try {
            // 插入数据的sql语句

            Statement stmt1 = nn.createStatement();    // 创建用于执行静态sql语句的Statement对象
            ResultSet rs = stmt1.executeQuery(sql);
//            stmt1.executeQuery(sql);
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索
                tables.add(rs.getString(1));
                // 输出数据
//                System.out.print(rs.getString(1));
//                System.out.print("\n");
            }
            nn.close();   //关闭数据库连接
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 释放数据库连接
        try {
            if (nn != null)
                nn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return tables;
    }
}



