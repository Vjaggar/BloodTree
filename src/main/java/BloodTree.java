import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.*;

public class BloodTree {

    public static void main(String[] args) {

        // 接收输入的文件
        // 去掉文件中"--"后面的内容
        // 将文件按照";"切割成块
        // 循环这些文件,将每块的第一个表和后面的所有表(根据提供的"库名."匹配),形成一一对应关系
        // 将这些一一对应的关系装载到MySQL表中
        // 去除掉子表为空的数据


        String sourceFile = "";
        String resultFile = "";

        if (args.length == 2) {
            sourceFile = args[0];
            resultFile = args[1];

        } else {
            System.err.println("参数不完整");
            System.exit(2);
        }
        findTable(sourceFile, resultFile);
    }

    public static void findTable(String sourceFile, String resultFile) {
        BufferedReader br = null;
        String temp;
        String fatherTable = null;
        int i = 0;

        insert("DELETE FROM TEST.bloodtree WHERE ID=9527;");
        try {
            // 指定生成的文件为UTF-8格式
            br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "utf-8"));
            temp = br.readLine();
            while (temp != null) {
                String splitTemp = temp;
                String match ;
                String cutTmp = temp;
                String[] strArray = cutTmp.split("--");

//                System.out.println("strArray:" + Arrays.toString(strArray));
                if (strArray.length != 0) {
                    splitTemp = strArray[0];
//                    System.out.println("分割后的:" + splitTemp);
                }
                // 根据已知库名正则匹配查找表
                Pattern p = Pattern.compile("(BAS|BDA_GER_TMP|BDSP_COMM|BWT|CFG|CS_ETE|CUST_VIEW|DEFAULT|INF|INF10000|INF31W|INF3BSN|INF3CHN|INF3CPC|INF3CRM|INF3DAP|INF3OTH|INF3RM|INF3SETT|INF3SPS|INF3TPSS|INF3WG|INF_BSP|INF_BSS|INF_MSS|INF_NET|INF_OSS|INFDAP|INFEDC|INT|INT3ASS|INT3LAB|INT3MID|INT_NET|LAB|LSQ_HS|LXY_HS|TEST|TMP|TST|USER_BO|USER_DTD|USER_KHTYXT|USER_MRXT|USER_SJ|USER_SJGL|USER_SJJS|USER_WGYX|USER_ZHCW|USER_ZHJSXT|USER_ZHRL|USER_ZJJH|WID|ZNYX_APPED).\\S+");
                Matcher m = p.matcher(splitTemp.toUpperCase());
                while (m.find()) {
                    match = m.group();
                    if (match != null) {
//                        System.out.println("match: " + i + " " + match);
                        if (i == 0) {
                            fatherTable = match;
                            match = "";
                        }
                        i++;
                        insert("INSERT INTO TEST.bloodtree(ID,father,son) values(9527,'" + fatherTable + "','" + match + "');");
                        System.out.println("INSERT INTO TEST.bloodtree(ID,father,son) values(9527,'" + fatherTable + "','" + match + "');");
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
    }


    // 更新操作
    public static void insert(String sql) {
        String user = "test";
        String pass = "test";
        String driver = "com.mysql.jdbc.Driver";// 驱动程序类名
        String url = "jdbc:mysql://localhost:3306/test?" // 数据库URL
                + "useUnicode=true&characterEncoding=UTF8";// 防止乱码
        Connection conn = null;
        // 数据库连接
        try {
            Class.forName(driver);// 注册(加载)驱动程序
            conn = DriverManager.getConnection(url, user, pass);// 获取数据库连接
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 执行操作
        try {
            // 插入数据的sql语句
            Statement stmt1 = conn.createStatement();    // 创建用于执行静态sql语句的Statement对象
            stmt1.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数
            conn.close();   //关闭数据库连接
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 释放数据库连接
        try {
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
