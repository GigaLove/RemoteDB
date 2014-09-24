using System;
using System.Collections.Generic;
using System.Linq;

using MySql.Data;
using MySql.Data.MySqlClient;

using System.Web;
using System.Web.Services;
using System.IO;

namespace RemoteDB
{
    /// <summary>
    /// webRemoteService 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消对下行的注释。
    // [System.Web.Script.Services.ScriptService]
    public class webRemoteService : System.Web.Services.WebService
    {
        //恢复数据库
        private bool ReDataBase(List<string> dStr, List<string> mStr, List<string> bStr,MySqlConnection con)
        {
            //建立第一个事务
            MySqlTransaction tran = null;
            MySqlCommand com = new MySqlCommand();
            tran = con.BeginTransaction();
            com.Connection = con;
            com.Transaction = tran;

            //开始进行数据的插入
            //日表插入
            if(dStr != null)
            {
                foreach (string temp in dStr)
                {
                    if (temp == null)
                        continue;
                    //再次分割
                    string[] tempStr = temp.Split('*');
                    //
                    if (tempStr.Count() < 5)
                        continue;

                    string sql = null;
                    if (tempStr[7].Equals("0"))
                        sql = "INSERT INTO DAY VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] + "')";
                    else
                        sql = "UPDATE DAY SET LUG_NUM = '" + tempStr[2] + "',MONEY = '" + tempStr[3] + "',GA_TIME = '" + tempStr[5] + "',UL_TIME = '" + tempStr[6] + "' where DAY = '" + tempStr[0] + "'and PLACE = '" + tempStr[1] + "' and PERSON = '" + tempStr[4] + "'";
                    //
                    try
                    {
                        com.CommandText = sql;
                        com.ExecuteNonQuery();
                    }
                    catch
                    {
                        tran.Rollback();
                        return false;
                    }
                }
            }
            //月表插入
            if(mStr != null)
            {
                foreach (string temp in mStr)
                {
                    if (temp == null)
                        continue;
                    //再次分割
                    string[] tempStr = temp.Split('*');
                    //
                    if (tempStr.Count() < 5)
                        continue;
                    String sql = null;

                    if (tempStr[9].Equals("0"))
                        sql = "INSERT INTO MONTH VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] + "','" + tempStr[7] + "','" + tempStr[8] + "')";
                    else
                        sql = "UPDATE MONTH SET ST_DATE = '" + tempStr[1] + "',END_DATE = '" + tempStr[2] + "',LUG_NUM = '" + tempStr[4] + "',MONEY = '" + tempStr[5] + "',GA_TIME = '" + tempStr[7] + "',UL_TIME = '" + tempStr[8] + "' where MONTH = '" + tempStr[0] + "'and PLACE = '" + tempStr[3] + "' and PERSON = '" + tempStr[6] + "'";

                    try
                    {
                        //创建一个事务
                        com.CommandText = sql;
                        com.ExecuteNonQuery();
                    }
                    catch
                    {
                        tran.Rollback();
                        return false;
                    }
                }
            }
            //插入流水
            if (bStr != null)
            {
                foreach (string temp in bStr)
                {
                    if (temp == null)
                        continue;
                    //再次分割
                    string[] tempStr = temp.Split('*');
                    //
                    if (tempStr.Count() < 20)
                        continue;

                    string sql = null;
                    //
                    if (tempStr[20].Equals("0"))
                        sql = "INSERT INTO BILL VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] + "','" + tempStr[7] + "','" + tempStr[8]
                            + "','" + tempStr[9] + "','" + tempStr[10] + "','" + tempStr[11] + "','" + tempStr[12] + "','" + tempStr[13] + "','" + tempStr[14] + "','" + tempStr[15] + "','" + tempStr[16] + "','" + tempStr[17] + "','" + tempStr[18] + "','" + tempStr[19] + "')";
                    else
                        sql = "UPDATE BILL SET STATE = '" + tempStr[2] + "', GV_STAFF = '" + tempStr[10] + "', REV_PERSON = '" + tempStr[11] + "',END_DATE = '" + tempStr[13] + "', PAY = '" + tempStr[15] + "', UPDATE_TIME = '" + tempStr[17]  + "' where  LUG_NUMBER = '" + tempStr[0] + "';";

                    try
                    {
                        com.CommandText = sql;
                        com.ExecuteNonQuery();
                    }
                    catch
                    {
                        tran.Rollback();
                        return false;
                    }
                }
            }
            //提交事务
            tran.Commit();
            return true;
        }

        //写数据库
        private int WriteDataBase(string[] dStr, string[] mStr, string[] bStr,MySqlConnection con)
        {
            //建立一个事务
            MySqlTransaction tran = null;
            MySqlCommand com = new MySqlCommand();
            tran = con.BeginTransaction();
            com.Connection = con;
            com.Transaction =  tran;

            //开始进行数据的插入
            //日表插入
            foreach (string temp in dStr)
            {
                if (temp == null)
                    continue;
                //再次分割
                string[] tempStr = temp.Split('*');
                //对于不正确的数据格式 不进行操作
                if (tempStr.Count() < 5)
                    continue;
                
                //判断操作种类
                string sql = null;
                if (tempStr[7].Equals("0"))
                    sql = "INSERT INTO DAY VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] +"')";
                else
                    sql = "UPDATE DAY SET LUG_NUM = '" + tempStr[2] + "',MONEY = '" + tempStr[3] + "',GA_TIME = '" + tempStr[5] + "',UL_TIME = '" + tempStr[6]  + "' where DAY = '" + tempStr[0] + "'and PLACE = '" + tempStr[1] + "' and PERSON = '" + tempStr[4] + "'";

                //执行数据库操作
                try
                {
                    com.CommandText = sql;
                    com.ExecuteNonQuery();
                }
                catch
                {
                    tran.Rollback();
                    return -2;
                }
            }
            //月表插入
            foreach (string temp in mStr)
            {
                if (temp == null)
                    continue;
                //再次分割
                string[] tempStr = temp.Split('*');
                //对于不正确的数据格式 不进行操作
                if (tempStr.Count() < 5)
                    continue;

                String sql = null;
                if (tempStr[9].Equals("0"))
                    sql = "INSERT INTO MONTH VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] + "','" + tempStr[7] + "','" + tempStr[8]  + "')";
                else
                    sql = "UPDATE MONTH SET ST_DATE = '" + tempStr[1] + "',END_DATE = '" + tempStr[2] + "',LUG_NUM = '" + tempStr[4] + "',MONEY = '" + tempStr[5] + "',GA_TIME = '" + tempStr[7] + "',UL_TIME = '" + tempStr[8] + "' where MONTH = '" + tempStr[0] + "'and PLACE = '" + tempStr[3] + "' and PERSON = '" + tempStr[6] + "'";

                try
                {
                    com.CommandText = sql;
                    com.ExecuteNonQuery();
                }
                catch
                {
                    tran.Rollback();
                    return -2;//事务一失败
                }
            }
            //插入流水
            foreach (string temp in bStr)
            {
                if (temp == null)
                    continue;
                //再次分割
                string[] tempStr = temp.Split('*');

                //对于不正确的数据格式 不进行操作
                if (tempStr.Count() < 18)
                    continue;

                string sql = null;
                if (tempStr[20].Equals("0"))
                    sql = "INSERT INTO BILL VALUES ('" + tempStr[0] + "','" + tempStr[1] + "','" + tempStr[2] + "','" + tempStr[3] + "','" + tempStr[4] + "','" + tempStr[5] + "','" + tempStr[6] + "','" + tempStr[7] + "','" + tempStr[8]
                        + "','" + tempStr[9] + "','" + tempStr[10] + "','" + tempStr[11] + "','" + tempStr[12] + "','" + tempStr[13] + "','" + tempStr[14] + "','" + tempStr[15] + "','" + tempStr[16] + "','" + tempStr[17] + "','" + tempStr[18] + "','" + tempStr[19] + "')";
                else
                    sql = "UPDATE BILL SET STATE = '" + tempStr[2] + "', GV_STAFF = '" + tempStr[10] + "', REV_PERSON = '" + tempStr[11] + "',END_DATE = '" + tempStr[13] + "', PAY = '" + tempStr[15] + "', UPDATE_TIME = '" + tempStr[17] + "' where  LUG_NUMBER = '" + tempStr[0] + "';";

                try
                {
                    com.CommandText = sql;
                    com.ExecuteNonQuery();
                }
                catch
                {
                    tran.Rollback();
                    return -2;
                }
            }
            //提交事务
            tran.Commit();
            return 1;
        }
        //读备份文件 并且恢复 需要改进 对于有几十万条记录 分步执行
        private bool ReadFile(MySqlConnection con)
        {
            //判断文件夹是否存在
            DirectoryInfo dir = new DirectoryInfo(@"D:\\DataBaseBack");
            if (dir.Exists)
            {
                FileOperation fp = new FileOperation();

                //恢复日汇总表
                List<string> dList = new List<string>();
                //读入数据失败
                if(!fp.ReadFile("D:\\DataBaseBack\\dStrMsg.dat",ref dList))
                    return false;
                //恢复数据库失败
                if(!ReDataBase(dList, null, null, con))
                    return false;
                dList.Clear();

                //恢复月汇总表
                List<string> mList = new List<string>();
                if(!fp.ReadFile("D:\\DataBaseBack\\mStrMsg.dat",ref mList))
                    return false;

                if(!ReDataBase(null, mList, null, con))
                    return false;
                mList.Clear();
                
                //恢复流水
                List<string> bList = new List<string>();
                if(!fp.ReadFile("D:\\DataBaseBack\\bStrMsg.dat",ref bList))
                    return false;

                if(!ReDataBase(null, null, bList, con))
                    return false;
                bList.Clear();
            }
            return true;
        }
        //写备份文件
        private bool WriteFile(string[] dStr, string[] mStr, string[] bStr)
        {
            //判断文件夹是否存在
            DirectoryInfo dir = new DirectoryInfo(@"D:\\DataBaseBack");
            if(!dir.Exists)
                dir.Create();

            FileOperation fp = new FileOperation();
            //日报数据
            if (dStr != null)
            {
                if (!fp.WriteFile("D:\\DataBaseBack\\dStrMsg.dat", dStr))
                    return false;
            }
            //月报数据
            if (mStr != null)
            {
                if (!fp.WriteFile("D:\\DataBaseBack\\mStrMsg.dat", mStr))
                    return false;
            }
            //流水数据
            if (bStr != null)
            {
                if (!fp.WriteFile("D:\\DataBaseBack\\bStrMsg.dat", bStr))
                    return false;
            }

            return true;
        }
        [WebMethod]
        //初始化链接数据库
        private int InitDB(string user, string pawd,ref MySqlConnection con)
        {
            //查看是否存在数据库 存在则打开
            try
            {
                string info = "Data Source = 127.0.0.1;Initial Catalog = DATA;User ID = " + user + ";Password = " + pawd;
                con = new MySqlConnection(info);
                con.Open();
                return 1;
            }
            //没有数据库需要跑创建
            catch
            {
                string info = "Data Source = 127.0.0.1;User ID = " + user + ";Password = " + pawd;
                try
                {
                    //创建数据库
                    con = new MySqlConnection(info);
                    MySqlCommand cmd = new MySqlCommand("CREATE DATABASE DATA;", con);
                    con.Open();
                    cmd.ExecuteNonQuery();
                    con.Dispose();
                    //创建table 同时打开数据库
                    info = "Data Source = 127.0.0.1;Initial Catalog = DATA;User ID = " + user + ";Password = " + pawd;
                    con = new MySqlConnection(info);
                    con.Open();

                    string day = "CREATE TABLE DAY(DAY VARCHAR(30),PLACE VARCHAR(30),LUG_NUM VARCHAR(10),MONEY VARCHAR(10),PERSON VARCHAR(10),GA_TIME VARCHAR(30),UL_TIME VARCHAR(30),primary key(DAY,PLACE,PERSON))";
                    cmd = new MySqlCommand(day, con);
                    cmd.ExecuteNonQuery();

                    string mouth = "CREATE TABLE MONTH(MONTH VARCHAR(30),ST_DATE VARCHAR(30),END_DATE VARCHAR(30),PLACE VARCHAR(30),LUG_NUM VARCHAR(10),MONEY VARCHAR(10),PERSON VARCHAR(10),GA_TIME VARCHAR(30),UL_TIME VARCHAR(30),primary key(MONTH,PLACE,PERSON))";
                    cmd = new MySqlCommand(mouth, con);
                    cmd.ExecuteNonQuery();

                    string bill = "CREATE TABLE BILL(LUG_NUMBER varchar(30) Primary Key,ST_DATE varchar(30),STATE varchar(10),CH_PERSON varchar(100),DOCU_NUMBER varchar(100),PHONE_NUMBER varchar(50),LUG_COUNT varchar(10),CHECK_POINT varchar(50),DEPOSIT_PLACE varchar(50),ACP_STAFF varchar(100),GV_STAFF varchar(100),REV_PERSON varchar(200),PRE_DATE varchar(30),END_DATE varchar(30),PRY_PAY varchar(10),PAY varchar(10),IMPORTANT varchar(10),UPDATE_TIME varchar(50),LABEL varchar(100),MARK varchar(200))";
                    cmd = new MySqlCommand(bill, con);
                    cmd.ExecuteNonQuery();
                    //初始化 恢复数据库
                    if(!ReadFile(con))
                        return -1;
                    return 1;
                }
                //出现异常失败返回-1
                catch
                {
                    return -1;
                }
            }
        }
        //数据操作
        [WebMethod]
        public int updateDB(string user, string pawd, string dMsg, string mMsg, string bMsg)
        {
            MySqlConnection con = null;
            if(InitDB(user, pawd, ref con).Equals(-1))
                return -1;//数据库初始化失败
            //使用# 将数据分割开来
            string[] dStr = dMsg.Split('#');
            string[] mStr = mMsg.Split('#');
            string[] bStr = bMsg.Split('#');
            //写入数据库
            int State = WriteDataBase(dStr,mStr,bStr,con);
            con.Dispose();
            //判断写入状态
            if (State == 1)
                if(!WriteFile(dStr,mStr,bStr))
                    return -3;
            //返回执行状态
            return State;
        }
            
    }
}
