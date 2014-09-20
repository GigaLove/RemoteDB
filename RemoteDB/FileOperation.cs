using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.IO;
using System.Text;

namespace RemoteDB
{
    public class FileOperation
    {
        //进行原始数据的加密解密
        private string Converts(string Msg)
        {
            string NewMsg;
            byte[] bt = Encoding.Unicode.GetBytes(Msg);
            //进行加密 解密
            int count = 0;
            foreach (byte temp in bt)
            {
                bt[count] = (byte)(temp ^ 0x00000066);
                count++;
            }
            NewMsg = ASCIIEncoding.Unicode.GetString(bt);


            return NewMsg;
        }
        //写备份文件
        public bool WriteFile(string FileName,string[] str)
        {
            try
            {
                FileStream fs = new FileStream(FileName,FileMode.OpenOrCreate|FileMode.Append,FileAccess.Write);
                StreamWriter sw = new StreamWriter(fs);

                string msg;
                for (int i = 0; i < str.Length - 1; i++)
                {
                    //加密数据
                    msg = Converts(str[i]);
                    sw.WriteLine(msg);
                }

                fs.Flush();
                sw.Close();
                fs.Close();

                return true;
            }
            catch (Exception Ex)
            {
                return false;
            }
        }
        //读文件
        public bool ReadFile(string FileName,ref List<string> list)
        {
            string msg = null;
            try
            {
                FileStream fs = null;
                //打开文件
                fs = new FileStream(FileName, FileMode.OpenOrCreate,FileAccess.Read);
                StreamReader sw = new StreamReader(fs);

                while (true)
                {
                    msg = sw.ReadLine();
                    if (msg == null)
                        break;
                    //进行解密
                    msg = Converts(msg);
                    list.Add(msg);
                }

                sw.Close();
                fs.Close();

                return true;
            }
            catch(Exception Ex)
            {
                return false;
            }
        }
    }
}