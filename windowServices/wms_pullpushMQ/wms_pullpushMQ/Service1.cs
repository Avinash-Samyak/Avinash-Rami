using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace wms_pullpushMQ
{
    public partial class Service1 : ServiceBase
    {
        Thread thr = new Thread(new ThreadStart(run));
        //WMSservice1.WMS_Service1 obj = new WMSservice1.WMS_Service1();

        public Service1()
        {
            InitializeComponent();
        }

        static void run()
        {
            WCFssa.SSA_Service1 obj = new WCFssa.SSA_Service1();
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\start" + "-wmsPullPushMQ.log";
            string path2 = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-wmsPullPushMQ.txt";

            while (1 == 1)
            {
                try
                {
                    if (File.Exists(path))
                    {
                        File.Delete(path);
                    }

                    StreamWriter str = new StreamWriter(path, true);
                    try
                    {
                        obj.SSApullMQ();
                    }
                    catch (Exception e)
                    {
                        StreamWriter str2 = new StreamWriter(path2, true);
                        str2.WriteLine(DateTime.Now + " " + e.ToString());
                        str2.Close();
                    }
                    finally
                    {
                        str.Close();
                    }
                }
                catch (Exception e)
                {
                    StreamWriter str2 = new StreamWriter(path2, true);
                    str2.WriteLine(e.ToString());
                    str2.Close();
                }
            }
        }

        protected override void OnStart(string[] args)
        {
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-WMS_pullpushMQ.txt";
            //if (!File.Exists(path))
            //{
            //    File.Create(path);
            //}

            //if (File.Exists(path))
            //{
                StreamWriter str = new StreamWriter(path, true);
                str.WriteLine("Service started on : " + DateTime.Now.ToString());
                str.Close();

                thr.Start();
            //}
        }

        protected override void OnStop()
        {
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-WMS_pullpushMQ.txt";
            StreamWriter str = new StreamWriter(path, true);
            str.WriteLine("Service stoped on : " + DateTime.Now.ToString());
            str.Close();
            thr.Abort();
        }
    }
}
