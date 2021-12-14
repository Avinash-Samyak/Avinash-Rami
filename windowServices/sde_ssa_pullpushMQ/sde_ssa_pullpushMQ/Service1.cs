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
using log4net;
using sde_ssa_pullpushMQ.WCFssa;

namespace sde_ssa_pullpushMQ
{
    public partial class Service1 : ServiceBase
    {
        Thread thr = new Thread(new ThreadStart(run));

        public Service1()
        {
            InitializeComponent();
        }

        static void run()
        {
            WCFssa.SSA_Service1 obj = new WCFssa.SSA_Service1();
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\start" + "-ssa_pullpushMQ.log";
            string path2 = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-ssa_pullpushMQ.txt";

            while (1 == 1)
            { 
                try
                {
                    //TimeSpan ts = DateTime.Now - Convert.ToDateTime(File.GetLastWriteTime(path));
                    //if (ts.Minutes > 2)
                    //{
                        //if (File.Exists(path))
                        //{
                        //    File.Delete(path);
                        //}

                        //StreamWriter str = new StreamWriter(path, true);
                        try
                        {
                            obj.SSApullMQ();
                            Thread.Sleep(60000);  // sleep 1 minutes after task
                        }
                        catch (Exception e)
                        {
                            StreamWriter str2 = new StreamWriter(path2, true);
                            str2.WriteLine(DateTime.Now + " " + e.ToString());
                            str2.Close();
                        }
                        finally
                        {
                            //int ii = 0;
                            //// 0.5 minutes
                            //while (ii < 22)
                            //{
                            //    //1 second delay
                            //    int i = 0;
                            //    while (i < 500000000)
                            //    {
                            //        i++;
                            //    }

                            //    ii++;
                            //}
                            //str.Close();
                        }
                    //}
                }
                catch (Exception e)
                {
                    StreamWriter str2 = new StreamWriter(path2, true);
                    str2.WriteLine(DateTime.Now + " " + e.ToString());
                    str2.Close();
                }
            }
        }

        protected override void OnStart(string[] args)
        {
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-ssa_pullpushMQ.txt";
            StreamWriter str = new StreamWriter(path, true);
            str.WriteLine("Service started on : " + DateTime.Now.ToString());
            str.Close();

            thr.Start();
        }

        protected override void OnStop()
        {
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-ssa_pullpushMQ.txt";
            StreamWriter str = new StreamWriter(path, true);
            str.WriteLine("Service stoped on : " + DateTime.Now.ToString());
            str.Close();
            thr.Abort();
        }
    }
}
