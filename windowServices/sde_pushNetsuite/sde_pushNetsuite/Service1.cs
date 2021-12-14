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
using sde_pushNetsuite.WCFsde;

namespace sde_pushNetsuite
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
            WCFsde.Service1 obj = new WCFsde.Service1();
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\start" + "-pushNetsuite.log";
            string path2 = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-pushNetsuite.txt";

            while (1 == 1) 
            {
               try
               {
                    //TimeSpan ts = DateTime.Now - Convert.ToDateTime(File.GetLastWriteTime(path));
                    //if (ts.Minutes > 5)
                    //{
                    //    //StreamWriter str2 = new StreamWriter(path2, true);
                    //    //str2.WriteLine(DateTime.Now + "  Timespan: " + ts.Minutes.ToString());
                    //    //str2.Close();

                    //    if (File.Exists(path))
                    //    {
                    //        File.Delete(path);
                    //    }

                    //    StreamWriter str = new StreamWriter(path, true);
                        try
                        {
                            String pushNetsuite = obj.PushNetsuite();
                            Thread.Sleep(60000);  // sleep for 1 minutes
                        }
                        catch (Exception e)
                        {
                            StreamWriter str3 = new StreamWriter(path2, true);
                            str3.WriteLine(DateTime.Now + " " + e.ToString());
                            str3.Close();
                        }
                        finally
                        {
                            //int ii = 0;
                            //// 5 minutes
                            //while (ii < 220)
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
                    //else
                    //{
                    //    StreamWriter str2 = new StreamWriter(path2, true);
                    //    str2.WriteLine(DateTime.Now + "  Timespan: " + ts.Minutes.ToString());
                    //    str2.Close();
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
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-pushNetsuite.txt";
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
            string path = @"C:\inetpub\wwwroot\sde_publish\windowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-pushNetsuite.txt";
            StreamWriter str = new StreamWriter(path, true);
            str.WriteLine("Service stoped on : " + DateTime.Now.ToString());
            str.Close();
            thr.Abort();
        }
    }
}
