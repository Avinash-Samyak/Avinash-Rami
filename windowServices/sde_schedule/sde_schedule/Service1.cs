/*
 * Date          Developer       Issue       Remark 
 * ------------------------------------------------------------------------------------------
 * 19/Mar/2014   David           #361        log4Net
 * 19/Mar/2014   David           #362        log4Net
 */

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
using sde_schedule.WCFsde;

namespace sde_schedule
{
    public partial class Service1 : ServiceBase
    {
        Thread thr = new Thread(new ThreadStart(run));
        //WCFsde.Service1 obj = new WCFsde.Service1(); //web service #363
        //private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        //private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");              //#361

        public Service1()
        {
            InitializeComponent();
            //thr.Start();
        }

        static void run()
        {
            string path = @"C:\inetpub\wwwroot\sde_publish_uat\WindowServices\logger\start" + "-scheduler.log";
            string path2 = @"C:\inetpub\wwwroot\sde_publish_uat\WindowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-scheduler.txt";
              try
            {
                WCFsde.Service1 obj = new WCFsde.Service1();
         

                while (1 == 1)
                {
                    try
                    {
                        //TimeSpan ts = DateTime.Now - Convert.ToDateTime(File.GetLastWriteTime(path));
                        //if (ts.Minutes > 5)
                        //{
                        //    if (File.Exists(path))
                        //    {
                        //        File.Delete(path);
                        //    }

                        //    StreamWriter str = new StreamWriter(path, true);
                        try
                        {
                            String scheduler = obj.ExecuteScheduler();
                            Thread.Sleep(60000); // sleep 1 minute after task
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
                    }
                    //}
                    catch (Exception e)
                    {
                        StreamWriter str2 = new StreamWriter(path2, true);
                        str2.WriteLine(DateTime.Now + " " + e.ToString());
                        str2.Close();
                    }
                }  
 
            }
            catch (Exception e)
            {
                StreamWriter str2 = new StreamWriter(path2, true);
                str2.WriteLine(DateTime.Now + " " + e.ToString());
                str2.Close();
            }
         
        }

        protected override void OnStart(string[] args)
        {
            /*
            this.DataFromNetsuiteLog.Debug("1. Debug message"); //#361
            this.DataFromNetsuiteLog.Info("1. Info message"); //#361
            this.DataFromNetsuiteLog.Warn("1. Warn message"); //#361
            this.DataFromNetsuiteLog.Error("1. Err message"); //#361
            this.DataFromNetsuiteLog.Fatal("1. Fatal message"); //#361
            */

            string path = @"C:\inetpub\wwwroot\sde_publish_uat\WindowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-scheduler.txt";

            // if (!File.Exists(path))
            // {
            //     File.Create(path);
            // }

            // if (File.Exists(path))
            // {
                StreamWriter str = new StreamWriter(path, true);
                str.WriteLine("Service started on : " + DateTime.Now.ToString());
                str.Close();

                thr.Start();
            // }
        }

        protected override void OnStop()
        {
            string path = @"C:\inetpub\wwwroot\sde_publish_uat\WindowServices\logger\" + DateTime.Now.ToString("yyyyMMdd") + "-scheduler.txt";
            StreamWriter str = new StreamWriter(path, true);
            str.WriteLine("Service stoped on : " + DateTime.Now.ToString());
            str.Close();
            thr.Abort();
        }
    }
}
