/*
 * Date          Developer       Issue       Remark 
 * ------------------------------------------------------------------------------------------
 * 19/Mar/2014   David           #361        log4Net
 * 19/Mar/2014   David           #362        log4Net
 */

using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
//using sde.comNetsuiteSandboxServices;
using sde.comNetsuiteServices;
using System.IO;
using System.Text;
using System.Collections;
using log4net;
using System.Reflection;
using log4net.Config;
using System.Messaging;
using System.Xml;
using sde.WCFsde;
using System.Transactions;
using System.Web.SessionState;
using System.Globalization;
using System.Data.Entity.Validation;
using System.Data.Entity.Infrastructure;
using sde.Models;
using Microsoft.VisualBasic.FileIO;
using System.Timers;
using System.Data;
using MySql.Data.MySqlClient;
using System.Configuration;
using Microsoft.SqlServer;
//using Microsoft.SqlServer.Dts.Runtime;

namespace sde.Controllers
{
    [Authorize]
    public class AdHocController : Controller
    {
        //NetSuite
        private static NetSuiteService service = new NetSuiteService();
        WCFsde.Service1 obj = new WCFsde.Service1(); //web service #363
        WCFssa.SSA_Service1 ssa_obj = new WCFssa.SSA_Service1();
        //WCFwms.WMS_Service1 wms_obj = new WCFwms.WMS_Service1(); //web service #363

        //private readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType); //#361
        //private readonly ILog ErrLog = LogManager.GetLogger("SummaryLog");               //#361
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");              //#361


          [Authorize]
        public ActionResult AdhocExecuteWS()
        {
            return View();
        }

          public static string getNetsuitePassword()
          {
              string returnPass = "";
              try
              {
                  using (sdeEntities entities = new sdeEntities())
                  {
                      var nsSetting = (from s in entities.netsuite_setting
                                       where s.nss_account == @Resource.NETSUITE_LOGIN_EMAIL
                                       select new { s.nss_password }).ToList().FirstOrDefault();

                      returnPass = nsSetting.nss_password;

                  }
              }
              catch (Exception ex)
              {
                 
              }
              return returnPass;
          }

          public static Boolean login(NetSuiteService service)
          {
              service.Timeout = 820000000;
              service.CookieContainer = new CookieContainer();
              ApplicationInfo appinfo = new ApplicationInfo();
              appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
              service.applicationInfo = appinfo;

              Passport passport = new Passport();
              passport.account = sde.Resource.NETSUITE_LOGIN_ACCOUNT;         //"3479023"
              passport.email = sde.Resource.NETSUITE_LOGIN_EMAIL;             // "xypang@scholastic.asia"

              RecordRef role = new RecordRef();
              role.internalId = sde.Resource.NETSUITE_LOGIN_ROLE_INTERNALID;  //"18"

              passport.role = role;
              //passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;
              passport.password = getNetsuitePassword();
              return service.login(passport).status.isSuccess;
          }
        //
          [Authorize]
        public ViewResult AdhocScheduler()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                #region Scheduler
                var scheduler1 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("NS-") && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence,
                                      id = pro.sche_id
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.sequence).ToList();

                var scheduler2 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("SSA-") && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence,
                                      id = pro.sche_id
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.sequence).ToList();

                var scheduler3 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("MQPUSH-") && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence,
                                      id = pro.sche_id
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.sequence).ToList();

                var scheduler4 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("CPAS-") && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence,
                                      id = pro.sche_id
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.sequence).ToList();

                var allScheduler = scheduler1.Concat(scheduler2).Concat(scheduler3).ToList().Concat(scheduler4).ToList();
                SchedulerList scheList = new SchedulerList();
                scheList.scheduler = allScheduler;
                #endregion
                return View(new Tuple<sde.Models.SchedulerList>(scheList));
            }
        }

        public static int GetWeekNumber(DateTime dtDate)
        {
            CultureInfo ciGetNumber = CultureInfo.CurrentCulture;
            int returnNumber = ciGetNumber.Calendar.GetWeekOfYear(dtDate, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            return returnNumber;
        }

          [Authorize]
        public ActionResult EditScheduler(string scheId, string scheType, string tranType)
        {
            ViewData["scheId"] = scheId;
            ViewData["scheType"] = scheType;
            ViewData["tranType"] = tranType;
            return View();
        }
          [Authorize]
        public ActionResult UpdateScheduler(string scheId, string scheType, string scheStatus, string scheMinuteGap, string scheNextRun)
        {
            return View();
        }
          [Authorize]
        public ActionResult Measurement(string statId, string measureItem, string tranType)
        {
            ViewData["statId"] = statId;
            ViewData["measureItem"] = measureItem;

            //find work week number -begin
            DateTime dtDate = DateTime.Now;
            int[] weeknumbers = new int[] { GetWeekNumber(dtDate), GetWeekNumber(dtDate) - 1 };
            ViewData["weeknumbers"] = weeknumbers;
            //find work week number -end

            ViewData["tranType"] = tranType;
            return View();
        }

        [HttpPost]
        [Authorize]
        public ActionResult RunScheduler()
        {
            //while (1 == 1)
            //{
            try
            {
                obj.Url = @Resource.WCF_URL_SDE;
                String exeSchedulerResult = obj.ExecuteScheduler();
            }
            catch
            { }

            //}
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }

        [HttpPost]
        [Authorize]
        public ActionResult PushNetsuite()
        {
            //while (1 == 1)
            //{
            try
            {
                obj.Url = @Resource.WCF_URL_SDE;
                String pushNetsuiteResult = obj.PushNetsuite();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
            //}
        }

        [HttpPost]
        [Authorize]
        public ActionResult PullNetsuite()
        {
            //while (1 == 1)
            //{
            try
            {
                obj.Url = @Resource.WCF_URL_SDE;
                String pushNetsuiteResult = obj.PullNetsuite();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
            //}
        }

        [HttpPost]
        [Authorize]
        public ActionResult PushMQ()
        {
            //while (1 == 1)
            //{
            try
            {
                obj.Url = @Resource.WCF_URL_SDE;
                String pushMQResult = obj.PushMQ();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
            //}
        }

        [HttpPost]
        [Authorize]
        public ActionResult PullMQ()
        {
            //while (1 == 1)
            //{
            try
            {
                obj.Url = @Resource.WCF_URL_SDE;
                String pullMQResult = obj.PullMQ();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
            //}
        }

        [HttpPost]
        [Authorize]
        public ActionResult WMSpullMQ()
        {
            try
            {
                //wms_obj.Url = @Resource.WCF_URL_WMS;
                //wms_obj.WMSpullMQ();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }

        [HttpPost]
          [Authorize]
        public ActionResult SSApullMQ()
        {
            try
            {
                ssa_obj.Url = @Resource.WCF_URL_SSA;
                ssa_obj.SSApullMQ();
            }
            catch
            { }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }

        [HttpPost]
        [Authorize]
        public ActionResult Test()
        {
            #region Previous Code 
            //SearchPreferences sp = new SearchPreferences();
            //sp.bodyFieldsOnly = false;
            //service.searchPreferences = sp;

            //TransactionSearchAdvanced prtsa = new TransactionSearchAdvanced();
            //TransactionSearch prts = new TransactionSearch();
            //TransactionSearchBasic prtsb = new TransactionSearchBasic();

            //SearchEnumMultiSelectField poStatus = new SearchEnumMultiSelectField();
            //poStatus.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
            //poStatus.operatorSpecified = true;
            //poStatus.searchValue = new String[] { "_purchaseOrderPendingReceipt", "_purchaseOrderPartiallyReceived" };
            //prtsb.status = poStatus;

            //RecordRef r1 = new RecordRef();
            //r1.internalId = "101";
            //RecordRef r2 = new RecordRef();
            //r2.internalId = "103";

            //SearchMultiSelectField poBusinessChannel = new SearchMultiSelectField();
            //poBusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
            //poBusinessChannel.operatorSpecified = true;
            //poBusinessChannel.searchValue = new RecordRef[] { r1, r2 };
            //prtsb.@class = poBusinessChannel;
            
            //SearchMultiSelectCustomField poSync = new SearchMultiSelectCustomField();
            //poSync.@operator = SearchMultiSelectFieldOperator.anyOf;
            //poSync.operatorSpecified = true;
            //poSync.scriptId = "custbody_wms_field";
            //ListOrRecordRef listOrRecordRef = new ListOrRecordRef();
            //listOrRecordRef.internalId = "1";
            //listOrRecordRef.typeId = "136";
            //poSync.searchValue = new ListOrRecordRef[] { listOrRecordRef };
            //SearchCustomField[] scf = new SearchCustomField[] { poSync };
            //prtsb.customFieldList = scf;

            //prts.basic = prtsb;
            //prtsa.criteria = prts;
            //login();
            //SearchResult res = service.search(prtsa);
            //service.logout();
            #endregion

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {

                //String[] array_netsuiteID;
                //array_netsuiteID[0] = "114744"; //SO-MY00101
                //array_netsuiteID[1] = "108935"; //SO-MY0093
                //SalesOrder[] updateList = new SalesOrder[array_netsuiteID.Count()];

                //Int32 rowCount = 0;
                //for (int i = 0; i < array_netsuiteID.Count(); i++)
                //{
                //    rowCount = i + 1;

                //    //CustomFieldRef[] cfrList = new CustomFieldRef[1];
                //    //StringCustomFieldRef scfr = new StringCustomFieldRef();
                //    //scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                //    //scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                //    //scfr.value = updateValue;
                //    //cfrList[0] = scfr;

                //    SalesOrder tran = new SalesOrder();
                //    string a = array_netsuiteID[0].ToString();
                //    string b = array_netsuiteID[1].ToString();
                //    tran.internalId = array_netsuiteID[i];
                //    //tran.customFieldList = cfrList;
                    
                //    //updateList[i] = tran;
                     
                //    for (int c = 0; c < tran.itemList.item.Count(); c++)
                //    {
                //        tran.itemList.item[c].customFieldList[c].scriptId
                //    }
                     
                //}
                 
                //job = service.asyncUpdateList(updateList);
                //jobID = job.jobId; 
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("Test Exception: " + ex.ToString());
            }   
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }

        /*
        [HttpPost]
        [Authorize]
        public ActionResult SSIS()
        {
            this.DataFromNetsuiteLog.Debug("Package Execution results: Start");

            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                Application app = new Application();
                Package package = null;

                this.DataFromNetsuiteLog.Debug("Package Execution results: Get package");
                package = app.LoadPackage(@"C:\Dev\gstextract\gstextract_ssis\gstextract_ssis\pkgBCCustomer_CSV.dtsx", null);

                this.DataFromNetsuiteLog.Debug("Package Execution results: Execute package");
                Microsoft.SqlServer.Dts.Runtime.DTSExecResult results = package.Execute();

                if (results == Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure)
                {
                    foreach (Microsoft.SqlServer.Dts.Runtime.DtsError local_DtsError in package.Errors)
                    {
                        this.DataFromNetsuiteLog.Error("Package Execution results: " + local_DtsError.Description.ToString());
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Debug("Package Execution results: Success");
                }
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SSIS Exception: " + ex.ToString());
            }

            this.DataFromNetsuiteLog.Debug("Package Execution results: End");
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }
        */

        /*public ActionResult Test()
        {
            try
            {
                #region login
                service.Timeout = 820000000;
                service.CookieContainer = new CookieContainer();

                Passport passport = new Passport();
                passport.account = "3479023";
                //passport.email = "khng@scholastic.asia";
                passport.email = "lsting@scholastic.asia";

                RecordRef role = new RecordRef();
                role.internalId = "3";

                passport.role = role;
                //passport.password = "Netsuite2#";
                passport.password = "scholastic123";

                //List<cls_inventory> invList = new List<cls_inventory>();

                Status status = service.login(passport).status;
                #endregion

                InventoryItem ii = new InventoryItem();
                ii.internalId = "785";

                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                StringCustomFieldRef scfr = new StringCustomFieldRef();
                scfr.scriptId = "custitem63";
                scfr.internalId = "1409";
                scfr.value = "abc";
                cfrList[0] = scfr;
                
                ii.customFieldList = cfrList;
                
                WriteResponse res = service.update(ii);

            }
            catch (Exception ex)
            {
            }
            return RedirectToAction("AdhocExecuteWS", "AdHoc");
        }
    }*/

        public Boolean login()
        {
            service.Timeout = 820000000;
            service.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
            service.applicationInfo = appinfo;

            Passport passport = new Passport();
            passport.account = "3479023";
            passport.email = "lsting@scholastic.asia";

            RecordRef role = new RecordRef();
            role.internalId = "18";

            passport.role = role;
            passport.password = "lsTing0428";

            Status status = service.login(passport).status;
            return status.isSuccess;
        }


    }

}
