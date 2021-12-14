using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Net;
using sde.Models;
using log4net;
//using sde.comNetsuiteSandboxServices;
using sde.comNetsuiteServices;
using sde.WCF;
using System.Transactions;
using System.Timers;
using System.IO;
using Microsoft.VisualBasic.FileIO;

namespace sde.Controllers
{
    public class DashboardRitController : Controller
    {
        //NetSuite
        private static NetSuiteService service = new NetSuiteService();
        sde.Models.linq_recovery recovery = new sde.Models.linq_recovery();
        private static List<string> jobIDList = new List<string>();

        //
        // GET: /DashboardRIT/
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");
        private static Timer addListTimer;
        private static string jobId;
        string externalID;

        [Authorize]
        public ActionResult Index()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                //DateTime lastXDate = DateTime.Now.AddDays(-2);
                #region Scheduler
/*
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
                                     sequence = pro.sche_sequence
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
                                      sequence = pro.sche_sequence
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
                                      sequence = pro.sche_sequence
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
                                      sequence = pro.sche_sequence
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.sequence).ToList();

                var scheduler5 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("CONF-") && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence
                                  }).OrderBy(x => x.transactionType).OrderBy(y => y.nextRun).ToList();

                var allScheduler = scheduler1.Concat(scheduler2).Concat(scheduler3).ToList().Concat(scheduler4).ToList().Concat(scheduler5).ToList();
*/

                var scheduler6 = (from pro in entities.schedulers
                                  where pro.sche_status == "ACTIVE" && pro.sche_minuteGap == "1440"
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence
                                  }).OrderBy(x => x.minuteGap).OrderBy(y => y.nextRun).ToList();

                var scheduler7 = (from pro in entities.schedulers
                                  where pro.sche_status == "ACTIVE" && pro.sche_minuteGap != "1440"
                                  select new cls_scheduler()
                                  {
                                      transactionType = pro.sche_transactionType,
                                      status = pro.sche_status,
                                      minuteGap = pro.sche_minuteGap,
                                      nextRun = pro.sche_nextRun,
                                      nextRunSeqNo = pro.sche_nextRunSeqNo,
                                      lastRun = pro.sche_lastRun,
                                      lastRunSeqNo = pro.sche_lastRunSeqNo,
                                      sequence = pro.sche_sequence
                                  }).OrderBy(x => x.minuteGap).OrderBy(y => y.nextRun).ToList();

                var allScheduler = scheduler6.Concat(scheduler7).ToList();
                SchedulerList scheList = new SchedulerList();
                scheList.scheduler = allScheduler;
                #endregion
                return View(new Tuple<sde.Models.SchedulerList>(scheList));
            }
        }

        public ActionResult DataSyncBack()
        {
            DateTime threeDaysAgo = DateTime.Now.Date.AddDays(-3);
            using (sdeEntities entities = new sdeEntities())
            {
                var reqNSDataforMQ = (from req in entities.netsuitedataformqs
                                      where req.mq_status == "PULLED" &&
                                      req.mq_updatedDate >= threeDaysAgo
                                      orderby req.mq_updatedDate descending
                                      select new cls_nsdataformq()
                                      {
                                          transactionType = req.mq_transactionType,
                                          jobID = req.mq_nsj_jobID,
                                          internalID = req.mq_internalID,
                                          consolidateTable = req.mq_consolidateTable,
                                          status = req.mq_status,
                                          pushDate = req.mq_pushDate,
                                          pullDate = req.mq_pullDate,
                                          updatedDate = req.mq_updatedDate,
                                          tranID = req.mq_tranID
                                      }).ToList();
                NSDataForMqList nsDataForMqList = new NSDataForMqList();
                nsDataForMqList.list = reqNSDataforMQ;

                return View(new Tuple<sde.Models.NSDataForMqList>(nsDataForMqList));
            }
           
        }

        public ActionResult DummySO(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-1);
                    toDate = DateTime.Now.AddDays(1);
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }
                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;


               
                var dummyso = (from n in entities.dummysoes
                                  join mi in entities.map_item
                                  on n.dso_itemInternalId equals mi.mi_item_internalID
                               where n.dso_createdDate > fromDate &&
                               n.dso_createdDate < toDate

                                  select new cls_dummySO()
                                  {
                                      dummySOID = n.dso_soInternalId,
                                      itemInternalID = n.dso_itemInternalId,
                                      originalQty = n.dso_oriQuantity,
                                      newQty = n.dso_newQuantity,
                                      pushtoNSDate = n.dso_createdDate,
                                      pullFromWMSDate = n.dso_rangeTo,
                                      subsidiaryID = n.dso_subsidiary,
                                      businessChannelID = n.dso_businessChannel,
                                      ISBN = mi.mi_item_isbn,
                                      title = mi.mi_item_title,
                                      item_ID = mi.mi_item_ID,
                                      deductedQty = 0
                                  }).OrderByDescending(x => x.pushtoNSDate).ToList();

                dummySOList dummySOList = new dummySOList();
                dummySOList.dummySO = dummyso;

                return View(new Tuple<sde.Models.dummySOList>(dummySOList));
            }
        }

        [Authorize]
        public ActionResult SDEStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-1);
                    toDate = DateTime.Now.AddDays(1);
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }               

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                var requestnetsuite1 = (from n in entities.requestnetsuites
                                        where n.rn_updatedDate >= fromDate && n.rn_updatedDate <= toDate
                                        select new cls_requestnetsuite()
                                        {
                                            transactionType = n.rn_sche_transactionType,
                                            status = n.rn_status,
                                            seqNo = n.rn_seqNo,
                                            jobID = n.rn_jobID,
                                            rangeFrom = n.rn_rangeFrom,
                                            rangeTo = n.rn_rangeTo,
                                            updatedDate=n.rn_updatedDate,
                                            sequence = n.rn_sequence
                                        }).OrderByDescending(x => x.updatedDate).ToList();

                var requestnetsuite2 = (from n in entities.requestnetsuites
                                        where n.rn_status == "STAND BY"
                                        select new cls_requestnetsuite()
                                        {
                                            transactionType = n.rn_sche_transactionType,
                                            status = n.rn_status,
                                            seqNo = n.rn_seqNo,
                                            jobID = n.rn_jobID,
                                            rangeFrom = n.rn_rangeFrom,
                                            rangeTo = n.rn_rangeTo,
                                            sequence = n.rn_sequence
                                        }).OrderBy(x => x.transactionType).ToList();

                var allRequestNetsuite = requestnetsuite1.Concat(requestnetsuite2).ToList();
                RequestNetsuiteList rnList = new RequestNetsuiteList();
                rnList.requestnetsuite = allRequestNetsuite;
                return View(new Tuple<sde.Models.RequestNetsuiteList>(rnList));
            }
        }

        /*
        [Authorize]
        public ActionResult SDEStatistic(String tranType,String jobID,DateTime rangeTo, String tranPage)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DashboardSalesOrderList dbsoList = new DashboardSalesOrderList();
                if (tranType.Equals("SSA-FULFILLMENT"))//NS-SALES ORDER
                {
                    var salesOrderSum = (from so in entities.dashboard_salesorder
                                         where so.nsj_rangeTo == rangeTo
                                         group so by so.nsj_jobID into g
                                         select g).ToList();

                    foreach (var item in salesOrderSum)
                    {
                        view_dashboard_salesorder vdso = new view_dashboard_salesorder();
                        vdso.jobID = item.Key;

                        foreach (var det in item)
                        {
                            vdso.businessChannel = det.nsj_businessChannel_ID;
                            vdso.country_tag = det.nsj_country_tag;
                            vdso.moCount = det.nsj_job_mo_count;
                            vdso.rangeTo = det.nsj_rangeTo;
                        }
                        dbsoList.dashboardSOSummary = vdso;
                    }                   

                    var salesOrderDet = (from so in entities.dashboard_salesorder
                                         where so.nsj_rangeTo == rangeTo
                                         select new view_dashboard_salesorder()
                                         {
                                             jobID = so.nsj_jobID,
                                             jobMoID = so.nsjm_jobMo_ID,
                                             businessChannel = so.nsj_businessChannel_ID,
                                             country_tag = so.nsj_country_tag,
                                             moCount = so.nsj_job_mo_count,
                                             moNo = so.nsjm_moNo,
                                             moInternalID = so.nsjm_moNo_internalID,
                                             rangeTo = so.nsj_rangeTo
                                         }).ToList();
                    dbsoList.dashboardSODetails = salesOrderDet;
                }
                return View(dbsoList);
            }
        }
        */

        [Authorize]
        public ActionResult MQStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-1);
                    toDate = DateTime.Now.AddDays(1);
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                } 
                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                /*
                    var requestmq1 = (from n in entities.requestmqs
                                        where n.rmq_status == "START" && n.rmq_rangeFrom >= fromDate && n.rmq_rangeFrom <= toDate
                                        select new cls_requestmq()
                                        {
                                            transactionType = n.rmq_sche_transactionType,
                                            status = n.rmq_status,
                                            seqNo = n.rmq_seqNo,
                                            jobID = n.rmq_jobID,
                                            rangeFrom = n.rmq_rangeFrom,
                                            rangeTo = n.rmq_rangeTo,
                                            sequence = n.rmq_sequence
                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                    var requestmq2 = (from n in entities.requestmqs
                                        where n.rmq_status == "FINISHED" && n.rmq_rangeFrom >= fromDate && n.rmq_rangeFrom <= toDate
                                        select new cls_requestmq()
                                        {
                                            transactionType = n.rmq_sche_transactionType,
                                            status = n.rmq_status,
                                            seqNo = n.rmq_seqNo,
                                            jobID = n.rmq_jobID,
                                            rangeFrom = n.rmq_rangeFrom,
                                            rangeTo = n.rmq_rangeTo,
                                            sequence = n.rmq_sequence
                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                    var requestmq3 = (from n in entities.requestmqs
                                        where n.rmq_status == "UPLOADED" && n.rmq_rangeFrom >= fromDate && n.rmq_rangeFrom <= toDate
                                        select new cls_requestmq()
                                        {
                                            transactionType = n.rmq_sche_transactionType,
                                            status = n.rmq_status,
                                            seqNo = n.rmq_seqNo,
                                            jobID = n.rmq_jobID,
                                            rangeFrom = n.rmq_rangeFrom,
                                            rangeTo = n.rmq_rangeTo,
                                            sequence = n.rmq_sequence
                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                    var allRequestMQ = requestmq1.Concat(requestmq2).Concat(requestmq3).ToList();
                */

                var requestmq1 = (from n in entities.requestmqs
                                  where n.rmq_updatedDate >= fromDate && n.rmq_updatedDate <= toDate
                                  select new cls_requestmq()
                                  {
                                      transactionType = n.rmq_sche_transactionType,
                                      status = n.rmq_status,
                                      seqNo = n.rmq_seqNo,
                                      jobID = n.rmq_jobID,
                                      rangeFrom = n.rmq_rangeFrom,
                                      rangeTo = n.rmq_rangeTo,
                                      sequence = n.rmq_sequence,
                                      completedAt = n.rmq_completedAt
                                  }).OrderBy(x => x.transactionType).ToList();

                var requestmq2 = (from n in entities.requestmqs
                                  where n.rmq_status == "STAND BY"
                                  select new cls_requestmq()
                                  {
                                      transactionType = n.rmq_sche_transactionType,
                                      status = n.rmq_status,
                                      seqNo = n.rmq_seqNo,
                                      jobID = n.rmq_jobID,
                                      rangeFrom = n.rmq_rangeFrom,
                                      rangeTo = n.rmq_rangeTo,
                                      sequence = n.rmq_sequence
                                  }).OrderBy(x => x.transactionType).ToList();

                var allRequestMQ = requestmq1.Concat(requestmq2).OrderBy(y => y.rangeFrom).ToList();


                RequestMQList rmqList = new RequestMQList();
                rmqList.requestmq = allRequestMQ;

                return View(new Tuple<sde.Models.RequestMQList>(rmqList));
            }
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

        [Authorize]
        public ActionResult TasksStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-1);
                    toDate = DateTime.Now.AddDays(1);
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                var requesterror = (from n in entities.requestnetsuite_task
                                    where n.rnt_createdDate >= fromDate && n.rnt_createdDate <= toDate
                                    select new cls_error()
                                    {
                                        taskID = n.rnt_id,
                                        taskName = n.rnt_task,
                                        status = n.rnt_status,
                                        jobID = n.rnt_jobID,
                                        refNO = n.rnt_refNO,
                                        taskDescription = n.rnt_description,
                                        nsInternalId = n.rnt_nsInternalId,
                                        moInternalId = n.rnt_createdFromInternalId,
                                        errorDescription = n.rnt_errorDesc,
                                        updatedDate = n.rnt_updatedDate,
                                        createdDate = n.rnt_createdDate
                                    }).ToList();

                var allRequestError = requesterror.OrderBy(x => x.taskID).ToList();
                RequestErrorList errorList = new RequestErrorList();
                errorList.error = allRequestError;
                
                return View(new Tuple<sde.Models.RequestErrorList>(errorList));
            }
        }

        [Authorize]
        [HttpPost,ActionName("ErrorRerun")]
        public ActionResult ErrorRerun(IEnumerable<String> rerunList)
        {
            String updatedDate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            NetSuiteService service = new NetSuiteService();
            if (login(service) == true)
            {
                SearchPreferences sp = new SearchPreferences();
                sp.bodyFieldsOnly = false;
                service.searchPreferences = sp;

                var option = new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.RepeatableRead,
                    //Timeout = TimeSpan.FromSeconds(300)
                    Timeout = TimeSpan.FromSeconds(2400)
                };

                if (rerunList != null)
                { 
                    using (sdeEntities entities = new sdeEntities())
                    {
                        foreach (string rerun_job in rerunList)
                        {
                            string[] array_rerun_job = rerun_job.Split(';');
                            int taskId = Convert.ToInt32(array_rerun_job[0]);

                            var errorList = (from reqTask in entities.requestnetsuite_task
                                                where reqTask.rnt_id == taskId
                                                && reqTask.rnt_status == "FALSE"
                                                select new { reqTask.rnt_description, reqTask.rnt_nsInternalId, reqTask.rnt_createdFromInternalId})
                                            .ToList();

                            if (errorList.Count() > 0)
                            {
                                using (TransactionScope scope1 = new TransactionScope())
                                {
                                    string rerun = "";
                                    foreach (var err_rec in errorList)
                                    {
                                        string[] array_refNo = Convert.ToString(array_rerun_job[1]).Split('.');
                                        string refNo = "";
                                        string refRange = "";
                                        string refType = "";

                                        string jobType = Convert.ToString(array_rerun_job[2]);
                                        switch (jobType)
                                        {
                                            #region TRADE-ReRun
                                            case "SSA-FULFILLMENT":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                rerun = recovery.rerun_trade_sales_order_fulfill(service, entities, refNo, refRange);
                                                break;
                                            case "SSA-UNSCAN":
                                                break;
                                            case "SSA-PURCHASE ORDER":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                rerun = recovery.rerun_trade_purchase_order(service, entities, refNo, refRange);
                                                break;
                                            case "SSA-EXCESS PURCHASE ORDER":
                                                break;
                                            case "SSA-RETURN AUTHORIZATION":
                                                break;
                                            #endregion
                                            #region BCAS-ReRun
                                            case "BCAS-ORDER ADJUSTMENT":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                refType = array_refNo[4];
                                                rerun = recovery.rerun_bcas_order_adjustment(service, entities, refNo, refRange, refType);
                                                break;
                                            case "BCAS-JOURNAL":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                refType = array_refNo[4];
                                                rerun = recovery.rerun_bcas_journal(service, entities, refNo, refRange, refType);
                                                break;
                                            case "BCAS-SALES ORDER":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                refType = array_refNo[4];
                                                rerun = recovery.rerun_bcas_sales_order(service, entities, refNo, refRange, refType);
                                                break;
                                            case "UPD-STATUS.BCAS-SALES ORDER":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_bcas_sales_order_fulfill(service, entities, refNo);
                                                break;
                                            case "BCAS-DEDUCT DUMMY SALES ORDER":
                                                refNo = array_refNo[2];
                                                refRange = array_refNo[3];
                                                refType = array_refNo[4];
                                                rerun = recovery.rerun_bcas_deduct_dummy_sales_order(service, entities, refNo, refRange, refType);
                                                break;
                                            #endregion
                                            #region CPAS-ReRun
                                            case "CPAS-ORDER ADJUSTMENT":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_cpas_order_adjustment(service, entities, refNo, taskId);
                                                break;
                                            case "CPAS-CANCELLATION ORDER":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_cpas_order_cancellation(service, entities, refNo, taskId);
                                                break;
                                            case "CPAS-JOURNAL":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_cpas_journal(service, entities, refNo, taskId);
                                                break;
                                            case "CPAS-SALES ORDER":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_cpas_sales_order(service, entities, refNo, taskId);
                                                break;
                                            case "UPD-STATUS.CPAS-SALES ORDER":
                                                refNo = array_refNo[2];
                                                rerun = recovery.rerun_cpas_sales_order_fulfill(service, entities, refNo, taskId);
                                                break;
                                            #endregion

                                            default:
                                                break;
                                        }
                                    }
                                    if (rerun == "")
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_status = 'TRUE', rnt_errorDesc = '', rnt_updatedDate ='" + updatedDate + "' where rnt_id = " + taskId;
                                        entities.Database.ExecuteSqlCommand(updateTask);
                                    }
                                    else
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_status = 'FALSE', rnt_errorDesc = '" + rerun + "', rnt_updatedDate ='" + updatedDate + "' where rnt_id = " + taskId;
                                        entities.Database.ExecuteSqlCommand(updateTask);
                                    }
                                    scope1.Complete();
                                }
                            }
                        } 
                    }
                }
            }

            return Json(Url.Action("TasksStatus", "Dashboard"));              
        }

        //[HttpPost]
        //public ActionResult StockTransfer()
        //{
        //    string errorMessage = null;
        //    Boolean status = false;
        //    //using (TransactionScope scope1 = new TransactionScope())
        //    //{
        //        Boolean loginStatus = login(service);
        //        if (loginStatus == true)
        //        {
        //            using (sdeEntities entities = new sdeEntities())
        //            {
        //                AsyncStatusResult job = new AsyncStatusResult();
        //                Int32 daCount = 0;
        //                Guid gjob_id = Guid.NewGuid();

                       
        //                var directTransfer = (from nat in entities.netsuite_transfer
                                             
        //                                 select nat).ToList();

        //                InventoryTransfer[] invTransferList = new InventoryTransfer[directTransfer.Count()];

        //                foreach (var d in directTransfer)
        //                {
        //                    try
        //                    {
        //                        Int32 itemCount = 0;
        //                        InventoryTransfer invTransfer = new InventoryTransfer();
        //                        InventoryTransferInventoryList itil = new InventoryTransferInventoryList();
                                
        //                        RecordRef refSubsidiary = new RecordRef();
        //                        refSubsidiary.internalId = d.nat_subsidiaryID.ToString();

        //                        RecordRef refFromLocation = new RecordRef();
        //                        refFromLocation.internalId = d.nat_fromLocID.ToString();

        //                        RecordRef refToLocation = new RecordRef();
        //                        refToLocation.internalId = d.nat_toLocID.ToString();

        //                        RecordRef refBusinessChannel = new RecordRef();
        //                        refBusinessChannel.internalId = d.nat_businessChannelID.ToString();

        //                        RecordRef refPostingPeriod = new RecordRef();
        //                        refPostingPeriod.internalId = d.nat_postingPeriodID.ToString();

        //                        invTransfer.subsidiary = refSubsidiary;
        //                        invTransfer.@class = refBusinessChannel;
        //                        invTransfer.location = refFromLocation;
        //                        invTransfer.transferLocation = refToLocation;
        //                        invTransfer.tranDate = Convert.ToDateTime(d.nat_Date);
        //                        invTransfer.tranDateSpecified = true;
        //                        invTransfer.externalId = "Stock Transfer " + d.nat_id.ToString();
        //                        invTransfer.postingPeriod = refPostingPeriod;
        //                        invTransfer.memo = d.nat_Memo;


        //                        var directTransferItem = (from natd in entities.netsuite_transferdetail
        //                                                  where natd.natd_businessChannelID == d.nat_businessChannelID &&
        //                                                  natd.natd_fromLocID == d.nat_fromLocID &&
        //                                                  natd.natd_toLocID == d.nat_toLocID &&
        //                                                  natd.natd_subsidiaryID == d.nat_subsidiaryID &&
        //                                                  natd.natd_date == d.nat_Date &&
        //                                                  natd.nat_postingPeriodID == d.nat_postingPeriodID 
        //                                                  select natd).ToList();


        //                        if (directTransferItem.Count() > 0)
        //                        {
        //                            InventoryTransferInventory[] items = new InventoryTransferInventory[directTransferItem.Count()];

        //                            foreach (var i in directTransferItem)
        //                            {
        //                                RecordRef refItem = new RecordRef();
        //                                refItem.internalId = i.natd_itemInternalID.ToString();

        //                                InventoryTransferInventory item = new InventoryTransferInventory();

        //                                item.item = refItem;
        //                                item.adjustQtyBy = Double.Parse(i.natd_qty.ToString());
        //                                item.adjustQtyBySpecified = true;
        //                                //item.description = i.natd_itemID;
        //                                items[itemCount] = item;
        //                                itemCount++;
        //                            }
        //                            itil.inventory = items;
        //                            invTransfer.inventoryList = itil;
        //                            invTransferList[daCount] = invTransfer;

        //                            //rowCount = daCount + 1;
        //                            //var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
        //                            //    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-INVENTORY ADJUSTMENT', '" + d.da_directAdjID + "', '" + gjob_id.ToString() + "'," +
        //                            //    "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','')";
        //                            //this.DataFromNetsuiteLog.Debug(insertTask);
        //                            //entities.Database.ExecuteSqlCommand(insertTask);

        //                            daCount++;
        //                            status = true;
        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        errorMessage = ex.Message;
        //                        //this.DataFromNetsuiteLog.Error(ex.ToString());
        //                        status = false;
        //                    }
        //                }//end of directAdj

        //                if (status == true)
        //                {
        //                    login(service);
        //                    InventoryTransfer[] divInvTransferList = new InventoryTransfer[400];
        //                    for (int i = 0; i < invTransferList.Length; i = i + 400)
        //                    {

        //                        if (invTransferList.Length - i < 400)
        //                        {
        //                            AsyncStatusResult asyncRes = new AsyncStatusResult();
        //                            Array.Resize(ref divInvTransferList, invTransferList.Length - i);
        //                            Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, invTransferList.Length - i);
        //                            string y = divInvTransferList.Length.ToString();
        //                            asyncRes = service.asyncAddList(divInvTransferList);
        //                            jobId = asyncRes.jobId;
        //                            jobIDList.Add(jobId);

        //                        }
        //                        else
        //                        {
        //                            AsyncStatusResult asyncRes = new AsyncStatusResult();
        //                            Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, 400);
        //                            asyncRes = service.asyncAddList(divInvTransferList);
        //                            jobId = asyncRes.jobId;
        //                            jobIDList.Add(jobId);
        //                        }
        //                    }
        //                    addListTimer = new System.Timers.Timer(900000);
        //                    addListTimer.Elapsed += onTimedEvent;
        //                    addListTimer.Enabled = true;

        //                    while (addListTimer.Enabled == true)
        //                    {
        //                    }
                            
        //                }
        //                ;
        //            }//end of sdeEntities
        //        }
        //        else
        //        {
        //            throw new Exception(errorMessage);
        //        }
        //    //}//end of scope1
           
        //    return Content("bah");
        //    //return View();
        //}

        //[HttpPost]
        //public ActionResult NetsuiteAdjustment()
        //{
        //    string errorMessage = null;
        //    Boolean status = false;
        //    //using (TransactionScope scope1 = new TransactionScope())
        //    //{
               
        //        Boolean loginStatus = login(service);
        //        if (loginStatus == true)
        //        {
        //            using (sdeEntities entities = new sdeEntities())
        //            {
        //                AsyncStatusResult job = new AsyncStatusResult();
        //                Int32 daCount = 0;
        //                Guid gjob_id = Guid.NewGuid();

        //                //var directAdj = (from da in entities.wms_directadjustment
        //                //                 //where da.da_rangeTo == rangeTo
        //                //                 select da).ToList();

        //                var directAdj = (from nas in entities.netsuite_adjustment2
        //                                 select nas).ToList();

        //                //var directAdj = (from nai in entities.netsuite_adjustmentitem
        //                //                 join nas in entities.netsuite_adjustment on nai.nai_shipmentNo equals nas.nas_shipmentNo
        //                //                 select new { nas.nas_firstShipDate, nas.nas_analysisCode, nas.nas_receivedQty, nas.nas_localCost, nai.nai_nsLocationID, nai.nai_nsSubsidiaryID, nai.nai_nsitemInternalID, nai.nai_nsbusinessChannelID }).Distinct().ToList();

        //                InventoryAdjustment[] invAdjList = new InventoryAdjustment[directAdj.Count()];

        //                foreach (var d in directAdj)
        //                {
        //                    try
        //                    {
        //                        Int32 itemCount = 0;
        //                        InventoryAdjustment invAdj = new InventoryAdjustment();
        //                        InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

        //                        RecordRef refAccount = new RecordRef();
        //                        refAccount.internalId = d.nas_accountNo.ToString();

        //                        RecordRef refSubsidiary = new RecordRef();
        //                        refSubsidiary.internalId = d.nas_subsidiaryID.ToString();


        //                        RecordRef refPostingPeriod = new RecordRef();
        //                        refPostingPeriod.internalId = d.nas_postingPeriodID.ToString();
                              
        //                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
        //                        RecordRef refBusinessChannel = new RecordRef();
        //                        refBusinessChannel.internalId = d.nas_businessChannelID.ToString();
        //                        StringCustomFieldRef scfr = new StringCustomFieldRef();
        //                        scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
        //                        scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;

        //                        if (d.nas_Type == "RCV" || d.nas_Type.ToUpper() == "RECEIVING")
        //                        {
        //                            scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RCV;
        //                        }
        //                        else if (d.nas_Type == "ADJ" || d.nas_Type.ToUpper() == "ADJUSTMENT")
        //                        {
        //                            scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_ADJ;
        //                        }
        //                        else if (d.nas_Type == "Write-off/Damage" || d.nas_Type.ToUpper() == "WRITEOFF")
        //                        {
        //                            scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_WRITEOFFDMG;
        //                        }
        //                        cfrList[0] = scfr;

        //                        invAdj.customFieldList = cfrList;
        //                        invAdj.account = refAccount;
        //                        invAdj.memo = d.nas_shipmentNo + " " + d.nas_firstShipmentDate.ToString() + " " + d.nas_businessChannel + " " + d.nas_locationID.ToString();
        //                        invAdj.tranDate = Convert.ToDateTime(d.nas_firstShipmentDate);
        //                        invAdj.tranDateSpecified = true;
        //                        invAdj.subsidiary = refSubsidiary;
        //                        invAdj.@class = refBusinessChannel;
        //                        invAdj.postingPeriod = refPostingPeriod;
        //                        invAdj.externalId = "Adjustment " + d.nas_ID.ToString();

        //                        externalID = invAdj.externalId;
                              
        //                        var directAdjItem = (from nai in entities.netsuite_adjustmentdetail2
        //                                             where nai.nad_shipmentNo == d.nas_shipmentNo &&
        //                                             nai.nad_firstShipmentDate == d.nas_firstShipmentDate &&
        //                                             nai.nad_type == d.nas_Type &&
        //                                             nai.nad_nsLocationID == d.nas_locationID &&
        //                                             nai.nad_businessChannel == d.nas_businessChannel &&
        //                                             nai.nad_postingPeriodID == d.nas_postingPeriodID
                                                     
        //                                             select nai).ToList();


        //                        if (directAdjItem.Count() > 0)
        //                        {
                                    
        //                            InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[directAdjItem.Count()];

        //                            foreach (var i in directAdjItem)
        //                            {
        //                                RecordRef refItem = new RecordRef();
        //                                refItem.internalId = i.nad_nsItemID.ToString();

        //                                RecordRef refLocation = new RecordRef();
        //                                refLocation.internalId = i.nad_nsLocationID.ToString();

        //                                InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

        //                                item.item = refItem;
        //                                item.location = refLocation;
        //                                item.adjustQtyBy = Double.Parse(i.nad_receivedQty.ToString());

        //                                //item.unitCost = Double.Parse(i.nad_localCost.ToString());
        //                                //item.currentValue = 1;

        //                                item.foreignCurrencyUnitCostSpecified = true;
        //                                item.foreignCurrencyUnitCost = Double.Parse(i.nad_localCost.ToString());

        //                                item.adjustQtyBySpecified = true;
        //                                item.memo = d.nas_analysisCode;
        //                                items[itemCount] = item;
        //                                itemCount++;
        //                            }
        //                            iail.inventory = items;
        //                            invAdj.inventoryList = iail;
        //                            invAdjList[daCount] = invAdj;

        //                            //rowCount = daCount + 1;
        //                            //var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
        //                            //    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-INVENTORY ADJUSTMENT', '" + d.da_directAdjID + "', '" + gjob_id.ToString() + "'," +
        //                            //    "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','')";
        //                            //this.DataFromNetsuiteLog.Debug(insertTask);
        //                            //entities.Database.ExecuteSqlCommand(insertTask);

        //                            daCount++;
        //                            status = true;
        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        errorMessage = ex.Message;
        //                        //this.DataFromNetsuiteLog.Error(ex.ToString());
        //                        status = false;
        //                    }
        //                }//end of directAdj

        //                if (status == true)
        //                {
        //                    login(service);
        //                    InventoryAdjustment[] divInvAdjList = new InventoryAdjustment[400];
                            
        //                    List<string> jobIDList = new List<string>();
                            
        //                    for (int i = 0; i < invAdjList.Length; i = i + 400)
        //                    {
                                
        //                        if (invAdjList.Length - i < 400)
        //                        {
        //                            AsyncStatusResult asyncRes = new AsyncStatusResult();
        //                            Array.Resize( ref divInvAdjList, invAdjList.Length - i);
        //                            Array.ConstrainedCopy(invAdjList, i, divInvAdjList, 0, invAdjList.Length - i);
        //                            string y = divInvAdjList.Length.ToString();
        //                            asyncRes = service.asyncAddList(divInvAdjList);
        //                            jobId = asyncRes.jobId;
        //                            jobIDList.Add(jobId);
                                    
        //                        }
        //                        else
        //                        {
        //                            AsyncStatusResult asyncRes = new AsyncStatusResult();
        //                            Array.ConstrainedCopy(invAdjList, i, divInvAdjList, 0, 400);
        //                            asyncRes = service.asyncAddList(divInvAdjList);
        //                            jobId = asyncRes.jobId;
        //                            jobIDList.Add(jobId);
        //                        }
        //                    }

        //                    addListTimer = new System.Timers.Timer(900000);
        //                    addListTimer.Elapsed += onTimedEvent;
        //                    addListTimer.Enabled = true;

        //                    while (addListTimer.Enabled == true)
        //                    {
        //                    }

                            
        //                }
        //                else
        //                {
        //                    errorMessage = "Data retrieving failed or no data for adjustment available in database";
        //                    throw new Exception(errorMessage);
        //                }
        //                ;
        //            }//end of sdeEntities
        //        }
        //        else
        //        {
        //            errorMessage = "Netsuite Login Failed";
        //            throw new Exception(errorMessage);
        //        }
        //    //}//end of scope1
        //    //TempData["Alert"] = "Alert message";    
        //    //return JavaScript("<script language='javascript' type='text/javascript'>alert('Data Already Exists');</script>");
        //    //return RedirectToAction("Index", "Dashboard");
        //    return Content("bah");
        //    //return View();
        //}

        //[HttpPost]
        //public ActionResult csvFileUpload(HttpPostedFileBase file)
        //{
        //    // Verify that the user selected a file
        //    if (file != null && file.ContentLength > 0)
        //    {
        //        // extract only the fielname
        //        var fileName = Path.GetFileName(file.FileName);
        //        // store the file inside ~/App_Data/uploads folder
        //        var path = Path.Combine(Server.MapPath("~/App_Data/"), fileName);
        //        file.SaveAs(path);
        //    }
        //    // redirect back to the index action to show the form once again
        //    return RedirectToAction("Adjustment");
        //}

        //[HttpPost]
        //public ActionResult csvToDB(string fileName)
        //{
        //    return RedirectToAction("Adjustment");
        //}

        //private static void onTimedEvent(Object source, ElapsedEventArgs e)
        //{
        //    login(service);

        //    foreach (string jobID in jobIDList)
        //    {
        //        AsyncStatusResult result = service.checkAsyncStatus(jobID);
        //        if (result.status == AsyncStatusType.pending || result.status == AsyncStatusType.processing)
        //        {
        //            return;
        //        }
        //    }

        //    addListTimer.Enabled = false;
        //}
        //public ActionResult Adjustment()
        //{
        //    return View();
        //}

        public ActionResult Logger()
        {
            return View();
        }

        public ActionResult testtab()
        {

            return View();
        }
    }
}