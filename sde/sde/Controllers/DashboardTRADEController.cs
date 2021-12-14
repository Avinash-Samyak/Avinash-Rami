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
using System.IO;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Configuration;         //ckkoh 20150115
using MySql.Data.MySqlClient;       //ckkoh 20150115

namespace sde.Controllers
{
    public class DashboardTradeController : Controller
    {
        //
        // GET: /Dashboard/
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");

        [Authorize(Roles = "TRADE")]
        public ActionResult Index()
        {

            using (sdeEntities entities = new sdeEntities())
            {
                DateTime lastXDate = DateTime.Now.AddDays(-14);
                #region Scheduler
                var scheduler1 = (from pro in entities.schedulers
                                  where pro.sche_transactionType.StartsWith("NS-") 
                                  && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
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
                                  where pro.sche_transactionType.StartsWith("SSA-") 
                                  && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
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
                                  where pro.sche_transactionType.StartsWith("MQPUSH-") 
                                  && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
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
                                  where pro.sche_transactionType.StartsWith("CPAS-") 
                                  && pro.sche_status == "ACTIVE" //&& pro.sche_nextRun <= DateTime.Now
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

                var allScheduler = scheduler1.Concat(scheduler2).Concat(scheduler3).ToList().Concat(scheduler4).ToList();
                SchedulerList scheList = new SchedulerList();
                scheList.scheduler = allScheduler;
                #endregion

                return View(new Tuple<sde.Models.SchedulerList>(scheList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SDEStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-3);
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
                                var requestnetsuite1 = (from n in entities.requestnetsuites
                                                        where n.rn_status == "START" && n.rn_rangeFrom >= fromDate && n.rn_rangeFrom <= toDate
                                                        select new cls_requestnetsuite()
                                                        {
                                                            transactionType = n.rn_sche_transactionType,
                                                            status = n.rn_status,
                                                            seqNo = n.rn_seqNo,
                                                            jobID = n.rn_jobID,
                                                            rangeFrom = n.rn_rangeFrom,
                                                            rangeTo = n.rn_rangeTo,
                                                            sequence = n.rn_sequence
                                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                                var requestnetsuite2 = (from n in entities.requestnetsuites
                                                        where n.rn_status == "FINISHED" && n.rn_rangeFrom >= fromDate && n.rn_rangeFrom <= toDate
                                                        select new cls_requestnetsuite()
                                                        {
                                                            transactionType = n.rn_sche_transactionType,
                                                            status = n.rn_status,
                                                            seqNo = n.rn_seqNo,
                                                            jobID = n.rn_jobID,
                                                            rangeFrom = n.rn_rangeFrom,
                                                            rangeTo = n.rn_rangeTo,
                                                            sequence = n.rn_sequence
                                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                                var requestnetsuite3 = (from n in entities.requestnetsuites
                                                        where n.rn_status == "UPLOADED" && n.rn_rangeFrom >= fromDate && n.rn_rangeFrom <= toDate
                                                        select new cls_requestnetsuite()
                                                        {
                                                            transactionType = n.rn_sche_transactionType,
                                                            status = n.rn_status,
                                                            seqNo = n.rn_seqNo,
                                                            jobID = n.rn_jobID,
                                                            rangeFrom = n.rn_rangeFrom,
                                                            rangeTo = n.rn_rangeTo,
                                                            sequence = n.rn_sequence
                                                        }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList();

                                var allRequestNetsuite = requestnetsuite1.Concat(requestnetsuite2).Concat(requestnetsuite3).ToList();
                */

                ////
                var requestnetsuite1 = (from n in entities.requestnetsuites
                                        where n.rn_rangeFrom >= fromDate && n.rn_rangeFrom <= toDate
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

                var allRequestNetsuite = requestnetsuite1.Concat(requestnetsuite2).OrderBy(y => y.status).ToList();
                RequestNetsuiteList rnList = new RequestNetsuiteList();
                rnList.requestnetsuite = allRequestNetsuite;
                return View(new Tuple<sde.Models.RequestNetsuiteList>(rnList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SDEStatistic(String tranType, String jobID, DateTime rangeTo, String tranPage)
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

        [Authorize(Roles = "TRADE")]
        public ActionResult MQStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-3);
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
                                  where n.rmq_rangeFrom >= fromDate && n.rmq_rangeFrom <= toDate
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

        public string getNetsuitePassword()
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

        public Boolean login(NetSuiteService service)
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
            //passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;       //"Netsuite01"
            passport.password = getNetsuitePassword();
            return service.login(passport).status.isSuccess;
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult TasksStatus(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Now;
                DateTime toDate = DateTime.Now;
                if (fromDate1 == null && toDate1 == null)
                {
                    fromDate = DateTime.Now.AddDays(-3);
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
                /*
                var requestmq2 = (from n in entities.requestmqs
                                    where n.rmq_status == "FINISHED" && n.rmq_rangeFrom >= fromDate && n.rmq_rangeFrom <= toDate
                                    select new cls_error()
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
                                    select new cls_error()
                                    {
                                        transactionType = n.rmq_sche_transactionType,
                                        status = n.rmq_status,
                                        seqNo = n.rmq_seqNo,
                                        jobID = n.rmq_jobID,
                                        rangeFrom = n.rmq_rangeFrom,
                                        rangeTo = n.rmq_rangeTo,
                                        sequence = n.rmq_sequence
                                    }).OrderBy(x => x.transactionType).OrderBy(y => y.rangeFrom).ToList */

                var allRequestError = requesterror.OrderBy(x => x.taskID).ToList();
                RequestErrorList errorList = new RequestErrorList();
                errorList.error = allRequestError;

                return View(new Tuple<sde.Models.RequestErrorList>(errorList));
            }
        }

        [Authorize(Roles = "TRADE")]
        [HttpPost, ActionName("ErrorRerun")]
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
                    Timeout = TimeSpan.FromSeconds(60)
                };

                if (rerunList != null)
                {
                    using (TransactionScope scope1 = new TransactionScope())
                    {
                        using (sdeEntities entities = new sdeEntities())
                        {
                            foreach (string rerun_job in rerunList)
                            {
                                string[] array_rerun_job = rerun_job.Split(';');
                                int taskId = Convert.ToInt32(array_rerun_job[0]);

                                string refNo = Convert.ToString(array_rerun_job[1]);
                                refNo = refNo.Substring((refNo.LastIndexOf('.') + 1), (refNo.Length - (refNo.LastIndexOf('.') + 1)));

                                string jobType = Convert.ToString(array_rerun_job[2]);
                                string rerun = "";

                                switch (jobType)
                                {
                                    case "SO":
                                        rerun = rerun_SO(service, entities, taskId);
                                        break;
                                    case "CPAS-ORDER ADJUSTMENT":
                                        rerun = rerun_cpas_order_adjustment(service, entities, refNo);
                                        break;
                                    case "CPAS-CANCELLATION ORDER":
                                        rerun = rerun_cpas_order_cancellation(service, entities, refNo);
                                        break;
                                    case "CPAS-JOURNAL":
                                        rerun = rerun_cpas_journal(service, entities, refNo);
                                        break;
                                    case "CPAS-SALES ORDER":
                                        rerun = rerun_cpas_sales_order(service, entities, refNo);
                                        break;
                                    default:
                                        break;
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

            return Json(Url.Action("TasksStatus", "Dashboard"));
        }

        private string rerun_SO(NetSuiteService service, sdeEntities entities, int taskId)
        {
            Boolean status = true;
            String errorMsg = "";

            var ordMaster = (from jom in entities.netsuite_jobordmaster
                             from rnt in entities.requestnetsuite_task
                             join jomp in entities.netsuite_jobordmaster_pack on jom.nsjom_jobOrdMaster_ID equals jomp.nsjomp_jobOrdMaster_ID
                             join josp in entities.wms_jobordscan_pack on jomp.nsjomp_jobOrdMaster_pack_ID equals josp.josp_pack_ID
                             where (jom.nsjom_jobOrdMaster_ID == rnt.rnt_refNO || jomp.nsjomp_jobOrdMaster_pack_ID == rnt.rnt_refNO.Substring(31, rnt.rnt_refNO.Length - 31))
                             where rnt.rnt_id == taskId
                             select new { jom.nsjom_moNo_internalID, jom.nsjom_moNo, jom.nsjom_jobOrdMaster_ID, jomp.nsjomp_jobOrdMaster_pack_ID }).Distinct().ToList();

            if (ordMaster.Count() > 0)
            {
                status = true;
            }
            else
            {
                status = false;
            }

            Int32 ordCount = 0;
            ItemFulfillment[] iffList = new ItemFulfillment[ordMaster.Count()];

            foreach (var order in ordMaster)
            {
                try
                {
                    InitializeRef refSO = new InitializeRef();
                    refSO.type = InitializeRefType.salesOrder;
                    refSO.internalId = order.nsjom_moNo_internalID;
                    refSO.typeSpecified = true;

                    InitializeRecord recSO = new InitializeRecord();
                    recSO.type = InitializeType.itemFulfillment;
                    recSO.reference = refSO;

                    ReadResponse rrSO = service.initialize(recSO);

                    Record rSO = rrSO.record;
                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                    ItemFulfillment iff2 = new ItemFulfillment();
                    ItemFulfillmentItemList ifitemlist = iff1.itemList;

                    RecordRef refCreatedFrom = new RecordRef();
                    refCreatedFrom.internalId = iff1.createdFrom.internalId;
                    iff2.createdFrom = refCreatedFrom;

                    ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];

                    var scanpack = (from josp in entities.wms_jobordscan_pack
                                    join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                    where josp.josp_pack_ID == order.nsjomp_jobOrdMaster_pack_ID //&& josp.josp_fulFillStatus == "N"
                                    select new { jomp.nsjomp_ordPack, jomp.nsjomp_item_internalID, josp.josp_jobOrdMaster_ID, josp.josp_pack_ID, josp.josp_ordFulFill, jomp.nsjomp_location_internalID }).ToList();

                    if (ifitems.Count() > 0)
                    {
                        String josp_packID = null;
                        for (int i = 0; i < ifitemlist.item.Length; i++)
                        {
                            ItemFulfillmentItem iffi = new ItemFulfillmentItem();

                            iffi.quantity = 0;
                            iffi.quantitySpecified = true;
                            iffi.itemIsFulfilled = false;
                            iffi.itemIsFulfilledSpecified = true;
                            iffi.orderLine = ifitemlist.item[i].orderLine;
                            iffi.orderLineSpecified = true;

                            foreach (var item in scanpack)
                            {
                                if (ifitemlist.item[i].item.internalId.Equals(item.nsjomp_item_internalID))
                                {
                                    josp_packID = item.josp_pack_ID;

                                    RecordRef refLocation = new RecordRef();
                                    refLocation.internalId = item.nsjomp_location_internalID;

                                    iffi.item = ifitemlist.item[i].item;
                                    iffi.location = refLocation;
                                    iffi.quantity = Convert.ToInt32(item.josp_ordFulFill);
                                    iffi.quantitySpecified = true;
                                    iffi.itemIsFulfilled = true;
                                    iffi.itemIsFulfilledSpecified = true;

                                    break;
                                }
                            }
                            ifitems[i] = iffi;
                        }

                        ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                        ifil1.item = ifitems;
                        iff2.itemList = ifil1;

                        iffList[ordCount] = iff2;
                        ordCount++;
                    }

                    WriteResponseList resList = service.addList(iffList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    status = false;
                    errorMsg = ex.InnerException.Message;
                }
            }

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        private string rerun_cpas_order_adjustment(NetSuiteService service, sdeEntities entities, string refNo)
        {
            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 daCount = 0;
            Int32 itemCount = 0;

            try
            {
                var groupQ1 = (from q1 in entities.cpas_stockposting
                               where q1.spl_transactionType == "ADJUSTMENT" && q1.spl_sp_id == refNo
                               select new
                               {
                                   id = q1.spl_sp_id,
                                   tranType = q1.spl_transactionType,
                                   subsidiary = q1.spl_subsidiary_internalID,
                                   businessChannel = q1.spl_mb_businessChannel_internalID,
                                   memo = q1.spl_sDesc,
                                   postingDate = q1.spl_postingDate
                               }).Distinct().ToList();

                InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];
                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                foreach (var q1 in groupQ1)
                {
                    InventoryAdjustment invAdj = new InventoryAdjustment();

                    RecordRef refAccount = new RecordRef();
                    refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_REPLACEMENT;
                    invAdj.account = refAccount;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
                    scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_REPLACEMENT;
                    cfrList[0] = scfr;
                    invAdj.customFieldList = cfrList;

                    RecordRef refSubsidiary = new RecordRef();
                    refSubsidiary.internalId = q1.subsidiary;
                    invAdj.subsidiary = refSubsidiary;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = q1.businessChannel;
                    invAdj.@class = refBusinessChannel;

                    invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                    invAdj.memo = q1.memo;

                    var ordAdj = (from o in entities.cpas_stockposting
                                  where o.spl_transactionType == "ADJUSTMENT" && o.spl_sp_id == q1.id
                                  select o).ToList();

                    var ordAdjItem = from p in ordAdj
                                     let k = new
                                     {
                                         item = p.spl_mi_item_internalID,
                                         tranType = p.spl_transactionType,
                                         loc = p.spl_ml_location_internalID,
                                         inOut = p.spl_inout
                                     }
                                     group p by k into g
                                     select new
                                     {
                                         item = g.Key.item,
                                         tranType = g.Key.tranType,
                                         loc = g.Key.loc,
                                         inout = g.Key.inOut,
                                         qty = g.Sum(p => p.spl_dQty)
                                     };

                    if (ordAdjItem.Count() > 0)
                    {
                        InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];

                        foreach (var i in ordAdjItem)
                        {
                            InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                            RecordRef refItem = new RecordRef();
                            refItem.internalId = i.item;
                            item.item = refItem;

                            RecordRef refLocation = new RecordRef();
                            refLocation.internalId = i.loc;
                            item.location = refLocation;

                            if (i.inout.Equals("I"))
                            {
                                item.adjustQtyBy = Convert.ToDouble(i.qty);
                            }
                            else
                            {
                                item.adjustQtyBy = -(Convert.ToDouble(i.qty));
                            }
                            item.adjustQtyBySpecified = true;

                            items[itemCount] = item;
                            itemCount++;
                        }

                        iail.inventory = items;
                        invAdj.inventoryList = iail;
                        invAdjList[daCount] = invAdj;

                        daCount++;
                        status = true;
                    }
                }

                if (status == true)
                {
                    WriteResponseList resList = service.addList(invAdjList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        private string rerun_cpas_order_cancellation(NetSuiteService service, sdeEntities entities, string refNo)
        {
            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 daCount = 0;
            Int32 itemCount = 0;

            try
            {
                var groupQ1 = (from q1 in entities.cpas_stockposting
                               where (q1.spl_transactionType == "RNCO" || q1.spl_transactionType == "RETN") && (q1.spl_sp_id == refNo)
                               select new
                               {
                                   id = q1.spl_sp_id,
                                   tranType = q1.spl_transactionType,
                                   subsidiary = q1.spl_subsidiary_internalID,
                                   businessChannel = q1.spl_mb_businessChannel_internalID,
                                   memo = q1.spl_sDesc,
                                   postingDate = q1.spl_postingDate
                               }).Distinct().ToList();

                InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];
                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                foreach (var q1 in groupQ1)
                {
                    InventoryAdjustment invAdj = new InventoryAdjustment();

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;

                    RecordRef refAccount = new RecordRef();
                    if (q1.tranType.Equals("RETN"))
                    {
                        refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_RETURN;
                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RETN;
                        cfrList[0] = scfr;
                    }
                    else if (q1.tranType.Equals("RNCO"))
                    {
                        refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_RETURN_CHARGEOFF;
                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RNCO;
                        cfrList[0] = scfr;
                    }
                    invAdj.account = refAccount;
                    invAdj.customFieldList = cfrList;

                    RecordRef refSubsidiary = new RecordRef();
                    refSubsidiary.internalId = q1.subsidiary;
                    invAdj.subsidiary = refSubsidiary;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = q1.businessChannel;
                    invAdj.@class = refBusinessChannel;

                    invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                    invAdj.memo = q1.memo;

                    var ordAdj = (from o in entities.cpas_stockposting
                                  where o.spl_transactionType == q1.tranType && o.spl_sp_id == q1.id
                                  select o).ToList();

                    var ordAdjItem = from p in ordAdj
                                     let k = new
                                     {
                                         item = p.spl_mi_item_internalID,
                                         tranType = p.spl_transactionType,
                                         loc = p.spl_ml_location_internalID,
                                         memo = p.spl_sDesc,
                                         postingDate = p.spl_postingDate
                                     }
                                     group p by k into g
                                     select new
                                     {
                                         item = g.Key.item,
                                         tranType = g.Key.tranType,
                                         loc = g.Key.loc,
                                         memo = g.Key.memo,
                                         postingDate = g.Key.postingDate,
                                         qty = g.Sum(p => p.spl_dQty)
                                     };

                    if (ordAdjItem.Count() > 0)
                    {
                        InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];

                        foreach (var i in ordAdjItem)
                        {
                            InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                            RecordRef refItem = new RecordRef();
                            refItem.internalId = i.item;

                            RecordRef refLocation = new RecordRef();
                            refLocation.internalId = i.loc;

                            item.item = refItem;
                            item.location = refLocation;
                            item.adjustQtyBy = Convert.ToDouble(i.qty);
                            item.adjustQtyBySpecified = true;
                            items[itemCount] = item;
                            itemCount++;
                        }
                        iail.inventory = items;
                        invAdj.inventoryList = iail;
                        invAdjList[daCount] = invAdj;

                        daCount++;
                        status = true;
                    }
                }

                if (status == true)
                {
                    WriteResponseList resList = service.addList(invAdjList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        private string rerun_cpas_journal(NetSuiteService service, sdeEntities entities, string refNo)
        {
            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 jnCount = 0;

            try
            {
                var journal = (from jn in entities.cpas_journal
                               where (jn.jn_journalID == refNo) && (jn.jn_tranType != "PRESALES")
                               select jn).ToList();

                var journalItem = from j in journal
                                  let k = new
                                  {
                                      tranType = j.jn_tranType,
                                      subsidiary = j.jn_subsidiary_internalID,
                                      postingDate = j.jn_postingDate
                                  }
                                  group j by k into g
                                  select new
                                  {
                                      tranType = g.Key.tranType,
                                      subsidiary = g.Key.subsidiary,
                                      postingDate = g.Key.postingDate
                                  };

                JournalEntry[] jeList = new JournalEntry[journalItem.Count()];
                foreach (var j in journalItem)
                {
                    try
                    {
                        Int32 lineCount = 0;
                        JournalEntry je = new JournalEntry();
                        JournalEntryLineList jell = new JournalEntryLineList();

                        je.tranDate = Convert.ToDateTime(j.postingDate);
                        je.tranDateSpecified = true;

                        RecordRef refSub = new RecordRef();
                        refSub.internalId = j.subsidiary;
                        je.subsidiary = refSub;

                        var journalLine = new List<cpas_journal>();
                        if (j.tranType.Equals("TOTAL UNSHIP"))
                        {
                            journalLine = (from jn in entities.cpas_journal
                                           where (jn.jn_tranType == j.tranType || jn.jn_tranType == "PRESALES") && jn.jn_subsidiary_internalID == j.subsidiary && jn.jn_postingDate == j.postingDate
                                           select jn).ToList();
                        }
                        else
                        {
                            journalLine = (from jn in entities.cpas_journal
                                           where jn.jn_tranType == j.tranType && jn.jn_subsidiary_internalID == j.subsidiary && jn.jn_postingDate == j.postingDate
                                           select jn).ToList();
                        }

                        if (journalLine.Count() > 0)
                        {
                            JournalEntryLine[] lines = new JournalEntryLine[journalLine.Count()];

                            foreach (var i in journalLine)
                            {
                                JournalEntryLine line = new JournalEntryLine();

                                RecordRef refAccount = new RecordRef();
                                refAccount.internalId = i.jn_account_internalID;
                                line.account = refAccount;

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = i.jn_businessChannel_internalID;
                                line.@class = refBusinessChannel;

                                if (i.jn_crAmount > 0)
                                {
                                    line.credit = Convert.ToDouble(i.jn_crAmount);
                                    line.creditSpecified = true;
                                }
                                else if (i.jn_drAmount > 0)
                                {
                                    line.debit = Convert.ToDouble(i.jn_drAmount);
                                    line.debitSpecified = true;
                                }
                                lines[lineCount] = line;
                                lineCount++;
                            }

                            jell.line = lines;
                            je.lineList = jell;
                            jeList[jnCount] = je;
                            jnCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        errorMsg = ex.ToString();
                        status = false;
                    }
                }

                if (status == true)
                {
                    WriteResponseList resList = service.addList(jeList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        private string rerun_cpas_sales_order(NetSuiteService service, sdeEntities entities, string refNo)
        {
            Boolean status = false;
            String errorMsg = "";

            Int32 soCount = 0;

            try
            {
                var cpasSalesGroup = (from c in entities.cpas_stockposting
                                      where (c.spl_transactionType == "SALES" || c.spl_transactionType == "UNSHIP")
                                      && (c.spl_sp_id == refNo)
                                      select new
                                      {
                                          id = c.spl_sp_id,
                                          tranType = c.spl_transactionType,
                                          subsidiary = c.spl_subsidiary_internalID,
                                          businessChannel = c.spl_mb_businessChannel_internalID,
                                          postingDate = c.spl_postingDate,
                                          memo = c.spl_sDesc
                                      }).Distinct().ToList();


                SalesOrder[] soList = new SalesOrder[cpasSalesGroup.Count()];

                foreach (var con in cpasSalesGroup)
                {
                    try
                    {
                        String salesNo = null;
                        SalesOrder so = new SalesOrder();

                        RecordRef refForm = new RecordRef();
                        switch (con.subsidiary)
                        {
                            case "3"://hard code
                                refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY;
                                so.customForm = refForm;
                                break;
                            case "5"://hard code
                                refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG;
                                so.customForm = refForm;
                                break;
                        }

                        RecordRef refTerm = new RecordRef();
                        refTerm.internalId = @Resource.ACCOUNTINGLIST_TERMS_INTERNALID;//default 60 days
                        so.terms = refTerm;

                        RecordRef refEntity = new RecordRef();
                        refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                        so.entity = refEntity;

                        so.tranDate = Convert.ToDateTime(con.postingDate);
                        so.tranDateSpecified = true;

                        so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;//put billed failed
                        so.orderStatusSpecified = true;

                        RecordRef refClass = new RecordRef();
                        refClass.internalId = con.businessChannel;
                        so.@class = refClass;

                        so.memo = con.memo;
                        salesNo = con.id;

                        var conItem = (from i in entities.cpas_stockposting
                                       where i.spl_sp_id == salesNo
                                       select i).ToList();

                        var conItemGroup = from p in conItem
                                           let k = new
                                           {
                                               item = p.spl_mi_item_internalID,
                                               location = p.spl_ml_location_internalID,
                                           }
                                           group p by k into g
                                           select new
                                           {
                                               item = g.Key.item,
                                               location = g.Key.location,
                                               qty = g.Sum(p => p.spl_dQty)
                                           };

                        SalesOrderItem[] soii = new SalesOrderItem[conItemGroup.Count()];
                        SalesOrderItemList soil = new SalesOrderItemList();

                        if (conItemGroup.Count() > 0)
                        {
                            int itemCount = 0;
                            foreach (var item in conItemGroup)
                            {
                                SalesOrderItem soi = new SalesOrderItem();

                                RecordRef refItem = new RecordRef();
                                refItem.type = RecordType.inventoryItem;
                                refItem.typeSpecified = true;
                                refItem.internalId = item.item;
                                soi.item = refItem;

                                soi.quantity = Convert.ToDouble(item.qty);
                                soi.quantitySpecified = true;

                                //ANET-28 Sales Order able to apply commit tag at item line.
                                soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                //soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                soi.commitInventorySpecified = true;

                                soi.amount = 0;
                                soi.amountSpecified = true;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = item.location;
                                soi.location = refLocation;

                                soii[itemCount] = soi;
                                itemCount++;
                            }
                            soil.item = soii;
                            so.itemList = soil;
                            soList[soCount] = so;

                            soCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        errorMsg = ex.ToString();
                        status = false;
                    }
                }

                if (status == true)
                {
                    WriteResponseList resList = service.addList(soList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }

                        if (result.status.isSuccess == true)
                        {
                            string rerun = rerun_cpas_sales_order_fulfill(service, entities, refNo, "ttt");
                            if (rerun != "")
                            {
                                errorMsg = rerun;
                                status = false;
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        private string rerun_cpas_sales_order_fulfill(NetSuiteService service, sdeEntities entities, string refNo, string netsuiteId)
        {
            Boolean status = false;
            String errorMsg = "";

            ////
            try
            {
                var location = (from q1 in entities.cpas_stockposting
                                where q1.spl_sp_id == refNo
                                select q1).ToList();

                var locationGroup1 = from p in location
                                     let k = new
                                     {
                                         id = p.spl_sp_id,
                                         location = p.spl_ml_location_internalID
                                     }
                                     group p by k into g
                                     select new
                                     {
                                         id = g.Key.id,
                                         location = g.Key.location,
                                     };

                ItemFulfillment[] iffList = new ItemFulfillment[locationGroup1.Count()];
                Int32 ordCount = 0;

                foreach (var l in locationGroup1)
                {
                    InitializeRef refSO = new InitializeRef();
                    refSO.type = InitializeRefType.salesOrder;
                    refSO.internalId = netsuiteId;
                    refSO.typeSpecified = true;

                    InitializeRecord recSO = new InitializeRecord();
                    recSO.type = InitializeType.itemFulfillment;
                    recSO.reference = refSO;

                    ReadResponse rrSO = service.initialize(recSO);
                    Record rSO = rrSO.record;

                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                    ItemFulfillment iff2 = new ItemFulfillment();
                    ItemFulfillmentItemList ifitemlist = iff1.itemList;

                    RecordRef refCreatedFrom = new RecordRef();
                    refCreatedFrom.internalId = iff1.createdFrom.internalId;
                    iff2.createdFrom = refCreatedFrom;

                    ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length]; //new ItemFulfillmentItem[itemList.Count()];
                    int count1 = 0;
                    for (int i = 0; i < ifitemlist.item.Length; i++)
                    {
                        ItemFulfillmentItem iffi = new ItemFulfillmentItem();

                        RecordRef refItem = new RecordRef();
                        iffi.item = ifitemlist.item[i].item;// refItem;

                        iffi.orderLine = ifitemlist.item[i].orderLine;
                        iffi.orderLineSpecified = true;

                        RecordRef refLocation = new RecordRef();

                        if (l.location.Equals(ifitemlist.item[i].location.internalId))
                        {
                            iffi.location = ifitemlist.item[i].location;//refLocation;

                            iffi.quantity = ifitemlist.item[i].quantityRemaining;// Convert.ToInt32(item.spl_dQty);
                            iffi.quantitySpecified = true;

                            iffi.itemIsFulfilled = true;
                            iffi.itemIsFulfilledSpecified = true;

                            ifitems[count1] = iffi;
                            count1++;
                        }
                        else
                        {
                            iffi.location = ifitemlist.item[i].location;//refLocation;

                            iffi.quantity = 0;
                            iffi.quantitySpecified = true;

                            iffi.itemIsFulfilled = false;
                            iffi.itemIsFulfilledSpecified = true;

                            ifitems[count1] = iffi;
                            count1++;
                        }
                    }
                    ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                    ifil1.item = ifitems;
                    iff2.itemList = ifil1;

                    iffList[ordCount] = iff2;
                    status = true;

                    if (status == true)
                    {
                        WriteResponse res = service.add(iffList[ordCount]);
                        if (res.status.isSuccess == false)
                        {
                            status = false;
                            if (res.status.statusDetail != null)
                            {
                                for (int j = 0; j < res.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + res.status.statusDetail[j].code + ": " + res.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }

                    ordCount++;
                }
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }

        [Authorize(Roles = "TRADE")]
        [HttpPost]
        public ActionResult UploadToNetsuite()
        {
            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                NetSuiteService service = new NetSuiteService();
                Boolean loginStatus = login(service);
                if (loginStatus == true)
                {
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 daCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        //var directAdj = (from da in entities.wms_directadjustment
                        //                 //where da.da_rangeTo == rangeTo
                        //                 select da).ToList();

                        var directAdj = (from nas in entities.netsuite_adjustment2
                                         //where nas.nas_ID == 4
                                         select nas).ToList();

                        //var directAdj = (from nai in entities.netsuite_adjustmentitem
                        //                 join nas in entities.netsuite_adjustment on nai.nai_shipmentNo equals nas.nas_shipmentNo
                        //                 select new { nas.nas_firstShipDate, nas.nas_analysisCode, nas.nas_receivedQty, nas.nas_localCost, nai.nai_nsLocationID, nai.nai_nsSubsidiaryID, nai.nai_nsitemInternalID, nai.nai_nsbusinessChannelID }).Distinct().ToList();

                        InventoryAdjustment[] invAdjList = new InventoryAdjustment[directAdj.Count()];

                        foreach (var d in directAdj)
                        {
                            try
                            {
                                Int32 itemCount = 0;
                                InventoryAdjustment invAdj = new InventoryAdjustment();
                                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                                RecordRef refAccount = new RecordRef();
                                refAccount.internalId = "159"; // hardcoded first

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = "3";

                                //RecordRef refPostingPeriod = new RecordRef();
                                //refPostingPeriod.internalId = "59";

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = d.nas_businessChannelID.ToString();
                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
                                scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RCV;
                                cfrList[0] = scfr;

                                invAdj.customFieldList = cfrList;
                                invAdj.account = refAccount;
                                invAdj.memo = d.nas_shipmentNo + " " + d.nas_shipmentDate.ToString();
                                invAdj.tranDate = Convert.ToDateTime(d.nas_firstShipmentDate);
                                invAdj.tranDateSpecified = true;
                                invAdj.subsidiary = refSubsidiary;
                                invAdj.@class = refBusinessChannel;
                                //invAdj.postingPeriod = refPostingPeriod;
                                //invAdj.createdDate = Convert.ToDateTime(d.nas_firstShipmentDate);
                                //invAdj.createdDate = Convert.ToDateTime(d.nas_firstShipmentDate);

                                //var directAdjItem = (from nai in entities.netsuite_adjustmentitem
                                //                     join nas in entities.netsuite_adjustment on nai.nai_shipmentNo equals nas.nas_shipmentNo
                                //                     select new { nas.nas_firstShipDate, nas.nas_analysisCode, nas.nas_receivedQty, nas.nas_localCost, nai.nai_nsLocationID, nai.nai_nsSubsidiaryID, nai.nai_nsitemInternalID, nai.nai_nsbusinessChannelID }).ToList();

                                var directAdjItem = (from nai in entities.netsuite_adjustmentdetail2
                                                     where nai.nad_shipmentNo == d.nas_shipmentNo &&
                                                     nai.nad_firstShipmentDate == d.nas_firstShipmentDate
                                                     select nai).ToList();


                                if (directAdjItem.Count() > 0)
                                {
                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[directAdjItem.Count()];

                                    foreach (var i in directAdjItem)
                                    {
                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = i.nad_nsItemID.ToString();

                                        RecordRef refLocation = new RecordRef();
                                        refLocation.internalId = i.nad_nsLocationID.ToString();

                                        InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                                        item.item = refItem;
                                        item.location = refLocation;
                                        item.adjustQtyBy = Double.Parse(i.nad_receivedQty.ToString());
                                        //item.unitCost = Double.Parse(i.nad_localCost.ToString());
                                        //item.currentValue = 1;
                                        item.foreignCurrencyUnitCostSpecified = true;
                                        item.foreignCurrencyUnitCost = Double.Parse(i.nad_localCost.ToString());

                                        item.adjustQtyBySpecified = true;
                                        item.memo = d.nas_analysisCode;
                                        items[itemCount] = item;
                                        itemCount++;
                                    }
                                    iail.inventory = items;
                                    invAdj.inventoryList = iail;
                                    invAdjList[daCount] = invAdj;

                                    //rowCount = daCount + 1;
                                    //var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                    //    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-INVENTORY ADJUSTMENT', '" + d.da_directAdjID + "', '" + gjob_id.ToString() + "'," +
                                    //    "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','')";
                                    //this.DataFromNetsuiteLog.Debug(insertTask);
                                    //entities.Database.ExecuteSqlCommand(insertTask);

                                    daCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                //this.DataFromNetsuiteLog.Error(ex.ToString());
                                status = false;
                            }
                        }//end of directAdj

                        if (status == true)
                        {
                            //job = service.asyncAddList(invAdjList);
                            WriteResponseList resList = service.addList(invAdjList);
                            WriteResponse[] res = resList.writeResponse; 
                            //    String jobID = job.jobId;

                            //    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                            //    this.DataFromNetsuiteLog.Debug(updateTask);
                            //    entities.Database.ExecuteSqlCommand(updateTask);

                            //    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                            //        "rn_updatedDate = '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' where rn_sche_transactionType = 'SSA-INVENTORY ADJUSTMENT' " +
                            //        "and rn_rangeTo = '" + rangeTo.ToString("yyyy-MM-dd HH:mm:ss") + "'";
                            //    this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                            //    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            scope1.Complete();
                        }

                    }//end of sdeEntities
                }
                else
                {

                }
            }//end of scope1
            //TempData["Alert"] = "Alert message";    
            //return JavaScript("<script language='javascript' type='text/javascript'>alert('Data Already Exists');</script>");
            //return RedirectToAction("Index", "Dashboard");
            return Content("bah");
        }

        //follow the existing wms table structure

        public string NetsuiteLookUp(string type, string imasData)
        {
            using (sdeEntities entities = new sdeEntities())
            {

            }
            return "test";
        }

        public ActionResult Adjustment()
        {
            return View();
        }

        public ActionResult Logger()
        {
            return View();
        }

        public ActionResult testtab()
        {
            return View();
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSync1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Sync > Filter by Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var soSync1 = (from s in entities.netsuite_syncso
                               join i in entities.map_item on s.nt2_itemID equals i.mi_item_isbn
                               where s.nt2_rangeTo > fromDate 
                               && s.nt2_rangeTo <= toDate 
                               && s.nt2_progressStatus != null
                               let k = new
                               {
                                   rangeTo = s.nt2_rangeTo,
                                   progressStatus = s.nt2_progressStatus,
                                   moNo = s.nt2_moNo,
                                   customer = s.nt2_customer,
                                   addressee = s.nt2_addressee,
                                   country = s.nt2_country
                               }
                               group s by k into g
                               select new cls_soSync()
                               {
                                   rangeTo = g.Key.rangeTo,
                                   progressStatus = g.Key.progressStatus,
                                   moNo = g.Key.moNo,
                                   customer = g.Key.customer,
                                   addressee = g.Key.addressee,
                                   country = g.Key.country,
                                   numOfItems = g.Count(),
                                   sum_ordQty = g.Sum(s => s.nt2_ordQty),
                                   sum_qtyForWMS = g.Sum(s => s.nt2_qtyForWMS),
                               }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo);
                
                var soSync1Count = soSync1.Count();
                ViewBag.soSync1Count = soSync1Count;

                soSyncList soS1List = new soSyncList();
                soS1List.soSync = soSync1.ToList();
                return View(new Tuple<sde.Models.soSyncList>(soS1List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSync2(String moNo) // SO Sync > Search by SO Number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var soSync2 = (from s in entities.netsuite_syncso
                               join i in entities.map_item on s.nt2_itemID equals i.mi_item_isbn
                               where s.nt2_moNo == moNo
                               select new cls_soSyncItem()
                               {
                                   rangeTo = s.nt2_rangeTo,
                                   progressStatus = s.nt2_progressStatus,
                                   moNo = s.nt2_moNo,
                                   customer = s.nt2_customer,
                                   addressee = s.nt2_addressee,
                                   country = s.nt2_country,
                                   itemID = i.mi_item_isbn,
                                   itemDescription = i.mi_item_description,
                                   ordQty = s.nt2_ordQty,
                                   qtyForWMS = s.nt2_qtyForWMS
                               }).OrderBy(x => x.itemID).ThenBy(x => x.rangeTo);
        
                soSyncItemList soS2List = new soSyncItemList();
                soS2List.soSyncItem = soSync2.ToList();
                return View(new Tuple<sde.Models.soSyncItemList>(soS2List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSyncItemView(String pass1, String pass2) // SO Sync > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String progStatPassed = pass1;
                String moNoPassed = pass2;
                var soSyncItem = (from s in entities.netsuite_syncso
                                  join i in entities.map_item on s.nt2_itemID equals i.mi_item_isbn
                                  where s.nt2_progressStatus == progStatPassed
                                  where s.nt2_moNo == moNoPassed
                                  select new cls_soSyncItem()
                                  {
                                      rangeTo = s.nt2_rangeTo,
                                      progressStatus = s.nt2_progressStatus,
                                      moNo = s.nt2_moNo,
                                      customer = s.nt2_customer,
                                      addressee = s.nt2_addressee,
                                      country = s.nt2_country,
                                      itemID = i.mi_item_isbn,
                                      itemDescription = i.mi_item_description,
                                      ordQty = s.nt2_ordQty,
                                      qtyForWMS = s.nt2_qtyForWMS
                                  }).OrderBy(x => x.itemID);

                soSyncItemList soSIVList = new soSyncItemList();
                soSIVList.soSyncItem = soSyncItem.ToList();
                return View(new Tuple<sde.Models.soSyncItemList>(soSIVList));
            }
        }

        // start ckkoh 20150115 - #604 Pending Sync - case on hold
        [Authorize(Roles = "TRADE")]
        public ActionResult SOPendingSync()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime toDate = DateTime.Today.AddHours(23).AddMinutes(59).AddSeconds(59);
                List<cls_soPendingSync> tempSoPSList = new List<cls_soPendingSync>();

                var soPendingSync = "SELECT so2.nt2_progressStatus, so1.nt1_rangeTo, so1.nt1_moNo, so1.nt1_customer, so2.nt2_addressee, so2.nt2_country, SUM(so1.nt1_ordQty) as nt1_ordQty, nt2_fulfilledQty, COUNT(*) as totalItems, " +
                                    "sum(so1.nt1_committedQty) as nt1_committedQty, " +
                    //"SELECT so1.nt1_seqID, so1.nt1_moNo,so1.nt1_moNo_internalID, so1.nt1_status,so1.nt1_itemID," +
                    //"so1.nt1_item_internalID, sum(so1.nt1_committedQty) as nt1_committedQty,sum(so1.nt1_fulfilledQty) as nt1_fulfilledQty," +
                    //"so1.nt1_rangeTo," +
                    //"if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) as nt2_qtyForWMS," +
                    //"if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty) as nt2_fulfilledQty," +tv
                                    "(SUM(so1.nt1_ordQty)) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) + if(so2.nt2_unfulfilledQty is null,0,so2.nt2_unfulfilledQty) ) as calc_qtyForWMS," +
                                    "((so1.nt1_committedQty + so1.nt1_fulfilledQty) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) + if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty))) as calc_difference " +
                    //"so1.nt1_ordQty, so1.nt1_tax, so1.nt1_discount, so1.nt1_rate, so1.nt1_amount," +
                    //"so1.nt1_customer, so1.nt1_customer_internalID, so1.nt1_SEIS_moNo,so1.nt1_SEIS_moNo_internalID, so1.nt1_subsidiary" +
                                    "FROM view_newso so1 " +
                                    "left join " +
                                    "(SELECT nt2_seqID, nt2_moNo_internalID, nt2_item_internalID, nt2_addressee, nt2_country, nt2_progressStatus" +
                                    ",sum(nt2_qtyForWMS) as nt2_qtyForWMS," +
                                    "sum(nt2_unfulfilledQty) as nt2_unfulfilledQty, " +
                                    "sum(nt2_fulfilledQty) as nt2_fulfilledQty " +
                                    "FROM netsuite_syncso " +
                                    "group by nt2_moNo_internalID,nt2_item_internalID) so2 " +
                                    "on so1.nt1_moNo_internalID = so2.nt2_moNo_internalID " +
                                    "and so1.nt1_item_internalID = so2.nt2_item_internalID " +
                                    "where so1.nt1_status in ('PENDING FULFILLMENT','PENDING BILLING/PARTIALLY FULFILLED','PARTIALLY FULFILLED') " +
                                    "and so1.nt1_subsidiary = '" + @Resource.SUBSIDIARY_NAME_MY + "' " +
                                    "and so1.nt1_synctowms = '1' " +
                    //"and so1.nt1_rangeTo <= '" + toDate + "' " +
                                    "group by so1.nt1_moNo_internalID, so1.nt1_item_internalID " +
                    //"group by so1.nt1_moNo_internalID " +
                                    "having (calc_qtyForWMS > 0) and nt1_ordQty>0;";

                #region no use
                /*
                var soPendingSync = "SELECT so2.nt2_progressStatus, so1.nt1_rangeTo, so1.nt1_moNo, so1.nt1_customer, so2.nt2_addressee, so2.nt2_country, SUM(so1.nt1_ordQty), nt2_fulfilledQty, COUNT(*) as totalItems, " +
                                    "sum(so1.nt1_committedQty) as nt1_committedQty, " +
                    //"SELECT so1.nt1_seqID, so1.nt1_moNo,so1.nt1_moNo_internalID, so1.nt1_status,so1.nt1_itemID," +
                    //"so1.nt1_item_internalID, sum(so1.nt1_committedQty) as nt1_committedQty,sum(so1.nt1_fulfilledQty) as nt1_fulfilledQty," +
                    //"so1.nt1_rangeTo," +
                    //"if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) as nt2_qtyForWMS," +
                    //"if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty) as nt2_fulfilledQty," +tv
                                    "(so1.nt1_committedQty) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) - (so1.nt1_fulfilledQty)) as calc_qtyForWMS," +
                                    "((so1.nt1_committedQty + so1.nt1_fulfilledQty) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) + if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty))) as calc_difference " +
                    //"so1.nt1_ordQty, so1.nt1_tax, so1.nt1_discount, so1.nt1_rate, so1.nt1_amount," +
                    //"so1.nt1_customer, so1.nt1_customer_internalID, so1.nt1_SEIS_moNo,so1.nt1_SEIS_moNo_internalID, so1.nt1_subsidiary" +
                                    "FROM view_newso so1 " +
                                    "left join " +
                                    "(SELECT nt2_seqID, nt2_moNo_internalID, nt2_item_internalID, nt2_addressee, nt2_country, nt2_progressStatus" +
                                    ",sum(nt2_qtyForWMS) as nt2_qtyForWMS," +
                                    "sum(nt2_fulfilledQty) as nt2_fulfilledQty " +
                                    "FROM netsuite_syncso " +
                                    "group by nt2_moNo_internalID,nt2_item_internalID) so2 " +
                                    "on so1.nt1_moNo_internalID = so2.nt2_moNo_internalID " +
                                    "and so1.nt1_item_internalID = so2.nt2_item_internalID " +
                                    "where so1.nt1_status in ('PENDING FULFILLMENT','PENDING BILLING/PARTIALLY FULFILLED','PARTIALLY FULFILLED') " +
                                    "and so1.nt1_subsidiary = '"+ @Resource.SUBSIDIARY_NAME_MY +"' " +
                                    "and so1.nt1_synctowms = '1' " +
                    //"and so1.nt1_rangeTo <= '" + toDate + "' " +
                                    "group by so1.nt1_moNo_internalID, so1.nt1_item_internalID " +
                    //"group by so1.nt1_moNo_internalID " +
                                    "having (calc_qtyForWMS > 0 or calc_difference > 0) and nt1_committedQty > 0;";                 
                 */
                #endregion

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(soPendingSync, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_soPendingSync soPending = new cls_soPendingSync();
                    soPending.progressStatus = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                    soPending.rangeTo = (dtr.GetValue(1) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(1);
                    soPending.moNo = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    soPending.customer = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    soPending.addressee = (dtr.GetValue(4) == DBNull.Value) ? String.Empty : dtr.GetString(4);
                    soPending.country = (dtr.GetValue(5) == DBNull.Value) ? String.Empty : dtr.GetString(5);
                    soPending.ordQty = (dtr.GetValue(6) == DBNull.Value) ? 0 : dtr.GetInt32(6);
                    soPending.qtyForWMS = (dtr.GetValue(7) == DBNull.Value) ? 0 : dtr.GetInt32(7);
                    soPending.totalItems = (dtr.GetValue(8) == DBNull.Value) ? String.Empty : dtr.GetString(8);

                    tempSoPSList.Add(soPending);
                }
                dtr.Close();
                cmd.Dispose();

                soPendingSyncList soPendSyncList = new soPendingSyncList();
                soPendSyncList.soPendingSync = tempSoPSList;
                return View(new Tuple<sde.Models.soPendingSyncList>(soPendSyncList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOPendingSyncItemView(String pass1, String pass2) // SO Pending Sync > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String progressStatusPassed = pass1;
                String moNoPassed = pass2;
                var soSyncItem = (from s in entities.netsuite_syncso
                                  join i in entities.map_item on s.nt2_itemID equals i.mi_item_isbn
                                  where s.nt2_progressStatus == progressStatusPassed
                                  where s.nt2_moNo == moNoPassed
                                  select new cls_soSyncItem()
                                  {
                                      rangeTo = s.nt2_rangeTo,
                                      progressStatus = s.nt2_progressStatus,
                                      moNo = s.nt2_moNo,
                                      customer = s.nt2_customer,
                                      addressee = s.nt2_addressee,
                                      country = s.nt2_country,
                                      itemID = i.mi_item_isbn,
                                      itemDescription = i.mi_item_description,
                                      ordQty = s.nt2_ordQty,
                                      qtyForWMS = s.nt2_qtyForWMS
                                  }).OrderBy(x => x.itemID);

                soSyncItemList soPSIVList = new soSyncItemList();
                soPSIVList.soSyncItem = soSyncItem.ToList();
                return View(new Tuple<sde.Models.soSyncItemList>(soPSIVList));
            }
        }
        // end ckkoh 20150115 - #604 case on hold

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSyncExport() // SO Sync > Export Menu
        {
            using (sdeEntities entities = new sdeEntities())
            {
                return View();
            }
        }


        [Authorize(Roles = "TRADE")]
        public ActionResult SOSyncExportProcessAM(String button, DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Sync > Export Process
        {
            if (button == "SOSync") 
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync
                {
                    List<cls_soSyncExport> eSOSList = new List<cls_soSyncExport>();
                    soSyncExportReport eSOSr = new soSyncExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(11);

                    var soSyncExport = (from ns in entities.netsuite_syncso
                                   join mi in entities.map_item on ns.nt2_itemID equals mi.mi_item_isbn
                                   join nj in entities.netsuite_jobmo on ns.nt2_moNo equals nj.nsjm_moNo
                                   where ns.nt2_rangeTo > fromDate && ns.nt2_rangeTo <= toDate && nj.nsjm_rangeTo > fromDate
                                   select new cls_soSyncExport()
                                   {
                                       Extract_Date = ns.nt2_rangeTo,
                                       SO_Number = nj.nsjm_moNo,
                                       ISBN = mi.mi_item_isbn,
                                       Item_Description = mi.mi_item_description,
                                       Quantity_For_WMS = ns.nt2_qtyForWMS,
                                       Customer_ID = nj.nsjm_schID,
                                       Customer_Name = nj.nsjm_schName,
                                       Country = nj.nsjm_country,
                                       DeliveryAdd = nj.nsjm_deliveryAdd,
                                       DeliveryAdd_2 = nj.nsjm_deliveryAdd_2,
                                       DeliveryAdd_3 = nj.nsjm_deliveryAdd_3,
                                       PostCode = nj.nsjm_postCode,
                                       Delivery_Type = nj.nsjm_deliveryType,
                                       Contact_Person = nj.nsjm_contactPerson,
                                       TelNo = nj.nsjm_telNo
                                   }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);

                    if (soSyncExport.Count() != 0)
                    {
                        foreach (var r in soSyncExport)
                        {
                            cls_soSyncExport soS = new cls_soSyncExport();
                            soS.Extract_Date = r.Extract_Date;
                            soS.SO_Number = r.SO_Number;
                            soS.ISBN = r.ISBN;
                            soS.Item_Description = r.Item_Description;
                            soS.Quantity_For_WMS = r.Quantity_For_WMS;
                            soS.Customer_ID = r.Customer_ID;
                            soS.Customer_Name = r.Customer_Name;
                            soS.Country = r.Country;
                            soS.DeliveryAdd = r.DeliveryAdd;
                            soS.DeliveryAdd_2 = r.DeliveryAdd_2;
                            soS.DeliveryAdd_3 = r.DeliveryAdd_3;
                            soS.PostCode = r.PostCode;
                            soS.Delivery_Type = r.Delivery_Type;
                            soS.Contact_Person = r.Contact_Person;
                            soS.TelNo = r.TelNo;

                            eSOSList.Add(soS);
                        }
                        eSOSr.soSEr = eSOSList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSr.soSEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "AM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }
            }
            else if (button == "SOSyncForwarder")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Forwarder
                {
                    List<cls_soSyncForwarderExport> eSOSFList = new List<cls_soSyncForwarderExport>();
                    soSyncForwarderExportReport eSOSFr = new soSyncForwarderExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(11);

                    var soSyncForwarderExport = (from j in entities.netsuite_jobmo_address
                                                 where j.nsjma_rangeTo > fromDate && j.nsjma_rangeTo <= toDate
                                                 select new cls_soSyncForwarderExport()
                                                 {
                                                     Extract_Date = j.nsjma_rangeTo,
                                                     SO_Number = j.nsjma_moNo,
                                                     Internal_ID = j.nsjma_moNo_internalID,
                                                     Customer_Name = j.nsjma_jobMoAddress_name,
                                                     Address_1 = j.nsjma_jobMoAddress_1,
                                                     Address_2 = j.nsjma_jobMoAddress_2,
                                                     Address_3 = j.nsjma_jobMoAddress_3,
                                                     Address_4 = j.nsjma_jobMoAddress_4,
                                                     DeliveryType = j.nsjma_jobMoAddress_deliveryType,
                                                     Tag = j.nsjma_jobMoAddress_tag,
                                                     Contact = j.nsjma_jobMoAddress_contact,
                                                     Tel = j.nsjma_jobMoAddress_tel,
                                                     Tel2 = j.nsjma_jobMoAddress_tel2,
                                                     Fax = j.nsjma_jobMoAddress_fax
                                                 }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number);

                    if (soSyncForwarderExport.Count() != 0)
                    {
                        foreach (var r in soSyncForwarderExport)
                        {
                            cls_soSyncForwarderExport soSF = new cls_soSyncForwarderExport();
                            soSF.Extract_Date = r.Extract_Date;
                            soSF.SO_Number = r.SO_Number;
                            soSF.Internal_ID = r.Internal_ID;
                            soSF.Customer_Name = r.Customer_Name;
                            soSF.Address_1 = r.Address_1;
                            soSF.Address_2 = r.Address_2;
                            soSF.Address_3 = r.Address_3;
                            soSF.Address_4 = r.Address_4;
                            soSF.DeliveryType = r.DeliveryType;
                            soSF.Tag = r.Tag;
                            soSF.Contact = r.Contact;
                            soSF.Tel = r.Tel;
                            soSF.Tel2 = r.Tel2;
                            soSF.Fax = r.Fax;

                            eSOSFList.Add(soSF);
                        }
                        eSOSFr.soSFEr = eSOSFList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSFr.soSFEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Forwarder_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "AM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }

            }
            else if (button == "SOSyncPrice")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Price
                {
                    List<cls_soSyncPriceExport> eSOSPList = new List<cls_soSyncPriceExport>();
                    soSyncPriceExportReport eSOSPr = new soSyncPriceExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(11);            

                    var soSyncPriceExport = (from njp in entities.netsuite_jobordmaster_pack
                                             join mi in entities.map_item on njp.nsjomp_ordPack equals mi.mi_item_isbn
                                             where njp.nsjomp_rangeTo > fromDate && njp.nsjomp_rangeTo <= toDate
                                             select new cls_soSyncPriceExport()
                                             {
                                                 Extract_Date = njp.nsjomp_rangeTo,
                                                 SO_Number = njp.nsjomp_moNo,
                                                 ISBN = mi.mi_item_isbn,
                                                 Item_Description = mi.mi_item_description,
                                                 Order_Quantity = njp.nsjomp_ordQty,
                                                 Order_Rate = njp.nsjomp_ordRate,
                                                 Tax = njp.nsjomp_tax,
                                                 Discount = njp.nsjomp_discount
                                             }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);
                    if (soSyncPriceExport.Count() != 0)
                    {
                        foreach (var r in soSyncPriceExport)
                        {
                            cls_soSyncPriceExport soSP = new cls_soSyncPriceExport();
                            soSP.Extract_Date = r.Extract_Date;
                            soSP.SO_Number = r.SO_Number;
                            soSP.ISBN = r.ISBN;
                            soSP.Item_Description = r.Item_Description;
                            soSP.Order_Quantity = r.Order_Quantity;
                            soSP.Order_Rate = r.Order_Rate;
                            soSP.Tax = r.Tax;
                            soSP.Discount = r.Discount;

                            eSOSPList.Add(soSP);
                        }
                        eSOSPr.soSPEr = eSOSPList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSPr.soSPEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Price_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "AM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();
                }

            }
            return RedirectToAction("SOSync1", "dashboardTRADE");
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSyncExportProcessPM(String button, DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Sync > Export Process
        {
            if (button == "SOSync")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync
                {
                    List<cls_soSyncExport> eSOSList = new List<cls_soSyncExport>();
                    soSyncExportReport eSOSr = new soSyncExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = toDate.AddHours(11);  //fromDate.AddDays(-1).AddHours(6);
                    toDate = toDate.AddHours(23);

                    var soSyncExport = (from ns in entities.netsuite_syncso
                                        join mi in entities.map_item on ns.nt2_itemID equals mi.mi_item_isbn
                                        join nj in entities.netsuite_jobmo on ns.nt2_moNo equals nj.nsjm_moNo
                                        where ns.nt2_rangeTo > fromDate && ns.nt2_rangeTo <= toDate && nj.nsjm_rangeTo > fromDate
                                        select new cls_soSyncExport()
                                        {
                                            Extract_Date = ns.nt2_rangeTo,
                                            SO_Number = nj.nsjm_moNo,
                                            ISBN = mi.mi_item_isbn,
                                            Item_Description = mi.mi_item_description,
                                            Quantity_For_WMS = ns.nt2_qtyForWMS,
                                            Customer_ID = nj.nsjm_schID,
                                            Customer_Name = nj.nsjm_schName,
                                            Country = nj.nsjm_country,
                                            DeliveryAdd = nj.nsjm_deliveryAdd,
                                            DeliveryAdd_2 = nj.nsjm_deliveryAdd_2,
                                            DeliveryAdd_3 = nj.nsjm_deliveryAdd_3,
                                            PostCode = nj.nsjm_postCode,
                                            Delivery_Type = nj.nsjm_deliveryType,
                                            Contact_Person = nj.nsjm_contactPerson,
                                            TelNo = nj.nsjm_telNo
                                        }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);

                    if (soSyncExport.Count() != 0)
                    {
                        foreach (var r in soSyncExport)
                        {
                            cls_soSyncExport soS = new cls_soSyncExport();
                            soS.Extract_Date = r.Extract_Date;
                            soS.SO_Number = r.SO_Number;
                            soS.ISBN = r.ISBN;
                            soS.Item_Description = r.Item_Description;
                            soS.Quantity_For_WMS = r.Quantity_For_WMS;
                            soS.Customer_ID = r.Customer_ID;
                            soS.Customer_Name = r.Customer_Name;
                            soS.Country = r.Country;
                            soS.DeliveryAdd = r.DeliveryAdd;
                            soS.DeliveryAdd_2 = r.DeliveryAdd_2;
                            soS.DeliveryAdd_3 = r.DeliveryAdd_3;
                            soS.PostCode = r.PostCode;
                            soS.Delivery_Type = r.Delivery_Type;
                            soS.Contact_Person = r.Contact_Person;
                            soS.TelNo = r.TelNo;

                            eSOSList.Add(soS);
                        }
                        eSOSr.soSEr = eSOSList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSr.soSEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "PM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }
            }
            else if (button == "SOSyncForwarder")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Forwarder
                {
                    List<cls_soSyncForwarderExport> eSOSFList = new List<cls_soSyncForwarderExport>();
                    soSyncForwarderExportReport eSOSFr = new soSyncForwarderExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = toDate.AddHours(11);  //fromDate.AddDays(-1).AddHours(6);
                    toDate = toDate.AddHours(23);

                    var soSyncForwarderExport = (from j in entities.netsuite_jobmo_address
                                                 where j.nsjma_rangeTo > fromDate && j.nsjma_rangeTo <= toDate
                                                 select new cls_soSyncForwarderExport()
                                                 {
                                                     Extract_Date = j.nsjma_rangeTo,
                                                     SO_Number = j.nsjma_moNo,
                                                     Internal_ID = j.nsjma_moNo_internalID,
                                                     Customer_Name = j.nsjma_jobMoAddress_name,
                                                     Address_1 = j.nsjma_jobMoAddress_1,
                                                     Address_2 = j.nsjma_jobMoAddress_2,
                                                     Address_3 = j.nsjma_jobMoAddress_3,
                                                     Address_4 = j.nsjma_jobMoAddress_4,
                                                     DeliveryType = j.nsjma_jobMoAddress_deliveryType,
                                                     Tag = j.nsjma_jobMoAddress_tag,
                                                     Contact = j.nsjma_jobMoAddress_contact,
                                                     Tel = j.nsjma_jobMoAddress_tel,
                                                     Tel2 = j.nsjma_jobMoAddress_tel2,
                                                     Fax = j.nsjma_jobMoAddress_fax
                                                 }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number);

                    if (soSyncForwarderExport.Count() != 0)
                    {
                        foreach (var r in soSyncForwarderExport)
                        {
                            cls_soSyncForwarderExport soSF = new cls_soSyncForwarderExport();
                            soSF.Extract_Date = r.Extract_Date;
                            soSF.SO_Number = r.SO_Number;
                            soSF.Internal_ID = r.Internal_ID;
                            soSF.Customer_Name = r.Customer_Name;
                            soSF.Address_1 = r.Address_1;
                            soSF.Address_2 = r.Address_2;
                            soSF.Address_3 = r.Address_3;
                            soSF.Address_4 = r.Address_4;
                            soSF.DeliveryType = r.DeliveryType;
                            soSF.Tag = r.Tag;
                            soSF.Contact = r.Contact;
                            soSF.Tel = r.Tel;
                            soSF.Tel2 = r.Tel2;
                            soSF.Fax = r.Fax;

                            eSOSFList.Add(soSF);
                        }
                        eSOSFr.soSFEr = eSOSFList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSFr.soSFEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Forwarder_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "PM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }

            }
            else if (button == "SOSyncPrice")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Price
                {
                    List<cls_soSyncPriceExport> eSOSPList = new List<cls_soSyncPriceExport>();
                    soSyncPriceExportReport eSOSPr = new soSyncPriceExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = toDate.AddHours(11);  //fromDate.AddDays(-1).AddHours(6);
                    toDate = toDate.AddHours(23);

                    var soSyncPriceExport = (from njp in entities.netsuite_jobordmaster_pack
                                             join mi in entities.map_item on njp.nsjomp_ordPack equals mi.mi_item_isbn
                                             where njp.nsjomp_rangeTo > fromDate && njp.nsjomp_rangeTo <= toDate
                                             select new cls_soSyncPriceExport()
                                             {
                                                 Extract_Date = njp.nsjomp_rangeTo,
                                                 SO_Number = njp.nsjomp_moNo,
                                                 ISBN = mi.mi_item_isbn,
                                                 Item_Description = mi.mi_item_description,
                                                 Order_Quantity = njp.nsjomp_ordQty,
                                                 Order_Rate = njp.nsjomp_ordRate,
                                                 Tax = njp.nsjomp_tax,
                                                 Discount = njp.nsjomp_discount
                                             }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);
                    if (soSyncPriceExport.Count() != 0)
                    {
                        foreach (var r in soSyncPriceExport)
                        {
                            cls_soSyncPriceExport soSP = new cls_soSyncPriceExport();
                            soSP.Extract_Date = r.Extract_Date;
                            soSP.SO_Number = r.SO_Number;
                            soSP.ISBN = r.ISBN;
                            soSP.Item_Description = r.Item_Description;
                            soSP.Order_Quantity = r.Order_Quantity;
                            soSP.Order_Rate = r.Order_Rate;
                            soSP.Tax = r.Tax;
                            soSP.Discount = r.Discount;

                            eSOSPList.Add(soSP);
                        }
                        eSOSPr.soSPEr = eSOSPList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSPr.soSPEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Price_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "PM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();
                }

            }
            return RedirectToAction("SOSync1", "dashboardTRADE");
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOSyncExportProcess(String button, DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Sync > Export Process
        {
            if (button == "SOSync")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync
                {
                    List<cls_soSyncExport> eSOSList = new List<cls_soSyncExport>();
                    soSyncExportReport eSOSr = new soSyncExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(23);

                    var soSyncExport = (from ns in entities.netsuite_syncso
                                        join mi in entities.map_item on ns.nt2_itemID equals mi.mi_item_isbn
                                        join nj in entities.netsuite_jobmo on ns.nt2_moNo equals nj.nsjm_moNo
                                        where ns.nt2_rangeTo > fromDate && ns.nt2_rangeTo <= toDate && nj.nsjm_rangeTo > fromDate
                                          && nj.nsjm_rangeTo <= toDate
                                        select new cls_soSyncExport()
                                        {
                                            Extract_Date = ns.nt2_rangeTo,
                                            SO_Number = nj.nsjm_moNo,
                                            ISBN = mi.mi_item_isbn,
                                            Item_Description = mi.mi_item_description,
                                            Quantity_For_WMS = ns.nt2_qtyForWMS,
                                            Customer_ID = nj.nsjm_schID,
                                            Customer_Name = nj.nsjm_schName,
                                            Country = nj.nsjm_country,
                                            DeliveryAdd = nj.nsjm_deliveryAdd,
                                            DeliveryAdd_2 = nj.nsjm_deliveryAdd_2,
                                            DeliveryAdd_3 = nj.nsjm_deliveryAdd_3,
                                            PostCode = nj.nsjm_postCode,
                                            Delivery_Type = nj.nsjm_deliveryType,
                                            Contact_Person = nj.nsjm_contactPerson,
                                            TelNo = nj.nsjm_telNo
                                        }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);

                    if (soSyncExport.Count() != 0)
                    {
                        foreach (var r in soSyncExport)
                        {
                            cls_soSyncExport soS = new cls_soSyncExport();
                            soS.Extract_Date = r.Extract_Date;
                            soS.SO_Number = r.SO_Number;
                            soS.ISBN = r.ISBN;
                            soS.Item_Description = r.Item_Description;
                            soS.Quantity_For_WMS = r.Quantity_For_WMS;
                            soS.Customer_ID = r.Customer_ID;
                            soS.Customer_Name = r.Customer_Name;
                            soS.Country = r.Country;
                            soS.DeliveryAdd = r.DeliveryAdd;
                            soS.DeliveryAdd_2 = r.DeliveryAdd_2;
                            soS.DeliveryAdd_3 = r.DeliveryAdd_3;
                            soS.PostCode = r.PostCode;
                            soS.Delivery_Type = r.Delivery_Type;
                            soS.Contact_Person = r.Contact_Person;
                            soS.TelNo = r.TelNo;

                            eSOSList.Add(soS);
                        }
                        eSOSr.soSEr = eSOSList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSr.soSEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + ".xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }
            }
            else if (button == "SOSyncForwarder")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Forwarder
                {
                    List<cls_soSyncForwarderExport> eSOSFList = new List<cls_soSyncForwarderExport>();
                    soSyncForwarderExportReport eSOSFr = new soSyncForwarderExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(11);

                    var soSyncForwarderExport = (from j in entities.netsuite_jobmo_address
                                                 where j.nsjma_rangeTo > fromDate && j.nsjma_rangeTo <= toDate
                                                 select new cls_soSyncForwarderExport()
                                                 {
                                                     Extract_Date = j.nsjma_rangeTo,
                                                     SO_Number = j.nsjma_moNo,
                                                     Internal_ID = j.nsjma_moNo_internalID,
                                                     Customer_Name = j.nsjma_jobMoAddress_name,
                                                     Address_1 = j.nsjma_jobMoAddress_1,
                                                     Address_2 = j.nsjma_jobMoAddress_2,
                                                     Address_3 = j.nsjma_jobMoAddress_3,
                                                     Address_4 = j.nsjma_jobMoAddress_4,
                                                     DeliveryType = j.nsjma_jobMoAddress_deliveryType,
                                                     Tag = j.nsjma_jobMoAddress_tag,
                                                     Contact = j.nsjma_jobMoAddress_contact,
                                                     Tel = j.nsjma_jobMoAddress_tel,
                                                     Tel2 = j.nsjma_jobMoAddress_tel2,
                                                     Fax = j.nsjma_jobMoAddress_fax
                                                 }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number);

                    if (soSyncForwarderExport.Count() != 0)
                    {
                        foreach (var r in soSyncForwarderExport)
                        {
                            cls_soSyncForwarderExport soSF = new cls_soSyncForwarderExport();
                            soSF.Extract_Date = r.Extract_Date;
                            soSF.SO_Number = r.SO_Number;
                            soSF.Internal_ID = r.Internal_ID;
                            soSF.Customer_Name = r.Customer_Name;
                            soSF.Address_1 = r.Address_1;
                            soSF.Address_2 = r.Address_2;
                            soSF.Address_3 = r.Address_3;
                            soSF.Address_4 = r.Address_4;
                            soSF.DeliveryType = r.DeliveryType;
                            soSF.Tag = r.Tag;
                            soSF.Contact = r.Contact;
                            soSF.Tel = r.Tel;
                            soSF.Tel2 = r.Tel2;
                            soSF.Fax = r.Fax;

                            eSOSFList.Add(soSF);
                        }
                        eSOSFr.soSFEr = eSOSFList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSFr.soSFEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Forwarder_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "AM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();

                }

            }
            else if (button == "SOSyncPrice")
            {
                using (sdeEntities entities = new sdeEntities()) // SO Sync To WMS > Export > SO Sync Price
                {
                    List<cls_soSyncPriceExport> eSOSPList = new List<cls_soSyncPriceExport>();
                    soSyncPriceExportReport eSOSPr = new soSyncPriceExportReport();

                    DateTime fromDate = DateTime.Today;
                    DateTime toDate = DateTime.Today;
                    if (toDate1 == null)
                    {
                        fromDate = DateTime.Today;
                        toDate = DateTime.Today;
                    }
                    else
                    {
                        fromDate = Convert.ToDateTime(fromDate1);
                        toDate = Convert.ToDateTime(toDate1);
                    }

                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;

                    DateTime displayfromDate = fromDate;
                    DateTime displaytoDate = toDate;

                    fromDate = fromDate.AddDays(-1).AddHours(23);
                    toDate = toDate.AddHours(11);

                    var soSyncPriceExport = (from njp in entities.netsuite_jobordmaster_pack
                                             join mi in entities.map_item on njp.nsjomp_ordPack equals mi.mi_item_isbn
                                             where njp.nsjomp_rangeTo > fromDate && njp.nsjomp_rangeTo <= toDate
                                             select new cls_soSyncPriceExport()
                                             {
                                                 Extract_Date = njp.nsjomp_rangeTo,
                                                 SO_Number = njp.nsjomp_moNo,
                                                 ISBN = mi.mi_item_isbn,
                                                 Item_Description = mi.mi_item_description,
                                                 Order_Quantity = njp.nsjomp_ordQty,
                                                 Order_Rate = njp.nsjomp_ordRate,
                                                 Tax = njp.nsjomp_tax,
                                                 Discount = njp.nsjomp_discount
                                             }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.SO_Number).ThenBy(x => x.ISBN);
                    if (soSyncPriceExport.Count() != 0)
                    {
                        foreach (var r in soSyncPriceExport)
                        {
                            cls_soSyncPriceExport soSP = new cls_soSyncPriceExport();
                            soSP.Extract_Date = r.Extract_Date;
                            soSP.SO_Number = r.SO_Number;
                            soSP.ISBN = r.ISBN;
                            soSP.Item_Description = r.Item_Description;
                            soSP.Order_Quantity = r.Order_Quantity;
                            soSP.Order_Rate = r.Order_Rate;
                            soSP.Tax = r.Tax;
                            soSP.Discount = r.Discount;

                            eSOSPList.Add(soSP);
                        }
                        eSOSPr.soSPEr = eSOSPList;
                    }
                    else
                    {
                        return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                    }

                    GridView gv = new GridView();
                    gv.DataSource = eSOSPr.soSPEr;
                    gv.DataBind();
                    Response.ClearContent();
                    Response.Buffer = true;
                    Response.AddHeader("content-disposition", "attachment; filename=" + "SO_Sync_Price_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + "AM.xls");
                    Response.ContentType = "application/vnd.ms-excel";
                    Response.Charset = "";
                    StringWriter sw = new StringWriter();
                    HtmlTextWriter htw = new HtmlTextWriter(sw);
                    gv.RenderControl(htw);
                    Response.Output.Write(sw.ToString());
                    Response.Flush();
                    Response.End();
                }

            }
            return RedirectToAction("SOSync1", "dashboardTRADE");
        }



        [Authorize(Roles = "TRADE")]
        public ActionResult POSync1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // PO Sync > Filter by Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(6);
                //toDate = toDate.AddHours(6);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var poSync1 = (from np in entities.netsuite_pr
                               join npi in entities.netsuite_pritem on np.nspr_pr_ID equals npi.nspi_nspr_pr_ID
                               join mi in entities.map_item on npi.nspi_item_ID equals mi.mi_item_isbn
                               where np.nspr_createdDate > fromDate 
                               && np.nspr_createdDate <= toDate
                               select new
                               {
                                   pr_ID = np.nspr_pr_ID,
                                   rangeTo = np.nspr_rangeTo,
                                   pr_number = np.nspr_pr_number,
                                   pr_supplier = np.nspr_pr_supplier,
                                   pr_location = np.nspr_pr_location,
                                   item_ID = npi.nspi_item_ID,
                                   pritem_qty = npi.nspi_pritem_qty,
                                   pritem_price = npi.nspi_pritem_price
                               }).ToList();

                var groupQ2 = from p in poSync1 
                              let k = new
                                {
                                   pr_ID = p.pr_ID,
                                   rangeTo = p.rangeTo,
                                   pr_number = p.pr_number,
                                   pr_supplier = p.pr_supplier,
                                   pr_location = p.pr_location,
                                }
                                group p by k into g
                                select new cls_poSync()
                                {
                                   pr_ID = g.Key.pr_ID,
                                   rangeTo = g.Key.rangeTo,
                                   pr_number = g.Key.pr_number,
                                   pr_supplier = g.Key.pr_supplier,
                                   pr_location = g.Key.pr_location,
                                   numOfItems = g.Count(),
                                   sum_pritemQty = g.Sum(p => p.pritem_qty),
                                   sum_pritemPrice = g.Sum(p => p.pritem_price)
                                };

                var poSync1Count = groupQ2.Count();
                ViewBag.poSync1Count = poSync1Count;

                poSyncList poS1List = new poSyncList();
                poS1List.poSync = groupQ2.OrderByDescending(x => x.rangeTo).ThenBy(x => x.pr_number).ToList();
                return View(new Tuple<sde.Models.poSyncList>(poS1List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POSync2(String pr_number) // PO Sync > Search by PO Number 
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String pr_numberPassed = pr_number;
                
                var poSync2 = (from np in entities.netsuite_pr
                               join npi in entities.netsuite_pritem on np.nspr_pr_ID equals npi.nspi_nspr_pr_ID
                               join mi in entities.map_item on npi.nspi_item_ID equals mi.mi_item_isbn
                               where np.nspr_pr_number == pr_numberPassed
                               select new cls_poSyncItem()
                               {
                                   pr_ID = np.nspr_pr_ID,
                                   pr_number = np.nspr_pr_number,
                                   pr_supplier = np.nspr_pr_supplier,
                                   pr_location = np.nspr_pr_location,
                                   rangeTo = np.nspr_rangeTo,
                                   item_ID = mi.mi_item_isbn,
                                   item_description = mi.mi_item_description,
                                   pritem_qty = npi.nspi_pritem_qty,
                                   pritem_price = npi.nspi_pritem_price
                               }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.pr_number);

                poSyncItemList poS2List = new poSyncItemList();
                poS2List.poSyncItem = poSync2.ToList();
                return View(new Tuple<sde.Models.poSyncItemList>(poS2List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POSyncItemView(String pass1)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String prNumberPassed = pass1;
                var poSyncItem = (from np in entities.netsuite_pr
                                  join npi in entities.netsuite_pritem on np.nspr_pr_ID equals npi.nspi_nspr_pr_ID
                                  join mi in entities.map_item on npi.nspi_item_ID equals mi.mi_item_isbn
                                  where np.nspr_pr_number == prNumberPassed
                                  select new cls_poSyncItem()
                                  {
                                      pr_ID = np.nspr_pr_ID,
                                      pr_number = np.nspr_pr_number,
                                      pr_supplier = np.nspr_pr_supplier,
                                      pr_location = np.nspr_pr_location,
                                      rangeTo = np.nspr_rangeTo,
                                      item_ID = mi.mi_item_isbn,
                                      item_description = mi.mi_item_description,
                                      pritem_qty = npi.nspi_pritem_qty,
                                      pritem_price = npi.nspi_pritem_price
                                  }).OrderByDescending(x => x.item_ID);

                poSyncItemList soPIVList = new poSyncItemList();
                soPIVList.poSyncItem = poSyncItem.ToList();
                return View(new Tuple<sde.Models.poSyncItemList>(soPIVList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POSyncExport() // PO Sync > Export Menu
        {
            using (sdeEntities entities = new sdeEntities())
            {
                return View();
            }  
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POSyncExportProcess(DateTime? fromDate1 = null, DateTime? toDate1 = null) // PO Sync > Export Process
        {
            using (sdeEntities entities = new sdeEntities())
            {
                List<cls_poSyncExport> ePOSList = new List<cls_poSyncExport>();
                poSyncExportReport ePOSr = new poSyncExportReport();

                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                DateTime displayfromDate = fromDate;
                DateTime displaytoDate = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(6);
                //toDate = toDate.AddHours(6);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var poSyncExport = (from np in entities.netsuite_pr
                                    join npi in entities.netsuite_pritem on np.nspr_pr_ID equals npi.nspi_nspr_pr_ID
                                    join mi in entities.map_item on npi.nspi_item_ID equals mi.mi_item_isbn
                                    where np.nspr_createdDate > fromDate && np.nspr_createdDate <= toDate
                                    select new cls_poSyncExport()
                                    {
                                        Extract_Date = np.nspr_rangeTo,
                                        PO_Number = np.nspr_pr_number,
                                        Supplier = np.nspr_pr_supplier,
                                        Location = np.nspr_pr_location,
                                        ISBN = mi.mi_item_isbn,
                                        Item_Description = mi.mi_item_description,
                                        Quantity = npi.nspi_pritem_qty
                                    }).OrderByDescending(x => x.Extract_Date).ThenBy(x => x.PO_Number).ThenBy(x => x.ISBN);
                if (poSyncExport.Count() != 0)
                {
                    foreach (var r in poSyncExport)
                    {
                        cls_poSyncExport poS = new cls_poSyncExport();
                        poS.Extract_Date = r.Extract_Date;
                        poS.PO_Number = r.PO_Number;
                        poS.Supplier = r.Supplier;
                        poS.Location = r.Location;
                        poS.ISBN = r.ISBN;
                        poS.Item_Description = r.Item_Description;
                        poS.Quantity = r.Quantity;

                        ePOSList.Add(poS);
                    }
                    ePOSr.poSEr = ePOSList;
                }
                else
                {
                    return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                }

                GridView gv = new GridView();
                gv.DataSource = ePOSr.poSEr;
                gv.DataBind();
                Response.ClearContent();
                Response.Buffer = true;
                Response.AddHeader("content-disposition", "attachment; filename=" + "PO_Sync_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + ".xls");
                Response.ContentType = "application/vnd.ms-excel";
                Response.Charset = "";
                StringWriter sw = new StringWriter();
                HtmlTextWriter htw = new HtmlTextWriter(sw);
                gv.RenderControl(htw);
                Response.Output.Write(sw.ToString());
                Response.Flush();
                Response.End();
                return RedirectToAction("POSync1", "dashboardTRADE");
            }
        }

        
        //start ckkoh 20150119 - #605 incomplete fulfillment

        [Authorize(Roles = "TRADE")]
        public ActionResult IncSOFulfillmentExportProcess(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                List<cls_incSOFulfillExport> eiSOFList = new List<cls_incSOFulfillExport>();
                incSOFulfillExportReport eiSOFr = new incSOFulfillExportReport();

                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                DateTime displayfromDate = fromDate;
                DateTime displaytoDate = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(10);
                //toDate = toDate.AddHours(10);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_soFulfillmentItem> tempIFList = new List<cls_soFulfillmentItem>();

                var incSOFulfillmentExport = "SELECT a.nt2_rangeTo, a.nt2_moNo, c.nsjomp_ordPack, c.nsjomp_packTitle, " +
                                            "c.nsjomp_ordQty, a.nt2_wmsfulfilledQty, a.nt2_lastfulfilledDate, " +
                                            "a.nt2_customer, a.nt2_subsidiary " +
                                            "FROM netsuite_syncso a " +
                                            "inner join netsuite_jobordmaster_pack c " +
                                            "on a.nt2_moNo = c.nsjomp_moNo " +
                                            "and a.nt2_progressStatus like CONCAT ('%', c.nsjomp_job_ID) " +
                                            "and a.nt2_item_internalID = c.nsjomp_item_internalID " +
                                            "where a.nt2_lastfulfilleddate > '" + date1 + "' " +
                                            "and a.nt2_lastfulfilleddate <= '" + date2 + "' " +
                                            "and c.nsjomp_ordQty > a.nt2_wmsfulfilledQty " +
                                            "ORDER BY a.nt2_lastfulfilleddate desc, a.nt2_moNo";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillmentExport, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_soFulfillmentItem incompleteFulfillmentItem = new cls_soFulfillmentItem();
                    incompleteFulfillmentItem.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    incompleteFulfillmentItem.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    incompleteFulfillmentItem.ordPack = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    incompleteFulfillmentItem.packTitle = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    incompleteFulfillmentItem.ordQty = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetInt32(4);
                    incompleteFulfillmentItem.ordFulfill = (dtr.GetValue(5) == DBNull.Value) ? 0 : dtr.GetInt32(5);
                    incompleteFulfillmentItem.lastFulfilledDate = (dtr.GetValue(6) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(6);
                    incompleteFulfillmentItem.customer = (dtr.GetValue(7) == DBNull.Value) ? String.Empty : dtr.GetString(7);
                    incompleteFulfillmentItem.subsidiary = (dtr.GetValue(8) == DBNull.Value) ? String.Empty : dtr.GetString(8);
                    
                    tempIFList.Add(incompleteFulfillmentItem);
                }
                dtr.Close();
                cmd.Dispose();

                soFulfillmentItemList incSOFulfillmentList = new soFulfillmentItemList();
                incSOFulfillmentList.soFulfillmentItem = tempIFList;

                if (tempIFList.Count() != 0)
                {
                    foreach (var r in tempIFList)
                    {
                        cls_incSOFulfillExport iSOF = new cls_incSOFulfillExport();
                        iSOF.Last_Fulfilled_Date = r.lastFulfilledDate;
                        iSOF.SO_Number = r.moNo;
                        iSOF.Customer = r.customer;
                        iSOF.Subsidiary = r.subsidiary;
                        iSOF.Order_Pack = r.ordPack;
                        iSOF.Pack_Title = r.packTitle;
                        iSOF.Order_Quantity = r.ordQty;
                        iSOF.Order_Fulfilled = r.ordFulfill;

                        eiSOFList.Add(iSOF);
                    }
                    eiSOFr.iSOFEr = eiSOFList;
                }
                else
                {
                    return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                }

                GridView gv = new GridView();
                gv.DataSource = eiSOFr.iSOFEr;
                gv.DataBind();
                Response.ClearContent();
                Response.Buffer = true;
                Response.AddHeader("content-disposition", "attachment; filename=" + "Incmplt_Fulfill_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + ".xls");
                Response.ContentType = "application/vnd.ms-excel";
                Response.Charset = "";
                StringWriter sw = new StringWriter();
                HtmlTextWriter htw = new HtmlTextWriter(sw);
                gv.RenderControl(htw);
                Response.Output.Write(sw.ToString());
                Response.Flush();
                Response.End();
                return RedirectToAction("IncompleteSOFulfillment", "dashboardTRADE");
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult IncompleteSOFulfillmentExport()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                return View();
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult IncompleteSOFulfillment(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(10);
                //toDate = toDate.AddHours(10);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_incompleteSOFulfillment> tempIFList = new List<cls_incompleteSOFulfillment>();

                var incSOFulfillment = "SELECT a.nt2_rangeTo, a.nt2_moNo, a.nt2_custID, a.nt2_customer, a.nt2_subsidiary, a.nt2_lastfulfilledDate " +
                                        "FROM netsuite_syncso a " +
                                        "inner join netsuite_jobordmaster_pack c " +
                                        "on a.nt2_moNo = c.nsjomp_moNo " +
                                        "and a.nt2_progressStatus like CONCAT ('%', c.nsjomp_job_ID) " +
                                        "and a.nt2_item_internalID = c.nsjomp_item_internalID " +
                                        "where a.nt2_lastfulfilleddate > '" + date1 + "' " +
                                        "and a.nt2_lastfulfilleddate <= '" + date2 + "' " +
                                        "and c.nsjomp_ordQty > a.nt2_wmsfulfilledQty " +
                                        "GROUP BY a.nt2_lastfulfilleddate, a.nt2_moNo, a.nt2_custID " +
                                        "ORDER BY a.nt2_lastfulfilleddate, a.nt2_moNo";
                
                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_incompleteSOFulfillment incompleteFulfillment = new cls_incompleteSOFulfillment();
                    incompleteFulfillment.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    incompleteFulfillment.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    incompleteFulfillment.custID = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    incompleteFulfillment.customer = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    incompleteFulfillment.subsidiary = (dtr.GetValue(4) == DBNull.Value) ? String.Empty : dtr.GetString(4);
                    incompleteFulfillment.lastFulfilledDate = (dtr.GetValue(5) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(5);
                    tempIFList.Add(incompleteFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                incompleteSOFulfillmentList incSOFulfillmentList = new incompleteSOFulfillmentList();
                incSOFulfillmentList.incompleteSOFulfillment = tempIFList;
                return View(new Tuple<sde.Models.incompleteSOFulfillmentList>(incSOFulfillmentList));
            }
        }


        [Authorize(Roles = "TRADE")]
        public ActionResult IncompleteSOFulfillmentItem(DateTime pass1, String pass2)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var date1 = pass1.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_soFulfillmentItem> tempIFIList = new List<cls_soFulfillmentItem>();

                var incSOFulfillment = "SELECT a.nt2_rangeTo, a.nt2_moNo, c.nsjomp_ordPack, c.nsjomp_packTitle, " +
                                        "c.nsjomp_ordQty, a.nt2_wmsfulfilledQty, a.nt2_lastfulfilledDate " +
                                        "FROM netsuite_syncso a " +
                                        "inner join netsuite_jobordmaster_pack c " +
                                        "on a.nt2_moNo = c.nsjomp_moNo " +
                                        "and a.nt2_progressStatus like CONCAT ('%', c.nsjomp_job_ID) " +
                                        "and a.nt2_item_internalID = c.nsjomp_item_internalID " +
                                        "where a.nt2_lastfulfilledDate = '" + date1 + "' " +
                                        "and a.nt2_moNo = '" + pass2 + "' " +
                                        "and c.nsjomp_ordQty > a.nt2_wmsfulfilledQty ";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_soFulfillmentItem incompleteFulfillmentItem = new cls_soFulfillmentItem();
                    incompleteFulfillmentItem.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    incompleteFulfillmentItem.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    incompleteFulfillmentItem.ordPack = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    incompleteFulfillmentItem.packTitle = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    incompleteFulfillmentItem.ordQty = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetInt32(4);
                    incompleteFulfillmentItem.ordFulfill = (dtr.GetValue(5) == DBNull.Value) ? 0 : dtr.GetInt32(5);
                    incompleteFulfillmentItem.lastFulfilledDate = (dtr.GetValue(6) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(6);
                    tempIFIList.Add(incompleteFulfillmentItem);
                }
                dtr.Close();
                cmd.Dispose();

                soFulfillmentItemList incSOFulfillmentList = new soFulfillmentItemList();
                incSOFulfillmentList.soFulfillmentItem = tempIFIList;
                return View(new Tuple<sde.Models.soFulfillmentItemList>(incSOFulfillmentList));
            }
        }

        //end ckkoh 20150119 - #605 incomplete fufillment

        [Authorize(Roles = "TRADE")]
        public ActionResult SOFulfillment1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Fulfillment > By Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddHours(4);
                //toDate = toDate.AddDays(1).AddHours(4);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var soFulfillment1 = (from aa in entities.wms_jobordscan
                                        where aa.jos_moNo.Contains("SO-MY")
                                        && aa.jos_rangeTo > fromDate
                                        && aa.jos_rangeTo <= toDate
                                        let k = new
                                        {
                                            job_ID = aa.jos_job_ID,
                                            rangeTo = aa.jos_rangeTo,
                                            exportDate = aa.jos_exportDate,
                                            moNo = aa.jos_moNo,
                                            deliveryRef = aa.jos_deliveryRef
                                        }
                                        group aa by k into g
                                        join bb in entities.netsuite_jobmo on new { JobID = g.Key.job_ID, MoNo = g.Key.moNo }
                                        equals new { JobID = bb.nsjm_nsj_job_ID, MoNo = bb.nsjm_moNo } into gj
                                        from foreignData in gj.DefaultIfEmpty()
                                        //join cc in entities.requestnetsuite_task on bb.nsjm_moNo_internalID 
                                        //equals cc.rnt_createdFromInternalId
                                        select new cls_sofulfillment()
                                        {
                                            rangeTo = g.Key.rangeTo,//aa.jos_rangeTo,
                                            exportDate = g.Key.exportDate,//aa.jos_exportDate,
                                            moNo = g.Key.moNo,//aa.jos_moNo,
                                            deliveryRef = g.Key.deliveryRef,
                                            job_ID = g.Key.job_ID,
                                            schID = (foreignData == null ? String.Empty : foreignData.nsjm_schID),
                                            mono_internalID = (foreignData == null ? String.Empty : foreignData.nsjm_moNo_internalID)
                                        })
                                     .OrderByDescending(x => x.rangeTo)
                                     .ThenBy(x => x.exportDate)
                                     .ThenBy(x => x.moNo);

                soFulfillmentList soF1List = new soFulfillmentList();
                soF1List.soFulfillment = soFulfillment1.ToList();
                return View(new Tuple<sde.Models.soFulfillmentList>(soF1List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOFulfillment2(String moNo) // SO Fulfillment > By SO number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var soFulfillment2 = (from aa in entities.wms_jobordscan
                                      where aa.jos_moNo.Contains("SO-MY")
                                      && aa.jos_moNo == moNo
                                      let k = new
                                      {
                                          job_ID = aa.jos_job_ID,
                                          rangeTo = aa.jos_rangeTo,
                                          exportDate = aa.jos_exportDate,
                                          moNo = aa.jos_moNo,
                                          deliveryRef = aa.jos_deliveryRef
                                      }
                                      group aa by k into g
                                      join bb in entities.netsuite_jobmo on new { JobID = g.Key.job_ID, MoNo = g.Key.moNo }
                                      equals new { JobID = bb.nsjm_nsj_job_ID, MoNo = bb.nsjm_moNo } into gj
                                      from foreignData in gj.DefaultIfEmpty()
                                      //join cc in entities.requestnetsuite_task on bb.nsjm_moNo_internalID 
                                      //equals cc.rnt_createdFromInternalId
                                      select new cls_sofulfillment()
                                      {
                                          rangeTo = g.Key.rangeTo,//aa.jos_rangeTo,
                                          exportDate = g.Key.exportDate,//aa.jos_exportDate,
                                          moNo = g.Key.moNo,//aa.jos_moNo,
                                          deliveryRef = g.Key.deliveryRef,
                                          job_ID = g.Key.job_ID,
                                          schID = (foreignData == null ? String.Empty : foreignData.nsjm_schID),
                                          mono_internalID = (foreignData == null ? String.Empty : foreignData.nsjm_moNo_internalID)
                                      })
                                     .OrderByDescending(x => x.rangeTo)
                                     .ThenBy(x => x.exportDate)
                                     .ThenBy(x => x.moNo);

                soFulfillmentList soF2List = new soFulfillmentList();
                soF2List.soFulfillment = soFulfillment2.ToList();
                return View(new Tuple<sde.Models.soFulfillmentList>(soF2List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SOFulfillmentItem(String pass1, String pass2) // SO Fulfillment > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String jobIDPassed = pass1;
                String moNoPassed = pass2;
                var soFulfillmentItem = (from aa in entities.wms_jobordscan_pack
                                          where aa.josp_jobID == jobIDPassed
                                          && aa.josp_moNo == moNoPassed
                                          join bb in entities.netsuite_jobordmaster_pack on aa.josp_pack_ID equals bb.nsjomp_jobOrdMaster_pack_ID
                                          join dd in entities.wms_jobordscan on new { JobID = aa.josp_jobID, MoNo = aa.josp_moNo, RecNo = aa.josp_ordRecNo }
                                          equals new { JobID = dd.jos_job_ID, MoNo = dd.jos_moNo, RecNo = dd.jos_ordRecNo }
                                          select new cls_soFulfillmentItem()
                                          {
                                              rangeTo = aa.josp_rangeTo,
                                              exportDate = aa.josp_exportDate,
                                              moNo = aa.josp_moNo,
                                              deliveryRef = dd.jos_deliveryRef,
                                              ordPack = bb.nsjomp_ordPack,
                                              packTitle = bb.nsjomp_packTitle,
                                              ordFulfill = aa.josp_ordFulFill
                                          }).OrderBy(x => x.ordPack);

                soFulfillmentItemList soFIVList = new soFulfillmentItemList();
                soFIVList.soFulfillmentItem = soFulfillmentItem.ToList();
                return View(new Tuple<sde.Models.soFulfillmentItemList>(soFIVList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POReceive1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // PO Receive - By Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(9);
                //toDate = toDate.AddHours(9);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);
                    
                var poReceive1 = (from wp in entities.wms_poreceive
                                 where wp.po_rangeTo > fromDate && wp.po_rangeTo <= toDate
                                 join prdata in entities.netsuite_pr on wp.po_pr_ID equals prdata.nspr_pr_ID
                                 join wpi in entities.wms_poreceiveitem on wp.po_poreceive_ID equals wpi.poi_poreceive_ID
                                 join mi in entities.map_item on wpi.poi_item_ID equals mi.mi_item_isbn
                                 select new
                                 {
                                     rangeTo = wp.po_rangeTo,
                                     prNumber = prdata.nspr_pr_number,
                                     internalID = prdata.nspr_pr_internalID,
                                     supplier = prdata.nspr_pr_supplier,
                                     location = prdata.nspr_pr_location,
                                     number = wp.po_poreceive_number,
                                     invoice = wp.po_poreceive_invoice
                                 }).ToList();
                var groupQ2 = from p in poReceive1
                              let k = new
                              {
                                  rangeTo = p.rangeTo,
                                  prNumber = p.prNumber,
                                  internalID = p.internalID,
                                  supplier = p.supplier,
                                  location = p.location,
                                  number = p.number,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select new cls_poReceive
                              {
                                  rangeTo = g.Key.rangeTo,
                                  prNumber = g.Key.prNumber,
                                  internalID = g.Key.internalID,
                                  supplier = g.Key.supplier,
                                  location = g.Key.location,
                                  number = g.Key.number,
                                  invoice = g.Key.invoice,
                                  numOfItems = g.Count()
                              };

                poReceiveList poR1List = new poReceiveList();
                poR1List.poReceive = groupQ2.OrderByDescending(x => x.rangeTo).ThenBy(x => x.prNumber).ToList();
                return View(new Tuple<sde.Models.poReceiveList>(poR1List));

            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POReceive2(String pr_number) // PO Receive - By PO Number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var poReceive2 = (from wp in entities.wms_poreceive
                                 join prdata in entities.netsuite_pr on wp.po_pr_ID equals prdata.nspr_pr_ID
                                 join wpi in entities.wms_poreceiveitem on wp.po_poreceive_ID equals wpi.poi_poreceive_ID
                                 join mi in entities.map_item on wpi.poi_item_ID equals mi.mi_item_isbn
                                 where prdata.nspr_pr_number == pr_number
                                 select new
                                 {
                                     rangeTo = wp.po_rangeTo,
                                     prNumber = prdata.nspr_pr_number,
                                     internalID = prdata.nspr_pr_internalID,
                                     supplier = prdata.nspr_pr_supplier,
                                     location = prdata.nspr_pr_location,
                                     number = wp.po_poreceive_number,
                                     invoice = wp.po_poreceive_invoice
                                 }).ToList();
                var groupQ2 = from p in poReceive2
                              let k = new
                              {
                                  rangeTo = p.rangeTo,
                                  prNumber = p.prNumber,
                                  internalID = p.internalID,
                                  supplier = p.supplier,
                                  location = p.location,
                                  number = p.number,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select new cls_poReceive
                              {
                                  rangeTo = g.Key.rangeTo,
                                  prNumber = g.Key.prNumber,
                                  internalID = g.Key.internalID,
                                  supplier = g.Key.supplier,
                                  location = g.Key.location,
                                  number = g.Key.number,
                                  invoice = g.Key.invoice,
                                  numOfItems = g.Count()
                              };

                poReceiveList poR2List = new poReceiveList();
                poR2List.poReceive = groupQ2.OrderByDescending(x => x.rangeTo).ThenBy(x => x.prNumber).ToList();
                return View(new Tuple<sde.Models.poReceiveList>(poR2List));

            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult POReceiveItem(String pass1, DateTime pass2, String pass3) // PO Receipt > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String pr_numberPassed = pass1;
                DateTime rangeToPassed = pass2;
                String pr_receivedIDPassed = pass3;
                var poReceiveItem = (from wp in entities.wms_poreceive
                                     join prdata in entities.netsuite_pr on wp.po_pr_ID equals prdata.nspr_pr_ID
                                     join wpi in entities.wms_poreceiveitem on wp.po_poreceive_ID equals wpi.poi_poreceive_ID
                                     join mi in entities.map_item on wpi.poi_item_ID equals mi.mi_item_isbn
                                     where prdata.nspr_pr_number == pr_numberPassed && wp.po_rangeTo == rangeToPassed && wp.po_poreceive_number == pr_receivedIDPassed
                                     select new cls_poReceiveItem
                                     {
                                         rangeTo = wp.po_rangeTo,
                                         prNumber = prdata.nspr_pr_number,
                                         number = wp.po_poreceive_number,
                                         invoice = wp.po_poreceive_invoice,
                                         location = wpi.poi_location_code,
                                         itemID = mi.mi_item_isbn,
                                         itemDescription = mi.mi_item_description,
                                         itemQty = wpi.poi_poreceiveItem_qty,
                                         damageQty = wpi.poi_damage_qty,
                                         excessQty = wpi.poi_excessQty

                                     }).OrderBy(x => x.itemID);

                poReceiveItemList poRIList = new poReceiveItemList();
                poRIList.poReceiveItem = poReceiveItem.ToList();
                return View(new Tuple<sde.Models.poReceiveItemList>(poRIList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORSync1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SOR Sync > Filter by Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(6);
                //toDate = toDate.AddHours(6);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var sorSync1 = (from nr in entities.netsuite_return
                                join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                where nr.nsr_createdDate > fromDate && nr.nsr_createdDate <= toDate
                                select new
                                {
                                    schID = nr.nsr_rr_schID,
                                    number = nr.nsr_rr_number,
                                    createdDate = nr.nsr_createdDate,
                                    invoice = nri.nsri_rritem_invoice,
                                    isbn = nri.nsri_rritem_isbn,
                                    return_qty = nri.nsri_rritem_return_qty
                                }).ToList();
                var groupQ2 = from p in sorSync1
                              let k = new
                              {
                                  schID = p.schID,
                                  number = p.number,
                                  createdDate = p.createdDate,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select new cls_sorSync()
                              {
                                  schID = g.Key.schID,
                                  number = g.Key.number,
                                  createdDate = g.Key.createdDate,
                                  invoice = g.Key.invoice,
                                  numOfItems = g.Count(),
                                  sum_return_qty = g.Sum(p => p.return_qty)
                              };

                var sorSyncCount = groupQ2.Count();
                ViewBag.sorSyncCount = sorSyncCount;

                sorSyncList sorS1List = new sorSyncList();
                sorS1List.sorSync = groupQ2.OrderByDescending(x => x.createdDate).ThenBy(x => x.number).ToList();
                return View(new Tuple<sde.Models.sorSyncList>(sorS1List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORSyncItemView(String pass1) // SOR Sync > Item View   
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String returnNumberPassed = pass1;
                var sorSyncItemView = (from nr in entities.netsuite_return
                                       join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                       join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                       where nr.nsr_rr_number == returnNumberPassed
                                       select new cls_sorSyncItem
                                       {
                                           schID = nr.nsr_rr_schID,
                                           number = nr.nsr_rr_number,
                                           createdDate = nr.nsr_createdDate,
                                           invoice = nri.nsri_rritem_invoice,
                                           isbn = mi.mi_item_isbn,
                                           item_description = mi.mi_item_description,
                                           return_qty = nri.nsri_rritem_return_qty
                                       }).OrderBy(x => x.isbn);

                sorSyncItemList sorSIVList = new sorSyncItemList();
                sorSIVList.sorSyncItem = sorSyncItemView.ToList();
                return View(new Tuple<sde.Models.sorSyncItemList>(sorSIVList));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORSync2(String return_number) // SOR Sync > Search by Return Number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var sorSync2 = (from nr in entities.netsuite_return
                                join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                where nr.nsr_rr_number == return_number
                                select new cls_sorSyncItem
                                {
                                    schID = nr.nsr_rr_schID,
                                    number = nr.nsr_rr_number,
                                    createdDate = nr.nsr_createdDate,
                                    invoice = nri.nsri_rritem_invoice,
                                    isbn = mi.mi_item_isbn,
                                    item_description = mi.mi_item_description,
                                    return_qty = nri.nsri_rritem_return_qty
                                }).OrderByDescending(x => x.createdDate).ThenBy(x => x.isbn);

                sorSyncItemList sorS2List = new sorSyncItemList();
                sorS2List.sorSyncItem = sorSync2.ToList();
                return View(new Tuple<sde.Models.sorSyncItemList>(sorS2List));
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORSyncExport() // SOR Sync > Export Menu
        {
            using (sdeEntities entities = new sdeEntities())
            {
                return View();
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORSyncExportProcess(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SOR Sync > Export Process
        {
            using (sdeEntities entities = new sdeEntities())
            {
                List<cls_sorSyncExport> eSORSList = new List<cls_sorSyncExport>();
                sorSyncExportReport eSORSr = new sorSyncExportReport();

                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                DateTime displayfromDate = fromDate;
                DateTime displaytoDate = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(6);
                //toDate = toDate.AddHours(6);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var sorSyncExport = (from nr in entities.netsuite_return
                                    join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                    join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                    where nr.nsr_createdDate > fromDate && nr.nsr_createdDate <= toDate
                                    select new cls_sorSyncExport()
                                    {
                                        CreatedDate = nr.nsr_createdDate,
                                        Return_Number = nr.nsr_rr_number,
                                        Customer = nr.nsr_rr_schID,
                                        Invoice = nri.nsri_rritem_invoice,
                                        ISBN = mi.mi_item_isbn,
                                        Item_Description = mi.mi_item_description,
                                        Return_Quantity = nri.nsri_rritem_return_qty
                                    }).OrderByDescending(x => x.CreatedDate).ThenBy(x => x.Return_Number).ThenBy(x => x.ISBN);
                if (sorSyncExport.Count() != 0)
                {
                    foreach (var r in sorSyncExport)
                    {
                        cls_sorSyncExport sorS = new cls_sorSyncExport();
                        sorS.CreatedDate = r.CreatedDate;
                        sorS.Return_Number = r.Return_Number;
                        sorS.Customer = r.Customer;
                        sorS.Invoice = r.Invoice;
                        sorS.ISBN = r.ISBN;
                        sorS.Item_Description = r.Item_Description;
                        sorS.Return_Quantity = r.Return_Quantity;

                        eSORSList.Add(sorS);
                    }
                    eSORSr.sorSEr = eSORSList;
                }
                else
                {
                    return Content("<script language='javascript' type='text/javascript'>alert('No records found.');history.back();</script>");
                }

                GridView gv = new GridView();
                gv.DataSource = eSORSr.sorSEr;
                gv.DataBind();
                Response.ClearContent();
                Response.Buffer = true;
                Response.AddHeader("content-disposition", "attachment; filename=" + "SOR_Sync_" + displayfromDate.ToString("ddMMyyyy") + "_" + displaytoDate.ToString("ddMMyyyy") + ".xls");
                Response.ContentType = "application/vnd.ms-excel";
                Response.Charset = "";
                StringWriter sw = new StringWriter();
                HtmlTextWriter htw = new HtmlTextWriter(sw);
                gv.RenderControl(htw);
                Response.Output.Write(sw.ToString());
                Response.Flush();
                Response.End();
                return RedirectToAction("SORSync1", "dashboardTRADE");
            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORReceive1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SOR Receive > By Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                //fromDate = fromDate.AddDays(-1).AddHours(9);
                //toDate = toDate.AddHours(9);
                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var sorReceive1 = (from nr in entities.netsuite_return
                                  join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                  join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                  where nr.nsr_rr_returnDate > fromDate && nr.nsr_rr_returnDate <= toDate
                                  //where nr.nsr_createdDate > fromDate && nr.nsr_createdDate <= toDate     // for testing
                                  select new
                                  {
                                      schID = nr.nsr_rr_schID,
                                      number = nr.nsr_rr_number,
                                      returnDate = nr.nsr_rr_returnDate,
                                      //createdDate = nr.nsr_createdDate,
                                      invoice = nri.nsri_rritem_invoice,
                                      isbn = nri.nsri_rritem_isbn,
                                      receive_qty = nri.nsri_rritem_receive_qty
                                  }).ToList();
                var groupQ2 = from p in sorReceive1
                              let k = new
                              {
                                  schID = p.schID,
                                  number = p.number,
                                  returnDate = p.returnDate,
                                  //createdDate = p.createdDate,    // for testing
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select new cls_sorReceive()
                              {
                                  schID = g.Key.schID,
                                  number = g.Key.number,
                                  returnDate = g.Key.returnDate,
                                  //createdDate = g.Key.createdDate,    // for testing
                                  invoice = g.Key.invoice,
                                  numOfItems = g.Count(),
                                  sum_receive_qty = g.Sum(p => p.receive_qty)
                              };         

                var sorReceiveCount = groupQ2.Count();
                ViewBag.sorReceiveCount = sorReceiveCount;

                sorReceiveList sorR1List = new sorReceiveList();
                sorR1List.sorReceive = groupQ2.OrderByDescending(x => x.returnDate).ThenBy(x => x.number).ToList();
                //sorRList.sorReceive = groupQ2.OrderByDescending(x => x.createdDate).ThenBy(x => x.number).ToList();   // for testing
                return View(new Tuple<sde.Models.sorReceiveList>(sorR1List));

            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORReceive2(String return_number) // SOR Receive > By Return Number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var sorReceive2 = (from nr in entities.netsuite_return
                                   join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                   join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                   where nr.nsr_rr_number == return_number
                                   select new
                                   {
                                       schID = nr.nsr_rr_schID,
                                       number = nr.nsr_rr_number,
                                       returnDate = nr.nsr_rr_returnDate,
                                       //createdDate = nr.nsr_createdDate,  // for testing
                                       invoice = nri.nsri_rritem_invoice,
                                       isbn = nri.nsri_rritem_isbn,
                                       receive_qty = nri.nsri_rritem_receive_qty
                                   }).ToList();
                var groupQ2 = from p in sorReceive2
                              let k = new
                              {
                                  schID = p.schID,
                                  number = p.number,
                                  returnDate = p.returnDate,
                                  //createdDate = p.createdDate,    // for testing
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select new cls_sorReceive()
                              {
                                  schID = g.Key.schID,
                                  number = g.Key.number,
                                  returnDate = g.Key.returnDate,
                                  //createdDate = g.Key.createdDate,    // for testing
                                  invoice = g.Key.invoice,
                                  numOfItems = g.Count(),
                                  sum_receive_qty = g.Sum(p => p.receive_qty)
                              };

                var sorReceiveCount = groupQ2.Count();
                ViewBag.sorReceiveCount = sorReceiveCount;

                sorReceiveList sorR2List = new sorReceiveList();
                sorR2List.sorReceive = groupQ2.OrderByDescending(x => x.returnDate).ThenBy(x => x.number).ToList();
                //sorRList.sorReceive = groupQ2.OrderByDescending(x => x.createdDate).ThenBy(x => x.number).ToList();   // for testing
                return View(new Tuple<sde.Models.sorReceiveList>(sorR2List));

            }
        }

        [Authorize(Roles = "TRADE")]
        public ActionResult SORReceiveItemView(String pass1) // SOR Receive > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String returnNumberPassed = pass1;
                var sorReceiveItemView = (from nr in entities.netsuite_return
                                          join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                          join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                          where nr.nsr_rr_number == returnNumberPassed
                                          select new cls_sorReceiveItem
                                          {
                                              schID = nr.nsr_rr_schID,
                                              number = nr.nsr_rr_number,
                                              returnDate = nr.nsr_rr_returnDate,
                                              //createdDate = nr.nsr_createdDate,   // for testing
                                              invoice = nri.nsri_rritem_invoice,
                                              isbn = mi.mi_item_isbn,
                                              item_description = mi.mi_item_description,
                                              receive_qty = nri.nsri_rritem_receive_qty
                                          }).OrderBy(x => x.isbn);

                sorReceiveItemList sorRIVList = new sorReceiveItemList();
                sorRIVList.sorReceiveItem = sorReceiveItemView.ToList();
                return View(new Tuple<sde.Models.sorReceiveItemList>(sorRIVList));
            }
        }

        //ANET-26 - Exception Report: Inbound & outbound Reports
        [Authorize(Roles = "TRADE")]
        public ActionResult SONotSync1(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Not Sync > Filter by Date
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                if (toDate1 != null)
                {
                    Session["FromDate"] = fromDate;
                    Session["ToDate"] = toDate;
                }
                else
                {
                    Session["FromDate"] = string.Empty;
                    Session["ToDate"] = string.Empty;
                }

                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                if (fromDate1 != null && toDate1 != null)
                {
                    var soNotSync1 = (from ns in entities.netsuite_newso
                                      join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                      where ns.nt1_sodate > fromDate && ns.nt1_sodate <= toDate &&
                                      ns.nt1_credit_hold == 1 && ns.nt1_status == "PENDING FULFILLMENT"
                                      && ns.nt1_synctowms == "1"
                                      && ns.nt1_moNo.Contains("SO-MY")
                                      let k = new
                                      {
                                          rangeTo = ns.nt1_rangeTo,
                                          sodate = ns.nt1_sodate,
                                          moNo = ns.nt1_moNo,
                                          customer = ns.nt1_customer,
                                          addressee = ns.nt1_addressee,
                                          country = ns.nt1_country
                                      }
                                      group ns by k into g
                                      select new cls_soSync()
                                      {
                                          rangeTo = g.Key.rangeTo,
                                          sodate = g.Key.sodate,
                                          moNo = g.Key.moNo,
                                          customer = g.Key.customer,
                                          addressee = g.Key.addressee,
                                          country = g.Key.country,
                                          numOfItems = g.Count(),
                                          sum_ordQty = g.Sum(s => s.nt1_ordQty),
                                      }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo);


                    var soNotSync2 = (from ns in entities.netsuite_newso
                                      join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                      where ns.nt1_sodate > fromDate && ns.nt1_sodate <= toDate &&
                                      ns.nt1_credit_hold == 0 && ns.nt1_status == "PENDING FULFILLMENT"
                                      && ns.nt1_synctowms == "1" && ns.nt1_moNo.Contains("SO-MY") && ns.nt1_fulfilledQty == 0
                                      && !entities.netsuite_syncso.Any(x => x.nt2_moNo == ns.nt1_moNo && x.nt2_itemID == ns.nt1_itemID)
                                      let k = new
                                      {
                                          rangeTo = ns.nt1_rangeTo,
                                          sodate = ns.nt1_sodate,
                                          moNo = ns.nt1_moNo,
                                          customer = ns.nt1_customer,
                                          addressee = ns.nt1_addressee,
                                          country = ns.nt1_country,
                                      }
                                      group ns by k into g
                                      select new cls_soSync()
                                      {
                                          rangeTo = g.Key.rangeTo,
                                          sodate = g.Key.sodate,
                                          moNo = g.Key.moNo,
                                          customer = g.Key.customer,
                                          addressee = g.Key.addressee,
                                          country = g.Key.country,
                                          numOfItems = g.Count(),
                                          sum_ordQty = g.Sum(s => s.nt1_ordQty),
                                      }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo);

                    var soNotSync1Count = soNotSync1.Count() + soNotSync2.Count();
                    ViewBag.soNotSync1Count = soNotSync1Count;

                    soSyncList soS1List = new soSyncList();
                    soS1List.soSync = soNotSync1.Union(soNotSync2).ToList();
                    if (soS1List.soSync.Count() > 0)
                    {
                        soS1List.soSync = soS1List.soSync.OrderByDescending(x => x.sodate).ToList();
                    }
                    return View(new Tuple<sde.Models.soSyncList>(soS1List));
                }
                else
                {
                    var soNotSync1 = (from ns in entities.netsuite_newso
                                      join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                      where ns.nt1_rangeTo > fromDate && ns.nt1_rangeTo <= toDate &&
                                      ns.nt1_credit_hold == 1 && ns.nt1_status == "PENDING FULFILLMENT"
                                      && ns.nt1_synctowms == "1"
                                      && ns.nt1_moNo.Contains("SO-MY")
                                      let k = new
                                      {
                                          rangeTo = ns.nt1_rangeTo,
                                          sodate = ns.nt1_sodate,
                                          moNo = ns.nt1_moNo,
                                          customer = ns.nt1_customer,
                                          addressee = ns.nt1_addressee,
                                          country = ns.nt1_country
                                      }
                                      group ns by k into g
                                      select new cls_soSync()
                                      {
                                          rangeTo = g.Key.rangeTo,
                                          sodate = g.Key.sodate,
                                          moNo = g.Key.moNo,
                                          customer = g.Key.customer,
                                          addressee = g.Key.addressee,
                                          country = g.Key.country,
                                          numOfItems = g.Count(),
                                          sum_ordQty = g.Sum(s => s.nt1_ordQty),
                                      }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo);


                    var soNotSync2 = (from ns in entities.netsuite_newso
                                      join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                      where ns.nt1_rangeTo > fromDate && ns.nt1_rangeTo <= toDate &&
                                      ns.nt1_credit_hold == 0 && ns.nt1_status == "PENDING FULFILLMENT"
                                      && ns.nt1_synctowms == "1" && ns.nt1_moNo.Contains("SO-MY") && ns.nt1_fulfilledQty == 0
                                      && !entities.netsuite_syncso.Any(x => x.nt2_moNo == ns.nt1_moNo && x.nt2_itemID == ns.nt1_itemID)
                                      let k = new
                                      {
                                          rangeTo = ns.nt1_rangeTo,
                                          sodate = ns.nt1_sodate,
                                          moNo = ns.nt1_moNo,
                                          customer = ns.nt1_customer,
                                          addressee = ns.nt1_addressee,
                                          country = ns.nt1_country,
                                      }
                                      group ns by k into g
                                      select new cls_soSync()
                                      {
                                          rangeTo = g.Key.rangeTo,
                                          sodate = g.Key.sodate,
                                          moNo = g.Key.moNo,
                                          customer = g.Key.customer,
                                          addressee = g.Key.addressee,
                                          country = g.Key.country,
                                          numOfItems = g.Count(),
                                          sum_ordQty = g.Sum(s => s.nt1_ordQty),
                                      }).OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo);

                    var soNotSync1Count = soNotSync1.Count() + soNotSync2.Count();
                    ViewBag.soNotSync1Count = soNotSync1Count;

                    soSyncList soS1List = new soSyncList();
                    soS1List.soSync = soNotSync1.Union(soNotSync2).ToList();
                    if (soS1List.soSync.Count() > 0)
                    {
                        soS1List.soSync = soS1List.soSync.OrderByDescending(x => x.sodate).ToList();
                    }

                    return View(new Tuple<sde.Models.soSyncList>(soS1List));
                }



                //var MS = dataSource1.Union(dataSource2).ToList();


            }
        }

        //ANET-26 - Exception Report: Inbound & outbound Reports
        [Authorize(Roles = "TRADE")]
        public ActionResult SONotSyncItemView(String pass1) // SO Not Sync > Item View
        {
            using (sdeEntities entities = new sdeEntities())
            {
                String moNoPassed = pass1;
                var soSyncItem = (from s in entities.netsuite_newso
                                  join i in entities.map_item on s.nt1_itemID equals i.mi_item_isbn
                                  where s.nt1_moNo == moNoPassed
                                  select new cls_soSyncItem()
                                  {
                                      rangeTo = s.nt1_rangeTo,
                                      moNo = s.nt1_moNo,
                                      customer = s.nt1_customer,
                                      addressee = s.nt1_addressee,
                                      country = s.nt1_country,
                                      itemID = i.mi_item_isbn,
                                      itemDescription = i.mi_item_description,
                                      ordQty = s.nt1_ordQty,
                                  }).OrderBy(x => x.itemID);

                soSyncItemList soSIVList = new soSyncItemList();
                soSIVList.soSyncItem = soSyncItem.ToList();
                return View(new Tuple<sde.Models.soSyncItemList>(soSIVList));
            }
        }

        //ANET-26 - Exception Report: Inbound & outbound Reports
        [Authorize(Roles = "TRADE")]
        public ActionResult SONotSync2(String moNo) // SO Not Sync > Search by SO Number
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var soSync2 = (from s in entities.netsuite_newso
                               join i in entities.map_item on s.nt1_itemID equals i.mi_item_isbn
                               where s.nt1_moNo == moNo
                               select new cls_soSyncItem()
                               {
                                   rangeTo = s.nt1_rangeTo,
                                   moNo = s.nt1_moNo,
                                   customer = s.nt1_customer,
                                   addressee = s.nt1_addressee,
                                   country = s.nt1_country,
                                   itemID = i.mi_item_isbn,
                                   itemDescription = i.mi_item_description,
                                   ordQty = s.nt1_ordQty
                               }).OrderBy(x => x.itemID).ThenBy(x => x.rangeTo);

                soSyncItemList soS2List = new soSyncItemList();
                soS2List.soSyncItem = soSync2.ToList();
                return View(new Tuple<sde.Models.soSyncItemList>(soS2List));
            }
        }

        //ANET-26 - Exception Report: Inbound & outbound Reports
        [Authorize(Roles = "TRADE")]
        public ActionResult SOInvoiceGenerate(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_soSync> tempIFList = new List<cls_soSync>();

                var soInvoiceGenerate = "select distinct requestnetsuite_task.rnt_createdDate as rangeTo, netsuite_newso.nt1_moNo as monNo, " +
                                        " netsuite_newso.nt1_customer as customer, netsuite_newso.nt1_addressee as addressee, " +
                                        " netsuite_newso.nt1_country as country, netsuite_newso.nt1_SEIS_moNo as dropshipmono from wms_jobordscan " +
                                       "inner join requestnetsuite_task " +
                                       "on wms_jobordscan.jos_moNo = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt_refNO, '.', 3), '.', -1) " +
                                       "inner join netsuite_newso " +
                                       "on wms_jobordscan.jos_moNo = netsuite_newso.nt1_mono " +
                                       "where wms_jobordscan.jos_moNo like '%SO-MY%' and " +
                                       "wms_jobordscan.jos_GMYInvoiceProgress is not null " +
                                       "and requestnetsuite_task.rnt_status = 'TRUE' " +
                                       "and requestnetsuite_task.rnt_description in( 'UPD-STATUS.GMY INVOICE','UPD-STATUS.GMY INVOICE PM') " +
                                       "and jos_rangeTo > '" + date1 + "' and jos_rangeTo <=  '" + date2 + "' " +
                                       "and rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' ";
                //"order by wms_jobordscan.jos_createdDate desc ";

                soInvoiceGenerate = soInvoiceGenerate + "union select distinct rnt.rnt_createdDate as rangeTo, ns.nt1_mono as monNo, " +
                                    " ns.nt1_customer as customer, ns.nt1_addressee as addressee, ns.nt1_country as country , '' as dropshipmono from requestnetsuite_task rnt " +
                                    " inner join netsuite_dropshipfulfillment nd on rnt.rnt_jobID = nd.dsf_invProgress " +
                                    " and nd.dsf_dropshipmono = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt.rnt_refNO, '.', 3), '.', -1) " +
                                    " inner join netsuite_newso ns on nd.dsf_dropshipmono_internalID = ns.nt1_mono_internalid " +
                                    " where rnt.rnt_description = 'UPD-STATUS.DROPSHIP INVOICE' and rnt.rnt_status = 'TRUE' " +
                                    " and nd.dsf_invProgress is not null and rnt.rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' " +
                                    " and nd.dsf_invUpdatedDate > '" + date1 + "' and nd.dsf_invUpdatedDate <= '" + date2 + "' ";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(soInvoiceGenerate, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_soSync data_cls_soSync = new cls_soSync();
                    data_cls_soSync.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    data_cls_soSync.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    data_cls_soSync.customer = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    data_cls_soSync.addressee = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    data_cls_soSync.country = (dtr.GetValue(4) == DBNull.Value) ? String.Empty : dtr.GetString(4);
                    data_cls_soSync.dropshipMono = (dtr.GetValue(5) == DBNull.Value) ? String.Empty : dtr.GetString(5);
                    tempIFList.Add(data_cls_soSync);
                }
                dtr.Close();
                cmd.Dispose();

                soSyncList soS1List = new soSyncList();
                soS1List.soSync = tempIFList;
                if (soS1List.soSync.Count() > 0)
                {
                    soS1List.soSync = soS1List.soSync.OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo).ToList();
                }
                return View(new Tuple<sde.Models.soSyncList>(soS1List));
            }
        }

        //ANET-26 - Exception Report: Inbound & outbound Reports
        [Authorize(Roles = "TRADE")]
        public ActionResult SOInvoiceNotGenerate(DateTime? fromDate1 = null, DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Today;
                }
                else
                {
                    fromDate = Convert.ToDateTime(fromDate1);
                    toDate = Convert.ToDateTime(toDate1);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                fromDate = fromDate.AddDays(-1).AddHours(23);
                toDate = toDate.AddHours(23);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_soSync> tempIFList = new List<cls_soSync>();

                var soInvoiceNotGenerate = "select distinct requestnetsuite_task.rnt_createdDate as rangeTo, netsuite_newso.nt1_moNo as monNo , netsuite_newso.nt1_customer as customer, netsuite_newso.nt1_addressee as addressee , netsuite_newso.nt1_country as country, netsuite_newso.nt1_SEIS_moNo as dropshipmono, " +
                                         " requestnetsuite_task.rnt_status as status, requestnetsuite_task.rnt_errorDesc as description " +
                                         " from wms_jobordscan " +
                                         "inner join requestnetsuite_task " +
                                         "on wms_jobordscan.jos_moNo = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt_refNO, '.', 3), '.', -1) " +
                                         "inner join netsuite_newso " +
                                         "on wms_jobordscan.jos_moNo = netsuite_newso.nt1_mono " +
                                         "where wms_jobordscan.jos_moNo like '%SO-MY%' and " +
                                         "wms_jobordscan.jos_GMYInvoiceProgress is not null " +
                                         "and requestnetsuite_task.rnt_status not in('TRUE') " +
                                         "and requestnetsuite_task.rnt_description in( 'UPD-STATUS.GMY INVOICE','UPD-STATUS.GMY INVOICE PM') " +
                                         "and jos_rangeTo > '" + date1 + "' and jos_rangeTo <=  '" + date2 + "' " +
                                         "and rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' " +
                                         "UNION " +
                                         "select distinct wms_jobordscan.jos_rangeTo as rangeTo, netsuite_newso.nt1_moNo as monNo , netsuite_newso.nt1_customer as customer, netsuite_newso.nt1_addressee as addressee, netsuite_newso.nt1_country as country, netsuite_newso.nt1_SEIS_moNo as dropshipmono, " +
                                         " '' as status, '' as description from wms_jobordscan " +
                                         "inner join netsuite_newso " +
                                         "on wms_jobordscan.jos_moNo = netsuite_newso.nt1_mono " +
                                         "where wms_jobordscan.jos_moNo like '%SO-MY%' and jos_rangeTo > '" + date1 + "' and jos_rangeTo <= '" + date2 + "' " +
                                         "and jos_GMYInvoiceProgress is null ";

                soInvoiceNotGenerate = soInvoiceNotGenerate + "union select distinct rnt.rnt_createdDate as rangeTo, ns.nt1_mono as monNo, " +
                                       " ns.nt1_customer as customer, ns.nt1_addressee as addressee, ns.nt1_country as country, '' as dropshipmono, " +
                                       " rnt.rnt_status as status, rnt.rnt_errorDesc as description " +
                                       " from requestnetsuite_task rnt inner join netsuite_dropshipfulfillment nd on rnt.rnt_jobID = nd.dsf_invProgress " +
                                       " and nd.dsf_dropshipmono = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt.rnt_refNO, '.', 3), '.', -1) " +
                                       " inner join netsuite_newso ns on nd.dsf_dropshipmono_internalID = ns.nt1_mono_internalid " +
                                       " where rnt.rnt_description = 'UPD-STATUS.DROPSHIP INVOICE' and rnt.rnt_status not in ( 'TRUE') " +
                                       " and nd.dsf_invProgress is not null " +
                                       " and rnt.rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' ";
                //" and nd.dsf_invUpdatedDate > '" + date1 + "' and nd.dsf_invUpdatedDate <= '" + date2 + "' ";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(soInvoiceNotGenerate, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_soSync data_cls_soSync = new cls_soSync();
                    data_cls_soSync.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    data_cls_soSync.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    data_cls_soSync.customer = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    data_cls_soSync.addressee = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    data_cls_soSync.country = (dtr.GetValue(4) == DBNull.Value) ? String.Empty : dtr.GetString(4);
                    data_cls_soSync.dropshipMono = (dtr.GetValue(5) == DBNull.Value) ? String.Empty : dtr.GetString(5);
                    data_cls_soSync.status = (dtr.GetValue(6) == DBNull.Value) ? String.Empty : dtr.GetString(6);
                    data_cls_soSync.description = (dtr.GetValue(7) == DBNull.Value) ? String.Empty : dtr.GetString(7);
                    tempIFList.Add(data_cls_soSync);
                }
                dtr.Close();
                cmd.Dispose();

                soSyncList soS1List = new soSyncList();
                soS1List.soSync = tempIFList;
                if (soS1List.soSync.Count() > 0)
                {
                    soS1List.soSync = soS1List.soSync.OrderByDescending(x => x.rangeTo).ThenBy(x => x.moNo).ToList();
                }
                return View(new Tuple<sde.Models.soSyncList>(soS1List));
            }
        }
    }
}
        