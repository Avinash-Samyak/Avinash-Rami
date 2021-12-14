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

namespace sde.Controllers
{
    public class DashboardCpasController : Controller
    {
        //
        // GET: /Dashboard/
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");

        [Authorize(Roles="CPAS")]
        public ActionResult Index()
        {

            using (sdeEntities entities = new sdeEntities())
            {
                DateTime lastXDate = DateTime.Now.AddDays(-14);

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

                var allScheduler = scheduler1.Concat(scheduler2).Concat(scheduler3).ToList().Concat(scheduler4).ToList();
                SchedulerList scheList = new SchedulerList();
                scheList.scheduler = allScheduler;
                #endregion
                /*
                #region Request Netsuiet
                var requestnetsuite1 = (from n in entities.requestnetsuites
                                        where n.rn_status == "START" && n.rn_rangeFrom >= lastXDate && n.rn_rangeTo <= DateTime.Now
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
                                        where n.rn_status == "FINISHED" && n.rn_rangeFrom >= lastXDate && n.rn_rangeTo <= DateTime.Now
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
                                        where n.rn_status == "UPLOADED" && n.rn_rangeFrom >= lastXDate && n.rn_rangeTo <= DateTime.Now
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
                RequestNetsuiteList rnList = new RequestNetsuiteList();
                rnList.requestnetsuite = allRequestNetsuite;
                #endregion
                #region Message Queue
                var requestmq1 = (from n in entities.requestmqs
                                  where n.rmq_status == "START"
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
                                  where n.rmq_status == "FINISHED"
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
                                  where n.rmq_status == "UPLOADED"
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
                RequestMQList rmqList = new RequestMQList();
                rmqList.requestmq = allRequestMQ;
                #endregion
                */
                return View(new Tuple<sde.Models.SchedulerList>(scheList));
            }
        }

        [Authorize(Roles = "CPAS")]
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

        [Authorize(Roles = "CPAS")]
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

        [Authorize(Roles = "CPAS")]
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

        public Boolean login(NetSuiteService service)
        {

            service.Timeout = 820000000;
            service.CookieContainer = new CookieContainer();

            Passport passport = new Passport();
            passport.account = sde.Resource.NETSUITE_LOGIN_ACCOUNT;         //"3479023"
            passport.email = sde.Resource.NETSUITE_LOGIN_EMAIL;             // "xypang@scholastic.asia"

            RecordRef role = new RecordRef();
            role.internalId = sde.Resource.NETSUITE_LOGIN_ROLE_INTERNALID;  //"18"

            passport.role = role;
            passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;       //"Netsuite01"
            return service.login(passport).status.isSuccess;
        }


        [Authorize(Roles = "CPAS")]
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


        [Authorize(Roles = "CPAS")]
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

                    WriteResponse[] res = service.addList(iffList);
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
                    WriteResponse[] res = service.addList(invAdjList);
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
                    WriteResponse[] res = service.addList(invAdjList);
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
                    WriteResponse[] res = service.addList(jeList);
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

                                soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
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
                    WriteResponse[] res = service.addList(soList);
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


        [Authorize(Roles = "CPAS")]
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
                            WriteResponse[] writeRes = service.addList(invAdjList);
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

        [Authorize(Roles = "CPAS")]
        public ActionResult StockPosting(DateTime? toDate1 = null)
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Today;
                if (toDate1 == null)
                {
                    fromDate = DateTime.Today;
                    toDate = DateTime.Now;
                }
                else
                {
                    fromDate = Convert.ToDateTime(toDate1);
                    toDate = Convert.ToDateTime(toDate1).AddHours(23).AddMinutes(59).AddSeconds(59);
                }

                Session["FromDate"] = fromDate;
                Session["ToDate"] = toDate;

                var stockPosting = (from c in entities.cpas_stockposting
                                    join m in entities.map_location
                                    on c.spl_ml_location_internalID equals m.ml_location_internalID
                                    where (c.spl_createdDate > fromDate && c.spl_createdDate <= toDate)
                                    select new cls_stockPosting()
                                    {
                                        transactionType = c.spl_transactionType,
                                        sPID = c.spl_sPID,
                                        sDesc = c.spl_sDesc,
                                        dQty = c.spl_dQty,
                                        sLoc = c.spl_sLoc,
                                        subsidiary = c.spl_subsidiary,
                                        postingDate = c.spl_postingDate,
                                        createdDate = c.spl_createdDate
                                    }).OrderByDescending(x => x.createdDate);

                var stockPostingCount = stockPosting.Count();
                ViewBag.stockPostingCount = stockPostingCount;

                stockPostingList spList = new stockPostingList();
                spList.stockPosting = stockPosting.ToList();
                return View(new Tuple<sde.Models.stockPostingList>(spList));
            }
        }
    }
}
