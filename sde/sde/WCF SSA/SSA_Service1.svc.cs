using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Data.Odbc;
using sde.Models;
using log4net;
using System.Transactions;
using System.Net;
using System.Messaging;
using Sybase.Data.AseClient;

namespace sde.WCF_SSA
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "SSA_Service1" in code, svc and config file together.
    // NOTE: In order to launch WCF Test Client for testing this service, please select SSA_Service1.svc or SSA_Service1.svc.cs at the Solution Explorer and start debugging.
    public class SSA_Service1 : ISSA_Service1
    {
        private readonly ILog SSAPullPushMQLog = LogManager.GetLogger("SSAPullPushMQ");    //#361
        public static Int32 exceptionCount = 0;

        public void SSApullMQ()
        {
            MessageQueueTransaction msgTx = new MessageQueueTransaction();
            System.Messaging.Message objMessage = new System.Messaging.Message();

            string qname = @Resource.QUEUENAME_SSA;
            MessageQueue messageQueue = null;
            messageQueue = new MessageQueue(qname);
            this.SSAPullPushMQLog.Info("Connecting to " + qname);

            #region defineDataStructure
            Object o = new Object();
            System.Type[] arrTypes = new System.Type[19];

            arrTypes[0] = o.GetType();

            List<JOB> jList = new List<JOB>();
            arrTypes[1] = jList.GetType();

            List<JobMO> jmList = new List<JobMO>();
            arrTypes[2] = jmList.GetType();

            List<JobMoAddress> jmaList = new List<JobMoAddress>();
            arrTypes[3] = jmaList.GetType();

            List<JobMoPack> jmpList = new List<JobMoPack>();
            arrTypes[4] = jmpList.GetType();

            List<JobItem> jiList = new List<JobItem>();
            arrTypes[5] = jiList.GetType();

            List<JobOrdMaster> jomList = new List<JobOrdMaster>();
            arrTypes[6] = jomList.GetType();

            List<JobOrdMasterPack> jompList = new List<JobOrdMasterPack>();
            arrTypes[7] = jompList.GetType();

            List<JobOrdMasterPackDetail> jompdList = new List<JobOrdMasterPackDetail>();
            arrTypes[8] = jompdList.GetType();

            List<requestDataForm> reqData = new List<requestDataForm>();
            arrTypes[9] = reqData.GetType();

            List<PurchaseRequest> prList = new List<PurchaseRequest>();
            arrTypes[10] = prList.GetType();

            List<PurchaseRequestItem> priList = new List<PurchaseRequestItem>();
            arrTypes[11] = priList.GetType();

            List<cls_map_item> iiList = new List<cls_map_item>();
            arrTypes[12] = iiList.GetType();

            List<CashSales> csList = new List<CashSales>();
            arrTypes[13] = csList.GetType();

            List<CashSalesItem> csiList = new List<CashSalesItem>();
            arrTypes[14] = csiList.GetType();

            List<SOReturn> sorList = new List<SOReturn>();
            arrTypes[15] = sorList.GetType();

            List<SOReturnItem> sriList = new List<SOReturnItem>();
            arrTypes[16] = sriList.GetType();

            List<DiscountAndTax> datList = new List<DiscountAndTax>();
            arrTypes[17] = datList.GetType();

            List<JobMOCls> jmcList = new List<JobMOCls>();
            arrTypes[18] = jmcList.GetType();
            #endregion

            messageQueue.Formatter = new XmlMessageFormatter(arrTypes);

            try
            {
                msgTx.Begin();
                byte[] bt = new byte[10];
                objMessage = messageQueue.Receive(new TimeSpan(0, 10, 0), msgTx);

                this.SSAPullPushMQLog.Info("Extracting message from MQ with label: " + objMessage.Label.ToString());
                int indexFind = objMessage.Label.IndexOf(">");
                string jobMQ = objMessage.Label.Substring(indexFind);
                String insData = "FAIL";

                #region queue checking
                switch (jobMQ)
                {
                    case "> MQPUSH-JOB":
                        jList = ((List<JOB>)objMessage.Body);
                        insData = Job(jList);
                        break;
                    case "> MQPUSH-JOB MO":
                        jmList = ((List<JobMO>)objMessage.Body);
                        insData = JobMO(jmList);
                        break;
                    case "> MQPUSH-JOB MO CLS":
                        jmcList = ((List<JobMOCls>)objMessage.Body);
                        insData = JobMOCls(jmcList);
                        break;
                    case "> MQPUSH-JOB MO ADDRESS":
                        jmaList = ((List<JobMoAddress>)objMessage.Body);
                        insData = MoAddress(jmaList);
                        break;
                    case "> MQPUSH-JOB MO PACK":
                        jmpList = ((List<JobMoPack>)objMessage.Body);
                        insData = JobMoPack(jmpList);
                        break;
                    case "> MQPUSH-JOB ITEM":
                        jiList = ((List<JobItem>)objMessage.Body);
                        insData = JobItem(jiList);
                        break;
                    case "> MQPUSH-JOB ORD MASTER":
                        jomList = ((List<JobOrdMaster>)objMessage.Body);
                        insData = JobOrdMaster(jomList);
                        break;
                    case "> MQPUSH-JOB ORD MASTER PACK":
                        jompList = ((List<JobOrdMasterPack>)objMessage.Body);
                        insData = JobOrdMasterPack(jompList);
                        break;
                    case "> MQPUSH-JOB ORD MASTER PACK DETAIL":
                        jompdList = ((List<JobOrdMasterPackDetail>)objMessage.Body);
                        insData = JobOrdMasterPackDetail(jompdList);
                        break;
                    case "> MQPUSH-SALES FULFILLMENT":
                    case "> MQPUSH-SALES FULFILLMENT PM":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-JOBORDMASTER EXTRACTION"://Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-ITEM":
                        iiList = ((List<cls_map_item>)objMessage.Body);
                        insData = ImasItem(iiList);
                        break;
                    //Added to get daily created items - WY-17.OCT.2014
                    case "> MQPUSH-NEW ITEM":
                        iiList = ((List<cls_map_item>)objMessage.Body);
                        insData = ImasNewItem(iiList);
                        break;
                    case "> MQPUSH-BCAS STOCK POSTING":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-PURCHASE REQUEST":
                        prList = ((List<PurchaseRequest>)objMessage.Body);
                        insData = PurchaseRequest(prList);
                        break;
                    case "> MQPUSH-PURCHASE REQUEST ITEM":
                        priList = ((List<PurchaseRequestItem>)objMessage.Body);
                        insData = PurchaseRequestItem(priList);
                        break;
                    case "> MQPUSH-PO RECEIVE":
                    case "> MQPUSH-PO RECEIVE PM":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-PO RECEIVE ITEM":
                    case "> MQPUSH-PO RECEIVE ITEM PM":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-CASH SALES":
                        csList = ((List<CashSales>)objMessage.Body);
                        insData = CashSales(csList);
                        break;
                    case "> MQPUSH-CASH SALES ITEM":
                        csiList = ((List<CashSalesItem>)objMessage.Body);
                        insData = CashSalesItem(csiList);
                        break;
                    case "> MQPUSH-SO RETURN":
                        sorList = ((List<SOReturn>)objMessage.Body);
                        insData = SOReturn(sorList);
                        break;
                    case "> MQPUSH-SO RETURN ITEM":
                        sriList = ((List<SOReturnItem>)objMessage.Body);
                        insData = SOReturnItem(sriList);
                        break;
                    case "> MQPUSH-SO RETURN UPDATE":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    case "> MQPUSH-DISCOUNT AND TAX":
                        datList = ((List<DiscountAndTax>)objMessage.Body);
                        insData = DiscountAndTax(datList);
                        break;
                    default:
                        insData = "SUCCESS";
                        break;
                }
                #endregion

                if (insData != "SUCCESS")
                {
                    msgTx.Abort();
                    //break;
                }
                else
                {
                    msgTx.Commit();
                }
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Timeout"))
                {
                    this.SSAPullPushMQLog.Error("SSApullMQ: " + ex.ToString());
                }
                else
                {
                    this.SSAPullPushMQLog.Info("SSApullMQ: " + ex.ToString());
                }
                msgTx.Abort();
            }
        }

        #region Extract data from IMAS and push to queue
        private String CheckRequestDataForm(List<requestDataForm> reqData)
        {
            String insData = "FAIL";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            for (int i = 0; i < reqData.Count(); i++)
            {
                switch (reqData[i].dataType)
                {
                    case "MQPUSH-SALES FULFILLMENT":
                    case "MQPUSH-SALES FULFILLMENT PM":
                        insData = SOFulfillment(reqData);
                        break;
                    case "MQPUSH-JOBORDMASTER EXTRACTION"://Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        insData = JobOrdMasterExtraction(reqData);
                        break;
                    /*case "MQPUSH-BCAS STOCK POSTING":
                        insData = BCASStockPosting(reqData);
                        break;*/
                    case "MQPUSH-PO RECEIVE":
                    case "MQPUSH-PO RECEIVE PM":
                        insData = POReceive(reqData);
                        break;
                    case "MQPUSH-PO RECEIVE ITEM":
                    case "MQPUSH-PO RECEIVE ITEM PM":
                        insData = POReceiveItem(reqData);
                        break;
                    case "MQPUSH-SO RETURN UPDATE":
                        List<SOReturn> sorList = new List<SOReturn>();
                        sorList = SOReturnUpdate(reqData);
                        if (sorList.Count() > 0)
                        {
                            insData = SOReturnItemUpdate(sorList);
                        }
                        else
                        {
                            insData = "SUCCESS";
                        }
                        break;
                }
            }
            return insData;
        }
        #endregion

        #region Upload Data InTo SSA
        private String Job(List<JOB> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJob(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("Job Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("Job: Job insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobMO(List<JobMO> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobMO(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobMO Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobMO: Job mo insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobMOCls(List<JobMOCls> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
            try
            {
                insMoAddr = insertJobMOCls(dataList);
                //scopeOuter.Complete();
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error("JobMOCls Exception: " + ex.Message.ToString());
                insMoAddr = "FAIL";
            }
            //}

            this.SSAPullPushMQLog.Info("JobMOCls: Job mo cls insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String MoAddress(List<JobMoAddress> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobMoAddr(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("MoAddress Exception: " + ex.Message.ToString());
                    //this.SSAPullPushMQLog.Error("MO Address insertion having exception. Database rollback!");
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("MoAddress: MO Adrresss insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobMoPack(List<JobMoPack> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobMoPack(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobMoPack Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobMoPack: Job mo pack insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobItem(List<JobItem> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobItem(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobItem Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobItem: Job item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobOrdMaster(List<JobOrdMaster> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insertJobOrdMaster(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobOrdMaster Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobOrdMaster: Job order master insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobOrdMasterPack(List<JobOrdMasterPack> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobOrdMasterPack(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobOrdMasterPack Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobOrdMasterPack: Job order master pack insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String JobOrdMasterPackDetail(List<JobOrdMasterPackDetail> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertJobOrdMasterPackDetail(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("JobOrdMasterPackDetail Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("JobOrdMasterPackDetail: Job order master pack detail insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String PurchaseRequest(List<PurchaseRequest> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertPurchaseRequest(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("PurchaseRequest Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("PurchaseRequest: Purchase Request insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String PurchaseRequestItem(List<PurchaseRequestItem> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    //sdeEntities entities = new sdeEntities();
                    //insertJob(entities, dataList);
                    insMoAddr = insertPurchaseRequestItem(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("PurchaseRequestItem Exception : " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}
            this.SSAPullPushMQLog.Info("PurchaseRequestItem: Purchase Request Item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String ImasItem(List<cls_map_item> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertImasItem(dataList);
                    if (insMoAddr == "SUCCESS")
                    {
                        insertImasItemBusiness(dataList);
                    } 
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("ImasItem Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("ImasItem: New IMAS item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        //Added to get daily created items - WY-17.OCT.2014
        private String ImasNewItem(List<cls_map_item> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };
             
            try
            {
                insMoAddr = insertImasNewItem(dataList);
                if (insMoAddr == "SUCCESS")
                {
                    insertImasNewItemBusiness(dataList);
                }
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error("ImasNewItem Exception: " + ex.Message.ToString());
                insMoAddr = "FAIL";
            } 

            this.SSAPullPushMQLog.Info("ImasNewItem: New IMAS item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String CashSales(List<CashSales> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertCashSales(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("CashSales Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("CashSales: Cash sales insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String CashSalesItem(List<CashSalesItem> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertCashSalesItem(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("CashSalesItem Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("CashSalesItem: Cash sales item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String SOReturn(List<SOReturn> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertSOReturn(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("SOReturn Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("SOReturn: Sales order return insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String SOReturnItem(List<SOReturnItem> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertSOReturnItem(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("SOReturnItem Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("SOReturnItem: Sales order return item insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String DiscountAndTax(List<DiscountAndTax> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };
            //using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            //{
                try
                {
                    insMoAddr = insertDiscountAndTax(dataList);
                    //scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("DiscountAndTax Exception: " + ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            //}

            this.SSAPullPushMQLog.Info("DiscountAndTax: Discount and tax insertion status. " + insMoAddr);
            return insMoAddr;
        }

        private String insertJob(List<JOB> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        Int32 active = 0;
                        if (dataList[i].jobActive == true)
                        {
                            active = 1;
                        } 

                        //Checking if jobID exist then no need insert - WY-30.AUG.2014
                        String strChkJobID = "select count(job_id) from wms_job where job_id = '" + dataList[i].jobID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobID: " + strChkJobID);
                        command.CommandText = strChkJobID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 
                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            String str1 = "insert into wms_job (job_id, businesschannel_id, country_tag,createdby,createddate,job_active," +
                                            "job_description,job_mo_count,job_status) values ('" + dataList[i].jobID + "','" + dataList[i].businessChannel + "','" + dataList[i].countryTag + "','" + dataList[i].createdBy + "'," +
                                            "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "'," + active + ",'" + dataList[i].jobDesc + "'," + dataList[i].moCount + "," + dataList[i].status + ")";

                            this.SSAPullPushMQLog.Debug("insertJob: " + str1);
                            command.CommandText = str1;
                            int nNoAdded = command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobID + "','wms_job'," +
                                "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','job_id')";
                            this.SSAPullPushMQLog.Debug("insertJob: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJob Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobMO(List<JobMO> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    { 
                        //Checking if jobMoID exist then no need insert - WY-30.AUG.2014
                        String strChkJobMoID = " select count(jobmo_id) from wms_jobmo where jobmo_id = '" + dataList[i].jobMoID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobMoID: " + strChkJobMoID);
                        command.CommandText = strChkJobMoID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();
                         
                        if (tolJobID == 0)
                        {

                            Int32 credit = Convert.ToInt32(checkIsNullToZero(dataList[i].creditTerm));
                             
                            String sSQLCommand = "insert into wms_jobmo (recid,job_id,mono,jobmo_id,consignmentnote,clscnt,contactperson,country,deliveryadd,deliveryadd_2,deliveryadd_3,ordrecnocnt,postcode," +
                                "processperiod,schid,schname,schname_2,status,telno,ordweight,jobno,mocurrency,deliverytype,molisence,motransmode,creditterm,nscreditterm,shipdate) values (88880000,'" + dataList[i].jobID + "'," +
                                "'" + SplitMoNo(dataList[i].moNo) + "','" + dataList[i].jobMoID + "','" + checkIsNull(dataList[i].consignmentNote).Replace("'", "''") + "'," + dataList[i].clsCnt + ",'" + checkIsNull(dataList[i].contactPerson).Replace("'", "''") + "'," +
                                "'" + dataList[i].country + "','" + dataList[i].deliveryAdd.Replace("'", "''") + "','" + dataList[i].deliveryAdd2.Replace("'", "''") + "','" + checkIsNull(dataList[i].deliveryAdd3).Replace("'", "''") + "'," + dataList[i].ordRecNoCnt + "," +
                                "'" + dataList[i].postCode + "','" + dataList[i].processPeriod + "','" + checkIsNull(dataList[i].schID).Replace("'","''") + "','" + dataList[i].schName.Replace("'", "''") + "','" + checkIsNull(dataList[i].schName2).Replace("'", "''") + "','" + checkIsNull(dataList[i].status).Replace("'", "''") + "'," +
                                "'" + checkIsNull(dataList[i].telNo).Replace("'","''") + "'," + dataList[i].ordWeight + ",'" + checkIsNull(dataList[i].jobNo).Replace("'", "''") + "','" + checkIsNull(dataList[i].moCurrency).Replace("'", "''") + "','" + checkIsNull(dataList[i].deliveryType).Replace("'", "''") + "'," +
                                "'" + checkIsNull(dataList[i].moLisence).Replace("'", "''") + "','" + checkIsNull(dataList[i].moTransMode).Replace("'", "''") + "'," + Convert.ToInt32(checkIsNullToZero(dataList[i].creditTerm)) + "," + Convert.ToInt32(checkIsNullToZero(dataList[i].creditTerm)) + ", '" + dataList[i].shipdate + "' )";

                            this.SSAPullPushMQLog.Debug("insertJobMO: " + sSQLCommand);
                            command.CommandText = sSQLCommand;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobMoID + "','wms_jobmo'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobmo_id')";
                            this.SSAPullPushMQLog.Debug("insertJobMO: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobMO Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }            
            return status;
        }
        private String insertJobMOCls(List<JobMOCls> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";

            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        //Checking if jobMoClsID exist then no need insert - WY-30.AUG.2014
                        String strChkJobMoClsID = " select count(jobmocls_id) from wms_jobmocls where jobmocls_id = '" + dataList[i].jobMoClsID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobMoClsID: " + strChkJobMoClsID);
                        command.CommandText = strChkJobMoClsID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            String sSQLCommand = "insert into wms_jobmocls (jobmocls_id,job_id,jobmo_id,cls_number,teacher_name) values ('" + dataList[i].jobMoClsID + "','" + dataList[i].jobID + "'," +
                                "'" + dataList[i].jobMoID + "','" + dataList[i].clsNo + "','" + checkIsNull(dataList[i].teacherName).Replace("'", "''") + "')";

                            this.SSAPullPushMQLog.Debug("insertJobMOCls: " + sSQLCommand);
                            command.CommandText = sSQLCommand;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobMoClsID + "','wms_jobmocls'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobmocls_id')";
                            this.SSAPullPushMQLog.Debug("insertJobMOCls: " + str3);
                            command.CommandText = str3;

                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobMOCls Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobMoAddr(List<JobMoAddress> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        //Checking if jobMoAddID exist then no need insert - WY-30.AUG.2014
                        String strChkJobMoAddID = " select count(jobmoaddress_id) from wms_jobmo_address where jobmoaddress_id = '" + dataList[i].jobMoAddrID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobMoAddID: " + strChkJobMoAddID);
                        command.CommandText = strChkJobMoAddID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        { 
                            String sSQLCommand = "insert into wms_jobmo_address (jobmoaddress_id,jobmo_id,jobmoaddress_name,jobmoaddress_1,jobmoaddress_2,jobmoaddress_3,jobmoaddress_ref," +
                                "mono,jobmoaddress_contact,jobmoaddress_deliverytype,job_id,jobmoaddress_tag,jobmoaddress_tel,jobmoaddress_tel2,jobmoaddress_fax,jobmoaddress_4) values ('" + dataList[i].jobMoAddrID + "'," +
                                "'" + dataList[i].jobMoID + "','" + dataList[i].addrName.Replace("'", "''") + "','" + dataList[i].addr1.Replace("'", "''") + "','" + dataList[i].addr2.Replace("'", "''") + "'," +
                                "'" + dataList[i].addr3.Replace("'", "''") + "','" + dataList[i].addrRef.Replace("'", "''") + "','" + SplitMoNo(dataList[i].moNo) + "','" + dataList[i].contactName.Replace("'", "''") + "'," +
                                "'" + dataList[i].deliveryType + "','" + dataList[i].jobID + "','" + dataList[i].addrTag + "','" + dataList[i].addrTel + "','" + dataList[i].addrTel2 + "','" + dataList[i].addrFax + "', " +
                                "'" + dataList[i].addr4.Replace("'", "''") + "')";

                            this.SSAPullPushMQLog.Debug("insertJobMoAddr: " + sSQLCommand);
                            command.CommandText = sSQLCommand;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobMoAddrID + "','wms_jobmo_address'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobmoaddress_id')";
                            this.SSAPullPushMQLog.Debug("insertJobMoAddr: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobMoAddr Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobMoPack(List<JobMoPack> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        //Checking if jobMoPackID exist then no need insert - WY-30.AUG.2014
                        String strChkJobMoPackID = " select count(jobmopack_id) from wms_jobmo_pack where jobmopack_id = '" + dataList[i].jobMoPackID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobMoPackID: " + strChkJobMoPackID);
                        command.CommandText = strChkJobMoPackID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            String sSQLCommand = "insert into wms_jobmo_pack (recid,jobmopack_id,job_id,period,mono,schid,schname,packid,packtitles,packprice,qty,amt,packisbn) values (" + dataList[i].recID + "," +
                                "'" + dataList[i].jobMoPackID + "','" + dataList[i].jobID + "','" + dataList[i].period + "','" + SplitMoNo(dataList[i].moNo) + "','" + checkIsNull(dataList[i].schID).Replace("'","''") + "','" + dataList[i].schName.Replace("'", "''") + "'," +
                                "'" + dataList[i].packID + "','" + dataList[i].packTitles.Replace("'", "''") + "'," + dataList[i].packPrice + "," + dataList[i].qty + "," + dataList[i].amount + ",'" + dataList[i].packISBN + "')";

                            this.SSAPullPushMQLog.Debug("insertJobMoPack: " + sSQLCommand);
                            command.CommandText = sSQLCommand;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobMoPackID + "','wms_jobmo_pack'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobmopack_id')";
                            this.SSAPullPushMQLog.Debug("insertJobMoPack: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobMoPack Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertImasItem(List<cls_map_item> itemList)
        {
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                //OdbcCommand cmd2 = oCon.CreateCommand();

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    this.SSAPullPushMQLog.Debug("updateImasItemBusiness: Total records is " + itemList.Count());
                    for (int i = 0; i < itemList.Count(); i++)
                    {
                        String tempItemID = itemList[i].mi_item_internalID;
                        command.CommandText = "select count(item_id) from imas_item where netsuiteInternal_Id = '" + tempItemID + "'";
                        int count = (int)command.ExecuteScalar();

                        this.SSAPullPushMQLog.Debug("updateImasItemBusiness: check against existing record is " + count);
                        if (count < 1)
                        {
                            String str1 = "insert into imas_item (item_id,item_description,item_title,item_uom,item_isbn,item_isbn_secondary,modifieddate,createddate," +
                                "item_reorder_level,item_reorder_qty,item_reorder_date,createdby,item_weight,accountclass_id,modifiedby,netsuiteInternal_Id,prodfamily) values (" +
                                "'" + itemList[i].mi_item_ID + "', '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "','" + checkIsNull(itemList[i].mi_item_title).Replace("'", "''") + "'," +
                                "'" + checkIsNull(itemList[i].mi_item_uom) + "','" + checkIsNull(itemList[i].mi_item_isbn) + "','" + checkIsNull(itemList[i].mi_isbn_secondary) + "'," +
                                "'" + itemList[i].mi_lastModifiedDate.Value.ToString("MMM dd yyyy hh:mm tt") + "','" + itemList[i].mi_createdDate.Value.ToString("MMM dd yyyy hh:mm tt") + "'," +
                                "" + CheckIsZero(Convert.ToDecimal(itemList[i].mi_item_reorder_level)) + "," + CheckIsZero(Convert.ToDecimal(itemList[i].mi_reorder_qty)) + ",'" + itemList[i].mi_reorder_date.Value.ToString("MMM dd yyyy hh:mm tt") + "'," +
                                "'" + "NETSUITE" + "'," + CheckIsZero(itemList[i].mi_item_weight) + ",'" + checkIsNull(itemList[i].mi_accountClassID) + "','NETSUITE','" + itemList[i].mi_item_internalID + "','" + checkIsNull(itemList[i].mi_prodfamily).Replace("'", "''") + "')";

                            this.SSAPullPushMQLog.Debug("insertImasItem: " + str1);
                            command.CommandText = str1;
                            command.ExecuteNonQuery();

                            //String str2 = "insert into imas_itembusiness (itembusiness_id,item_id,createdby,createddate,itembusiness_code,itembusiness_description,itembusiness_status) " +
                            //  " values ('" + newguid + "','" + itemList[i].mi_item_ID + "','NETSUITE','" + DateTime.Now.ToString("MMM dd yyyy hh:mm tt") + "','NETSUITE','" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "',1)";

                            //this.SSAPullPushMQLog.Debug("insertImasItemBusiness: " + str2);
                            //command.CommandText = str2;
                            //command.ExecuteNonQuery(); 
                        }
                        //Update item if exist - WY-08.OCT.2014
                        else
                        {
                            String str1 = "update imas_item SET item_description = '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "', item_title = '" + checkIsNull(itemList[i].mi_item_title).Replace("'", "''") + "', " +
                                          " item_uom = '" + checkIsNull(itemList[i].mi_item_uom) + "',item_isbn_secondary = '" + checkIsNull(itemList[i].mi_isbn_secondary) + "', " +
                                          " modifieddate = '" + itemList[i].mi_lastModifiedDate.Value.ToString("MMM dd yyyy hh:mm tt") + "', modifiedby = 'NETSUITE'," +
                                          " item_weight = " + CheckIsZero(itemList[i].mi_item_weight) + ",accountclass_id = '" + checkIsNull(itemList[i].mi_accountClassID) + "', " +
                                          " prodfamily = '" + checkIsNull(itemList[i].mi_prodfamily).Replace("'", "''") + "', " +
                                          " item_isbn = '" + checkIsNull(itemList[i].mi_item_isbn) + "' " +
                                          " where netsuiteInternal_Id = '" + tempItemID + "'";

                            this.SSAPullPushMQLog.Debug("updateImasItem: " + str1);
                            command.CommandText = str1;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                    this.SSAPullPushMQLog.Debug("updateImasItem: commit is " + status);
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertImasItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertImasItemBusiness(List<cls_map_item> itemList)//Split itembusiness - WY-08.OCT.2014
        {
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < itemList.Count(); i++)
                    {
                        String tempItemID = itemList[i].mi_item_isbn;
                        String[] bizName = checkIsNull(itemList[i].mi_businesschannel_name).Split(';');
                        String[] bizInternalId = checkIsNull(itemList[i].mi_businesschannel_InternalID).Split(';');
                        Decimal rrpbcprice = 0;

                        String updStatus = "update imas_itembusiness set itembusiness_status = 0 where itembusiness_code =  '" + tempItemID + "' and itembusiness_status <> 2";
                        this.SSAPullPushMQLog.Debug("updateImasItemBusinessStatus: " + updStatus);
                        command.CommandText = updStatus;
                        command.ExecuteNonQuery();

                        for (int a = 0; a < bizInternalId.Count(); a++)
                        {
                            String bizChannelID = string.Empty;
                            String bizChannelName = string.Empty;
                            Boolean isValid = true;
                            Guid newguid = Guid.NewGuid();
                            rrpbcprice = 0;

                            if (bizInternalId[a] == @Resource.LOB_BOOKFAIR_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_BOOKFAIR_BIZCHANNELID;//Book Fair
                            }
                            else
                                if (bizInternalId[a] == @Resource.LOB_BOOKCLUBS_INTERNALID)
                                {
                                    bizChannelID = @Resource.LOB_BOOKCLUBS_BIZCHANNELID;//Book Clubs
                                    rrpbcprice = Convert.ToDecimal(itemList[i].mip_item_price);//Only Book Clubs need the price
                                }
                                else
                                    if (bizInternalId[a] == @Resource.LOB_TRADE_INTERNALID)
                                    {
                                        bizChannelID = @Resource.LOB_TRADE_BIZCHANNELID;//Trade
                                    }
                                    else
                                        if (bizInternalId[a] == @Resource.LOB_EDUCATION_INTERNALID)
                                        {
                                            bizChannelID = @Resource.LOB_EDUCATION_BIZCHANNELID;//Education
                                        }
                                        else
                                            if (bizInternalId[a] == @Resource.LOB_DIRECTSALES_INTERNALID)
                                            {
                                                bizChannelID = @Resource.LOB_DIRECTSALES_BIZCHANNELID;//Direct Sales
                                            }
                                            else
                                            {
                                                bizChannelName = bizName[a];
                                                isValid = false;
                                                //this.SSAPullPushMQLog.Fatal("The item " + tempItemID + "(" + itemList[i].mi_item_description + ") with Line of Business = " + bizChannelName + "(" + bizInternalId[a] + ") can't match with the Line of Business in SSA.");
                                            }

                            command.CommandText = "select count(item.item_id) from imas_itembusiness biz join imas_item item on biz.item_id = item.item_id where item.item_isbn = '" + tempItemID + "'and biz.businesschannel_id = '" + bizChannelID + "'  and itembusiness_status <> 2 ";
                            int count = (int)command.ExecuteScalar();

                            if (count < 1)
                            {
                                if (isValid == true)
                                {
                                    command.CommandText = "select item.item_id from imas_item item where item.item_isbn = '" + tempItemID + "'";
                                    String imasItemId = (String)command.ExecuteScalar();

                                    String str2 = "insert into imas_itembusiness (itembusiness_id,item_id,createdby,createddate,itembusiness_code,itembusiness_description,itembusiness_status,businesschannel_id," +
                                                  "itembusiness_taxschedule,itembusiness_taxcode,itembusiness_rrpbcprice) " +
                                                  " values ('" + newguid + "','" + imasItemId + "','NETSUITE','" + DateTime.Now.ToString("MMM dd yyyy hh:mm tt") + "','" + itemList[i].mi_item_isbn + "','" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "',1," +
                                                  "'" + bizChannelID + "','" + checkIsNull(itemList[i].mi_tax_schedule).Replace("'", "''") + "','" + checkIsNull(itemList[i].mi_tax_code).Replace("'", "''") + "'," + rrpbcprice + ")";

                                    this.SSAPullPushMQLog.Debug("insertImasItemBusiness: " + str2);
                                    command.CommandText = str2;
                                    command.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                if (isValid == true)
                                {
                                    String str3 = "update imas_itembusiness set imas_itembusiness.itembusiness_status = 1, imas_itembusiness.itembusiness_description = '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "', " +
                                                   "imas_itembusiness.modifiedby = 'NETSUITE', imas_itembusiness.modifieddate = '" + DateTime.Now.ToString("MMM dd yyyy hh:mm tt") + "', " +
                                                   " imas_itembusiness.itembusiness_taxschedule = '" + checkIsNull(itemList[i].mi_tax_schedule).Replace("'", "''") + "', " +
                                                   " imas_itembusiness.itembusiness_taxcode = '" + checkIsNull(itemList[i].mi_tax_code).Replace("'", "''") + "', " +
                                                   " imas_itembusiness.itembusiness_rrpbcprice = " + rrpbcprice +
                                                   " from imas_itembusiness, imas_item where imas_itembusiness.item_id = imas_item.item_id and imas_item.item_isbn = '" + tempItemID + "' and imas_itembusiness.businesschannel_id = '" + bizChannelID + "' and imas_itembusiness.itembusiness_status  <> 2 ";

                                    this.SSAPullPushMQLog.Debug("updateImasItemBusiness: " + str3);
                                    command.CommandText = str3;
                                    command.ExecuteNonQuery();
                                }
                            }
                        }

                    }
                    transaction.Commit();
                    status = "SUCCESS";
                    this.SSAPullPushMQLog.Debug("updateImasItemBusiness: Commit is " + status);
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertImasItemBusiness Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        //Added to get daily created items and item business - WY-17.OCT.2014
        private String insertImasNewItem(List<cls_map_item> itemList)
        {
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    this.SSAPullPushMQLog.Debug("updateImasNewItem: total records are " + itemList.Count());
                    for (int i = 0; i < itemList.Count(); i++)
                    {
                        String tempItemID = itemList[i].mi_item_internalID;
                        command.CommandText = "select count(item_id) from imas_item where netsuiteInternal_Id = '" + tempItemID + "'";
                        int count = (int)command.ExecuteScalar();

                        this.SSAPullPushMQLog.Debug("updateImasNewItem: check against existing record is " + count);
                        if (count < 1)
                        {
                            String str1 = "insert into imas_item (item_id,item_description,item_title,item_uom,item_isbn,item_isbn_secondary,modifieddate,createddate," +
                                "item_reorder_level,item_reorder_qty,item_reorder_date,createdby,item_weight,accountclass_id,modifiedby,netsuiteInternal_Id,prodfamily) values (" +
                                "'" + itemList[i].mi_item_ID + "', '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "','" + checkIsNull(itemList[i].mi_item_title).Replace("'", "''") + "'," +
                                "'" + checkIsNull(itemList[i].mi_item_uom) + "','" + checkIsNull(itemList[i].mi_item_isbn) + "','" + checkIsNull(itemList[i].mi_isbn_secondary) + "'," +
                                "'" + itemList[i].mi_lastModifiedDate.Value.ToString("MMM dd yyyy hh:mm tt") + "','" + itemList[i].mi_createdDate.Value.ToString("MMM dd yyyy hh:mm tt") + "'," +
                                "" + CheckIsZero(Convert.ToDecimal(itemList[i].mi_item_reorder_level)) + "," + CheckIsZero(Convert.ToDecimal(itemList[i].mi_reorder_qty)) + ",'" + itemList[i].mi_reorder_date.Value.ToString("MMM dd yyyy hh:mm tt") + "'," +
                                "'" + "NETSUITE" + "'," + CheckIsZero(itemList[i].mi_item_weight) + ",'" + checkIsNull(itemList[i].mi_accountClassID) + "','NETSUITE','" + itemList[i].mi_item_internalID + "','" + checkIsNull(itemList[i].mi_prodfamily).Replace("'", "''") + "')";

                            this.SSAPullPushMQLog.Debug("insertImasNewItem: " + str1);
                            command.CommandText = str1;
                            command.ExecuteNonQuery();
                        }
                        else
                        {
                            String str1 = "update imas_item SET item_description = '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "', item_title = '" + checkIsNull(itemList[i].mi_item_title).Replace("'", "''") + "', " +
                                          " item_uom = '" + checkIsNull(itemList[i].mi_item_uom) + "',item_isbn_secondary = '" + checkIsNull(itemList[i].mi_isbn_secondary) + "', " +
                                          " modifieddate = '" + itemList[i].mi_lastModifiedDate.Value.ToString("MMM dd yyyy hh:mm tt") + "', modifiedby = 'NETSUITE'," +
                                          " item_weight = " + CheckIsZero(itemList[i].mi_item_weight) + ",accountclass_id = '" + checkIsNull(itemList[i].mi_accountClassID) + "', " +
                                          " prodfamily = '" + checkIsNull(itemList[i].mi_prodfamily).Replace("'", "''") + "', " +
                                          " item_isbn = '" + checkIsNull(itemList[i].mi_item_isbn) + "' " +
                                          " where netsuiteInternal_Id = '" + tempItemID + "'";

                            this.SSAPullPushMQLog.Debug("updateImasNewItem: " + str1);
                            command.CommandText = str1;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                    this.SSAPullPushMQLog.Debug("updateImasNewItem: commit is " + status);

                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertImasNewItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertImasNewItemBusiness(List<cls_map_item> itemList) 
        {
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < itemList.Count(); i++)
                    {
                        String tempItemID = itemList[i].mi_item_isbn;
                        String[] bizName = checkIsNull(itemList[i].mi_businesschannel_name).Split(';');
                        String[] bizInternalId = checkIsNull(itemList[i].mi_businesschannel_InternalID).Split(';');
                        Decimal rrpbcprice = 0;

                        String updStatus = "update imas_itembusiness set itembusiness_status = 0 where itembusiness_code =  '" + tempItemID + "' and itembusiness_status <> 2";
                        this.SSAPullPushMQLog.Debug("updateImasItemBusinessStatus: " + updStatus);
                        command.CommandText = updStatus;
                        command.ExecuteNonQuery();

                        for (int a = 0; a < bizInternalId.Count(); a++)
                        {
                            String bizChannelID = string.Empty;
                            String bizChannelName = string.Empty;
                            Boolean isValid = true;
                            Guid newguid = Guid.NewGuid();
                            rrpbcprice = 0;

                            if (bizInternalId[a] == @Resource.LOB_BOOKFAIR_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_BOOKFAIR_BIZCHANNELID;//Book Fair
                            }
                            else
                            if (bizInternalId[a] == @Resource.LOB_BOOKCLUBS_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_BOOKCLUBS_BIZCHANNELID;//Book Clubs
                                rrpbcprice = Convert.ToDecimal(itemList[i].mip_item_price);//Only Book Clubs need the price
                            }
                            else
                            if (bizInternalId[a] == @Resource.LOB_TRADE_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_TRADE_BIZCHANNELID;//Trade
                            }
                            else
                            if (bizInternalId[a] == @Resource.LOB_EDUCATION_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_EDUCATION_BIZCHANNELID;//Education
                            }
                            else
                            if (bizInternalId[a] == @Resource.LOB_DIRECTSALES_INTERNALID)
                            {
                                bizChannelID = @Resource.LOB_DIRECTSALES_BIZCHANNELID;//Direct Sales
                            }
                            else
                            {
                                bizChannelName = bizName[a];
                                isValid = false;
                                //this.SSAPullPushMQLog.Fatal("The item " + tempItemID + "(" + itemList[i].mi_item_description + ") with Line of Business = " + bizChannelName + "(" + bizInternalId[a] + ") can't match with the Line of Business in SSA.");
                            }

                            command.CommandText = "select count(item.item_id) from imas_itembusiness biz join imas_item item on biz.item_id = item.item_id where item.item_isbn = '" + tempItemID + "'and biz.businesschannel_id = '" + bizChannelID + "'  and itembusiness_status <> 2 ";
                            int count = (int)command.ExecuteScalar();

                            if (count < 1)
                            {
                                if (isValid == true)
                                {
                                    command.CommandText = "select item.item_id from imas_item item where item.item_isbn = '" + tempItemID + "'";
                                    String imasItemId = (String)command.ExecuteScalar();

                                    String str2 = "insert into imas_itembusiness (itembusiness_id,item_id,createdby,createddate,itembusiness_code,itembusiness_description,itembusiness_status,businesschannel_id," +
                                                  "itembusiness_taxschedule,itembusiness_taxcode,itembusiness_rrpbcprice) " +
                                                  " values ('" + newguid + "','" + imasItemId + "','NETSUITE','" + DateTime.Now.ToString("MMM dd yyyy hh:mm tt") + "','" + itemList[i].mi_item_isbn + "','" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "',1," +
                                                  "'" + bizChannelID + "','" + checkIsNull(itemList[i].mi_tax_schedule).Replace("'", "''") + "','" + checkIsNull(itemList[i].mi_tax_code).Replace("'", "''") + "'," + rrpbcprice + ")";
                                     
                                    this.SSAPullPushMQLog.Debug("insertImasNewItemBusiness: " + str2);
                                    command.CommandText = str2;
                                    command.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                if (isValid == true)
                                {
                                    String str3 = "update imas_itembusiness set imas_itembusiness.itembusiness_status = 1, imas_itembusiness.itembusiness_description = '" + checkIsNull(itemList[i].mi_item_description).Replace("'", "''") + "', " +
                                                   "imas_itembusiness.modifiedby = 'NETSUITE', imas_itembusiness.modifieddate = '" + DateTime.Now.ToString("MMM dd yyyy hh:mm tt") + "', " +
                                                   " imas_itembusiness.itembusiness_taxschedule = '" + checkIsNull(itemList[i].mi_tax_schedule).Replace("'", "''") + "', " +
                                                   " imas_itembusiness.itembusiness_taxcode = '" + checkIsNull(itemList[i].mi_tax_code).Replace("'", "''") + "', " +
                                                   " imas_itembusiness.itembusiness_rrpbcprice = " + rrpbcprice +
                                                   " from imas_itembusiness, imas_item where imas_itembusiness.item_id = imas_item.item_id and imas_item.item_isbn = '" + tempItemID + "' and imas_itembusiness.businesschannel_id = '" + bizChannelID + "' and imas_itembusiness.itembusiness_status  <> 2 ";

                                    this.SSAPullPushMQLog.Debug("updateImasNewItemBusiness: " + str3);
                                    command.CommandText = str3;
                                    command.ExecuteNonQuery();
                                }
                            }
                        }

                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertImasNewItemBusiness Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobItem(List<JobItem> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                List<String> itemIDList = new List<String>();
                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        String tempItemID = dataList[i].itemID;
                         

                        //OdbcCommand cmd0 = oCon.CreateCommand();
                        command.CommandText = "select item_id from imas_item where item_isbn = '" + tempItemID + "'";
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            itemIDList.Add(reader1.GetString(0));
                            //itemID = reader1.GetString(0);                            
                        }
                        else
                        {
                            itemIDList.Add("");
                        }
                        reader1.Close();
                    }                    

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        String itemID = itemIDList[i];

                        //Checking if jobItemID exist then no need insert - WY-30.AUG.2014
                        String strChkJobItemID = " select count(jobitem_id) from wms_jobitem where jobitem_id = '" + dataList[i].jobItemID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobItemID: " + strChkJobItemID);
                        command.CommandText = strChkJobItemID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            if (!String.IsNullOrEmpty(itemID))
                            { 
                                String str1 = "insert into wms_jobitem (jobitem_id,job_id,createdby,createddate,item_id,item_qty,posting_type,mono) values ('" + dataList[i].jobItemID + "'," +
                                        "'" + dataList[i].jobID + "','" + dataList[i].createdBy + "','" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','" + itemID + "'," + dataList[i].itemQty + ",'" + dataList[i].postingType + "'," +
                                        "'" + SplitMoNo(dataList[i].moNo) + "')";

                                this.SSAPullPushMQLog.Debug("insertJobItem: " + str1);
                                command.CommandText = str1;
                                command.ExecuteNonQuery();

                                String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].jobItemID + "','wms_jobitem'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobitem_id')";
                                this.SSAPullPushMQLog.Debug("insertJobItem: " + str3);
                                command.CommandText = str3;
                                command.ExecuteNonQuery();
                            }
                        }
                    }

                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;

        }
        private String insertJobOrdMaster(List<JobOrdMaster> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "SUCCESS";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {       
                        //Checking if jobOrdMasterID exist then no need insert - WY-30.AUG.2014
                        String strChkJobOrdMasterID = " select count(jobordmaster_id) from wms_jobordmaster where jobordmaster_id = '" + dataList[i].ordMasterID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobOrdMasterID: " + strChkJobOrdMasterID);
                        command.CommandText = strChkJobOrdMasterID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            // Get ascii character.
                            /*
                            String student = checkIsNull(dataList[i].ordStudent).Replace("'", "''");
                            this.SSAPullPushMQLog.Debug("insertJobOrdMaster: " + student);

                            char LastChar = char.Parse(student.Substring(student.Length, 1));
                            this.SSAPullPushMQLog.Debug("insertJobOrdMaster: " + LastChar);

                            int ascii_code = (int)LastChar;
                            this.SSAPullPushMQLog.Debug("insertJobOrdMaster: " + ascii_code);

                            if (ascii_code == 92)
                            {
                                student = student.Substring(0, student.Length-1);
                            }
                            */

                            String sSQLCommand = "";
                            if (dataList[i].ordRecNo.Length>8)
                            {
                                sSQLCommand = "insert into wms_jobordmaster (recid,jobordmaster_id,job_id,ordrecno,ordstudent,clsid,mono,consignmentnote,processperiod,country,jobmo_id,memo,jobmocls_id)" +
                                    " values (" + dataList[i].recID + ",'" + dataList[i].ordMasterID + "','" + dataList[i].jobID + "','" + dataList[i].ordRecNo.Substring(1,8) + "','" + checkIsNull(dataList[i].ordStudent).Replace("'", "''") + "'," +
                                    "'" + dataList[i].clsID.Replace("'", "''") + "','" + SplitMoNo(dataList[i].moNo) + "','" + dataList[i].consignmentNote + "','" + dataList[i].processPeriod + "','" + dataList[i].country + "','" + dataList[i].jobMoID + "','" + checkIsNull(dataList[i].memo).Replace("'", "''") + "', '" + dataList[i].jobCls_id + "')";
                            }
                            else
                            {
                                sSQLCommand = "insert into wms_jobordmaster (recid,jobordmaster_id,job_id,ordrecno,ordstudent,clsid,mono,consignmentnote,processperiod,country,jobmo_id,memo,jobmocls_id)" +
                                    " values (" + dataList[i].recID + ",'" + dataList[i].ordMasterID + "','" + dataList[i].jobID + "','" + dataList[i].ordRecNo + "','" + checkIsNull(dataList[i].ordStudent).Replace("'", "''") + "'," +
                                    "'" + dataList[i].clsID + "','" + SplitMoNo(dataList[i].moNo) + "','" + dataList[i].consignmentNote + "','" + dataList[i].processPeriod + "','" + dataList[i].country + "','" + dataList[i].jobMoID + "','" + checkIsNull(dataList[i].memo).Replace("'", "''") + "', '" + dataList[i].jobCls_id + "')";
                            }

                            this.SSAPullPushMQLog.Debug("insertJobOrdMaster //: " + sSQLCommand);
                            command.CommandText = sSQLCommand;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].ordMasterID + "','wms_jobordmaster'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobordmaster_id')";
                            this.SSAPullPushMQLog.Debug("insertJobOrdMaster: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobOrdMaster Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobOrdMasterPack(List<JobOrdMasterPack> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        //Checking if jobOrdMasterPackID exist then no need insert - WY-30.AUG.2014
                        String strChkJobOrdMasterPackID = " select count(jobordmaster_pack_id) from wms_jobordmaster_pack where jobordmaster_pack_id = '" + dataList[i].ordMasterPackID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobOrdPackMasterID: " + strChkJobOrdMasterPackID);
                        command.CommandText = strChkJobOrdMasterPackID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            String str1 = "insert into wms_jobordmaster_pack (jobordmaster_pack_id,jobordmaster_id,job_id,ordno,ordpack,ordqty,ordprice,ordreplace,ofrcode," +
                                "status,ordpackstatus,ordfulfill,ordpoint,packtitle,ordrate,tax,discount,basedprice,pricelevel) values ('" + dataList[i].ordMasterPackID + "','" + dataList[i].ordMasterID + "'," +
                                "'" + dataList[i].jobID + "','" + SplitMoNo(dataList[i].ordNo) + "','" + dataList[i].ordPack.Replace("'", "''") + "'," + dataList[i].ordQty + "," + dataList[i].ordPrice + ",'" + dataList[i].ordReplace + "'," +
                                "'" + dataList[i].ofrCode.Replace("'", "''") + "','" + dataList[i].status + "','" + dataList[i].ordPackStatus + "'," + dataList[i].ordFulfill + "," + dataList[i].ordPoint + "," +
                                "'" + dataList[i].packTitle.Replace("'", "''") + "',"  + dataList[i].ordRate + "," + dataList[i].tax + "," + dataList[i].discount + "," + dataList[i].basedPrice + ",'" + dataList[i].priceLevel + "')";
                            this.SSAPullPushMQLog.Debug("insertJobOrdMasterPack: " + str1);
                            command.CommandText = str1;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str2 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].ordMasterPackID + "','wms_jobordmaster_pack'," +
                               "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobordmaster_pack_id')";
                            this.SSAPullPushMQLog.Debug("insertJobOrdMasterPack: " + str2);
                            command.CommandText = str2;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobOrdMasterPack Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertJobOrdMasterPackDetail(List<JobOrdMasterPackDetail> dataList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                List<String> itemList = new List<String>();
                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        String tempItemID = dataList[i].itemID;
                        
                        //OdbcCommand cmd0 = oCon.CreateCommand();
                        command.CommandText = "select item_id from imas_item where item_isbn = '" + tempItemID + "'";
                        OdbcDataReader reader1 = command.ExecuteReader();
                        if (reader1.Read())
                        {
                            itemList.Add(reader1.GetString(0));
                        }
                        else
                        {
                            itemList.Add("");
                        }
                        reader1.Close();
                    }

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        String itemID = itemList[i];
                        
                        //Checking if jobOrdMasterPackDetailID exist then no need insert - WY-30.AUG.2014
                        String strChkJobOrdMasterPackDetailID = " select count(jobordmaster_packdetail_id) from wms_jobordmaster_packdetail where jobordmaster_packdetail_id = '" + dataList[i].ordMasterPackDetailID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobOrdPackMasterDetailID: " + strChkJobOrdMasterPackDetailID);
                        command.CommandText = strChkJobOrdMasterPackDetailID; 
                        OdbcDataReader reader1 = command.ExecuteReader();
                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 
                        reader1.Close();

                        if (tolJobID == 0)
                        {
                            if (!String.IsNullOrEmpty(itemID))
                            {
                                String str1 = "insert into wms_jobordmaster_packdetail (jobordmaster_packdetail_id,jobordmaster_pack_id,job_id,ordpack,skuno,isbn,isbn_secondary,sku_qty,item_id,total_qty,scanned_qty) " +
                                        "values ('" + dataList[i].ordMasterPackDetailID + "','" + dataList[i].ordMasterPackID + "','" + dataList[i].jobID + "','" + dataList[i].ordPack.Replace("'", "''") + "'," +
                                        "'" + dataList[i].skuNo + "','" + dataList[i].isbn + "','" + dataList[i].isbnSecondary + "'," + dataList[i].skuQty + ",'" + itemID + "'," + dataList[i].totalQty + ",0)";
                                this.SSAPullPushMQLog.Debug("insertJobOrdMasterPackDetail: " + str1);
                                command.CommandText = str1;
                                command.ExecuteNonQuery();

                                String str2 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + dataList[i].ordMasterPackDetailID + "','wms_jobordmaster_packdetail'," +
                                    "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','jobordmaster_packdetail_id')";
                                this.SSAPullPushMQLog.Debug("insertJobOrdMasterPackDetail: " + str2);
                                command.CommandText = str2;
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertJobOrdMasterPackDetail Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertPurchaseRequest(List<PurchaseRequest> prList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < prList.Count(); i++)
                    {
                        Int32 active = 0, exported = 0;
                        if (prList[i].active == true)
                        {
                            active = 1;
                        }
                        if (prList[i].exported == true)
                        {
                            exported = 1;
                        }
                        String[] tempVendor = prList[i].supplier.Split(' ');

                        String str1 = "insert into imas_pr (pr_id,pr_number,pr_desc,pr_requestor,pr_site,pr_comments,pr_supplier,pr_approvaltype,pr_accountclass,pr_active,pr_date,pr_neededdate," +
                            "pr_day,pr_month,pr_year,createdby,createddate,pr_email,pr_exported,pr_status,pr_deletereason,pr_deliverymethod,businesschannel_id) values ('" + prList[i].prID + "'," +
                            "'" + prList[i].prNumber + "','" + prList[i].desc + "','" + prList[i].requestor + "','" + prList[i].site + "','" + prList[i].comments + "','" + tempVendor[0] + "'," +
                            //"'" + prList[i].approvalType + "','" + prList[i].accoutClass + "'," + active + ",'" + prList[i].date + "','" + prList[i].neededDate + "'," + prList[i].day + "," +
                            "'" + prList[i].approvalType + "','" + prList[i].accoutClass + "'," + active + ",'" + Convert.ToDateTime(prList[i].date).ToString("MMM dd yyyy hh:mm tt") + "','" + Convert.ToDateTime(prList[i].neededDate).ToString("MMM dd yyyy hh:mm tt") + "'," + prList[i].day + "," +
                            "" + prList[i].month + "," + prList[i].year + ",'" + prList[i].requestor + "','" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','" + prList[i].email + "'," + exported + "," +
                            "" + prList[i].status + ",'" + prList[i].deleteReason + "','" + prList[i].deliveryMethod + "','" + prList[i].businessChannelID + "')";

                        this.SSAPullPushMQLog.Debug("insertPurchaseRequest: " + str1);
                        command.CommandText = str1;
                        command.ExecuteNonQuery();

                        String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + prList[i].prID + "','imas_pr'," +
                           "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','pr_id')";
                        this.SSAPullPushMQLog.Debug("insertPurchaseRequest: " + str3);
                        command.CommandText = str3;
                        command.ExecuteNonQuery();
                    }

                    String str2 = "update imas_pr set pr_supplier = (select supplier_id from imas_supplier where supplier_code = imas_pr.pr_supplier) where createddate='" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "'";
                    this.SSAPullPushMQLog.Debug("insertPurchaseRequest: " + str2);
                    command.CommandText = str2;
                    int nNoAdded = command.ExecuteNonQuery();

                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertPurchaseRequest Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertPurchaseRequestItem(List<PurchaseRequestItem> priList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                List<String> itemList = new List<String>();
                List<String> itemBusinessList = new List<String>();

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + priList.Count());
                    for (int i = 0; i < priList.Count(); i++)
                    {
                        String tempItemID = priList[i].itemID;
                        String tempItemBizID = priList[i].itemBusinessID;

                        this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + tempItemID);
                        this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + tempItemBizID);

                        command.CommandText = "select ib.item_id, ib.itembusiness_id from imas_itembusiness ib join imas_item i on ib.item_id = i.item_id " +
                            "where i.item_isbn = '" + tempItemID + "' and ib.businesschannel_id = '" + tempItemBizID + "'";//Added businesschannel_id condition - WY-14.OCT.2014
                        //command.CommandText = "select item_id from imas_item where item_isbn = '" + priList[i].itemID + "'";
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            itemList.Add(reader1.GetString(0));
                            itemBusinessList.Add(reader1.GetString(1));
                            //itemBusinessList.Add("");
                        }
                        else
                        {
                            itemList.Add("");
                            itemBusinessList.Add("");

                        }
                        reader1.Close();
                    }

                    for (int i = 0; i < priList.Count(); i++)
                    {
                        String itemID = itemList[i];
                        String itemBuisness = itemBusinessList[i];

                        this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + itemID);
                        this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + itemBuisness);

                        if (!String.IsNullOrEmpty(itemID))
                        {
                            Int32 converted = 0, approved = 0;
                            if (priList[i].converted == true)
                            {
                                converted = 1;
                            }
                            if (priList[i].approved == true)
                            {
                                approved = 1;
                            }
                            String str1 = "insert into imas_pritem (pritem_id,pr_id,item_id,itembusiness_id,pritem_qty,pritem_price,pritem_converted,pritem_approved,pritem_approvedate," +
                                "pritem_approvedby,pritem_comments,createddate) values ('" + priList[i].pritemID + "','" + priList[i].prID + "','" + itemID + "'," +
                                "'" + itemBuisness + "'," + priList[i].qty + "," + priList[i].price + "," + converted + "," + approved + "," +
                                //"'" + priList[i].approvedDate + "','" + priList[i].approvedBy + "','" + priList[i].comments + "','" + createdDate + "')";
                                "'" + priList[i].approvedDate.Value.ToString("MMM dd yyyy hh:mm tt") + "','" + priList[i].approvedBy + "','" + priList[i].comments + "','" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "')";

                            this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + str1);
                            command.CommandText = str1;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();
                            
                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + priList[i].pritemID + "','imas_pritem'," +
                           "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','pritem_id')";
                            this.SSAPullPushMQLog.Debug("insertPurchaseRequestItem: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertPurchaseRequestItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertCashSales(List<CashSales> csList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < csList.Count(); i++)
                    {
                        Int32 active = 0, posted = 0;
                        if (csList[i].adjActive == true)
                        {
                            active = 1;
                        }
                        if (csList[i].adjPosted == true)
                        {
                            posted = 1;
                        }
                        String str1 = "insert into imas_adjustment (adjustment_id,adjustment_code,createdby,createddate,modifiedby,modifieddate,adjustment_active,adjustment_posted,adjustment_remarks," +
                            "warehouse_id,adjustment_description,adjustment_type) values ('" + csList[i].adjID + "','" + csList[i].adjCode + "','" + csList[i].adjCreatedBy + "'," +
                            "'" +createdDate + "','" + csList[i].adjModifiedBy + "','" + csList[i].adjModifiedDate + "'," + active + "," +
                            "" + posted + ",'" + csList[i].adjRemarks + "','NA','" + csList[i].adjDesc + "','" + csList[i].adjType + "')";

                        this.SSAPullPushMQLog.Debug("insertCashSales: " + str1);
                        command.CommandText = str1;
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertCashSales Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertCashSalesItem(List<CashSalesItem> csiList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                List<String> itemList = new List<String>();
                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < csiList.Count(); i++)
                    {
                        String tempItemID = csiList[i].adjItemBusinessID;
                        //OdbcCommand cmd0 = oCon.CreateCommand();
                        command.CommandText = "select ib.itembusiness_id from imas_itembusiness ib join imas_item i on ib.item_id = i.item_id " +
                            "where i.item_isbn = '" + tempItemID + "' and ib.businesschannel_id = '4cc04da6-f632-4c49-bd9e-cfc7a48a8e01'";
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            itemList.Add(reader1.GetString(0));
                        }
                        else
                        {
                            itemList.Add("");
                        }
                        reader1.Close();                        
                    }

                    for (int i = 0; i < csiList.Count(); i++)
                    {
                        String itemID = itemList[i];

                        if (!String.IsNullOrEmpty(itemID))
                        {
                            Int32 status1 = 0;
                            if (csiList[i].adjItemStatus == true)
                            {
                                status1 = 1;
                            }
                            String str1 = "insert into imas_adjustmentitem (adjustmentitem_id,itembusiness_id,adjustmentitem_qty,adjustment_id,adjustmentitem_status,adjustmentitem_remarks) values (" +
                                "'" + csiList[i].adjItemID + "','" + itemID + "'," + csiList[i].adjItemQty + ",'" + csiList[i].adjID + "'," + status1 + "," +
                                "'" + csiList[i].adjItemRemarks + "')";

                            this.SSAPullPushMQLog.Debug("insertCashSalesItem: " + str1);
                            command.CommandText = str1;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertCashSalesItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertSOReturn(List<SOReturn> datList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < datList.Count(); i++)
                    {
                        Int32 active = 0;
                        if (datList[i].rrActive == true)
                        {
                            active = 1;
                        }
                        String str1 = "insert into imas_return (rr_id,sch_id,rr_number,rr_date,rr_description,createdby,rr_reference,rr_status,rr_active) values " +
                             "('" + datList[i].rrID + "','" + datList[i].schID + "','" + datList[i].rrNumber + "','" + datList[i].rrDate.Value.ToString("MMM dd yyyy hh:mm tt") + "','" + checkIsNull(datList[i].rrDesc).Replace("'", "''") + "','" + datList[i].rrCreatedBy + "'," +
                            "'" + checkIsNull(datList[i].rrReference).Replace("'", "''") + "'," + datList[i].rrStatus + "," + active + ")";
                        this.SSAPullPushMQLog.Debug("insertSOReturn: " + str1);
                        command.CommandText = str1;
                        command.ExecuteNonQuery();

                        String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + datList[i].rrID + "','imas_return'," +
                            "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','rr_id')";
                        this.SSAPullPushMQLog.Debug("insertSOReturn: " + str3);
                        command.CommandText = str3;
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertSOReturn Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertSOReturnItem(List<SOReturnItem> datList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;
                List<String> itemList = new List<String>();
                try
                {
                    oCon.Open();

                    transaction = oCon.BeginTransaction();

                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < datList.Count(); i++)
                    {
                        String tempItemID = datList[i].riItemID;

                        //OdbcCommand cmd0 = oCon.CreateCommand();
                        command.CommandText = "select item_id from imas_item where item_isbn = '" + tempItemID + "'";
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            itemList.Add(reader1.GetString(0));
                        }
                        else
                        {
                            itemList.Add("");
                        }
                        reader1.Close();                        
                    }

                    for (int i = 0; i < datList.Count(); i++)
                    {
                        String itemID = itemList[i];

                        if (!String.IsNullOrEmpty(itemID))
                        {
                            String str1 = "insert into imas_returnitem (rritem_id,rr_id,rritem_invoice,rritem_isbn,rritem_isbn2,rritem_return_qty,createddate,createdby,rritem_status,remarks," +
                                "item_id,pack_id) values ('" + datList[i].riID + "','" + datList[i].rrID + "','" + GetLastFewChars(datList[i].riInvoice,8) + "'," +
                                "'" + datList[i].riIsbn + "','" + datList[i].riIsbn2 + "'," + datList[i].riReturnQty + ",'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','" + datList[i].riCreatedBy + "'," +
                                "'" + datList[i].riStatus + "','" + checkIsNull(datList[i].riRemarks).Replace("'","''") + "','" + itemID + "','" + itemID + "')";
                            this.SSAPullPushMQLog.Debug("insertSOReturnItem: " + str1);
                            command.CommandText = str1;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();

                            String str3 = "insert into ssa_process (pro_item_id, pro_table_name,pro_createddt,pro_type,pro_column_name) values ('" + datList[i].riID + "','imas_returnitem'," +
                                "'" + createdDate.ToString("MMM dd yyyy hh:mm tt") + "','U','rritem_id')";
                            this.SSAPullPushMQLog.Debug("insertSOReturnItem: " + str3);
                            command.CommandText = str3;
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertSOReturnItem Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        private String insertDiscountAndTax(List<DiscountAndTax> datList)
        {
            DateTime createdDate = DateTime.Now;
            String status = "FAILED";
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_IMAS))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    for (int i = 0; i < datList.Count(); i++)
                    {
                        //Checking if jobOrdMasterPackDetail2ID exist then no need insert - WY-30.AUG.2014
                        String strChkJobOrdMasterPackDetail2ID = " select count(wms_job_id) from wms_jobordmaster_packdetail2 where wms_job_id = '" + datList[i].wms_job_id + "' " +
                                                                 " and moNo = '" + SplitMoNo(datList[i].moNo) + "' and itemID = '" + datList[i].itemID + "' ";
                        int tolJobID = 0;

                        this.SSAPullPushMQLog.Debug("checkJobOrdPackMasterDetail2ID: " + strChkJobOrdMasterPackDetail2ID);
                        command.CommandText = strChkJobOrdMasterPackDetail2ID; 
                        OdbcDataReader reader1 = command.ExecuteReader();

                        if (reader1.Read())
                        {
                            tolJobID = Convert.ToInt32(reader1.GetString(0));                          
                        } 

                        reader1.Close();


                        if (tolJobID == 0)
                        {
                            String str1 = "insert into wms_jobordmaster_packdetail2 (wms_jobordmaster_ID,itemID,discount,tax,moNo,moNo_internalID,wms_jobordmaster_pack_id,wms_job_id,qty,price,orderLine,memo) values (" +
                                "'" + datList[i].wms_jobordmaster_ID + "','" + datList[i].itemID + "'," + datList[i].discount + "," + datList[i].tax + ",'" + SplitMoNo(datList[i].moNo) + "'," +
                                "'" + datList[i].moNoInternalID + "','" + datList[i].wms_jobordmaster_pack_id + "','" + datList[i].wms_job_id + "'," + datList[i].qty + "," + datList[i].price + "," + datList[i].orderLine + ",'" + checkIsNull(datList[i].memo).Replace("'","''") + "')";
                            this.SSAPullPushMQLog.Debug("insertDiscountAndTax: " + str1);
                            command.CommandText = str1;
                            //int nNoAdded = 
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                    status = "SUCCESS";
                }
                catch (Exception ex)
                {
                    this.SSAPullPushMQLog.Error("insertDiscountAndTax Exception: " + ex.ToString());
                    status = "FAILED";
                    transaction.Rollback();
                }
            }
            return status;
        }
        #endregion

        #region Push Queue Function
        private String SOFulfillment(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("SOFulfillment: Retrieving wms_jobordscan.");
            
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            Boolean bTranStatus = false;
            String createdDate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            List<String> jobIDList = new List<String>();
            List<String> bcList = new List<String>();

            try
            {
                List<JobMoPack> jmpList = new List<JobMoPack>();
                List<JobOrdScan> josList = new List<JobOrdScan>();
                List<JobOrdScanPack> jospList = new List<JobOrdScanPack>();
                List<JobOrdMaster> jomList = new List<JobOrdMaster>();
                List<JobOrdMasterPack> jompList = new List<JobOrdMasterPack>();
                List<JobOrdMasterPackDetail> jompdList = new List<JobOrdMasterPackDetail>();
                List<JobItem> jiList = new List<JobItem>();

                #region JobOrdScan
                //retrieve trade and bcas
                OdbcCommand cmd1 = oCon.CreateCommand();
                cmd1.CommandText = "select distinct job_id,businesschannel_code from wms_jobordscan where (businesschannel_code = 'ET' or businesschannel_code = 'BC') and exportdate > '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' " +
                    "and exportdate <='" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";
                //cmd1.CommandText = "select distinct job_id,businesschannel_code from wms_jobordscan where (businesschannel_code = 'ET' or businesschannel_code = 'BC') and exportdate > '2015-01-04 07:20:00' " +
                //    "and exportdate <='2015-01-05 07:20:00'"; 
                cmd1.CommandTimeout = 300;//Add the commandTimeout value - WY-15.SEPT.2014
                OdbcDataReader reader1 = cmd1.ExecuteReader();
                String str = null;
                while (reader1.Read())
                {
                    String jos_jobID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                    String businessChannel = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);

                    if (str == null || str != jos_jobID)
                    {
                        str = jos_jobID;
                        jobIDList.Add(str);
                        bcList.Add(businessChannel);
                    }
                }
                reader1.Close();
                cmd1.Dispose();
                #endregion

                for (int i = 0; i < jobIDList.Count(); i++)
                {
                    #region BCAS-BC
                    /* 
                    * wms_jobordscan                    contain mo              & export date/rangeTo             & job_ID 
                    * netsuite_jobmo_pack               contain mo              & nsjmp_jobmoPack_ID              & nsjmp_nsj_jobID    & rangeTo
                    * netsuite_jobitem                  contain mo              & nsji_nsj_jobID
                    */

                    /* 
                     * netsuite_jobordmaster_pack       contain mo              &  nsjomp_jobOrdMaster_pack_ID    & nsjomp_job_ID
                     * netsuite_jobmo_pack              contain mo              &  nsjmp_jobmoPack_ID             & nsjmp_nsj_jobID    &   rangeTo

                     * wms_jobordscan_pack              contain pack quantity   &  rangeTo                        & josp_pack_ID
                     * netsuite_jobordmaster_packdetail contain price           &  nsjompd_jobOrdMaster_pack_ID
                     * 
                    */

                    if (bcList[i].Equals("BC"))
                    {
                        #region JobMOPack
                        /*
                         * //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        OdbcCommand cmd8 = oCon.CreateCommand();
                        cmd8.CommandText = "select * from wms_jobmo_pack where job_id = '" + jobIDList[i] + "'";
                        cmd8.CommandTimeout = 120;
                        OdbcDataReader reader8 = cmd8.ExecuteReader();
                        while (reader8.Read())
                        {
                            JobMoPack jmp = new JobMoPack();
                            jmp.jobMoPackID = (reader8.GetValue(1) == DBNull.Value) ? String.Empty : reader8.GetString(1);
                            jmp.jobID = (reader8.GetValue(2) == DBNull.Value) ? String.Empty : reader8.GetString(2);
                            jmp.period = (reader8.GetValue(3) == DBNull.Value) ? String.Empty : reader8.GetString(3);
                            jmp.moNo = (reader8.GetValue(4) == DBNull.Value) ? String.Empty : reader8.GetString(4);
                            jmp.schID = (reader8.GetValue(5) == DBNull.Value) ? String.Empty : reader8.GetString(5);
                            jmp.schName = (reader8.GetValue(6) == DBNull.Value) ? String.Empty : reader8.GetString(6);
                            jmp.packID = (reader8.GetValue(7) == DBNull.Value) ? String.Empty : reader8.GetString(7);
                            jmp.packTitles = (reader8.GetValue(8) == DBNull.Value) ? String.Empty : reader8.GetString(8);
                            jmp.packPrice = (reader8.GetValue(9) == DBNull.Value) ? 0 : Convert.ToDouble(reader8.GetValue(9));
                            jmp.qty = (reader8.GetValue(10) == DBNull.Value) ? 0 : Convert.ToInt32(reader8.GetValue(10));
                            jmp.amount = (reader8.GetValue(11) == DBNull.Value) ? 0 : Convert.ToDouble(reader8.GetValue(11));
                            jmp.packISBN = (reader8.GetValue(12) == DBNull.Value) ? String.Empty : reader8.GetString(12);
                            jmp.createdDate = DateTime.Now;
                            jmp.rangeTo = dataList[0].rangeTo;
                            jmpList.Add(jmp);
                        }
                        reader8.Close();
                        cmd8.Dispose();*/
                        #endregion
                        #region JobOrdScan
                        /*var jobordscan = (from j in josList
                                          where j.jobID == jobID
                                          select new JobOrdScan
                                          {
                                              jobOrdScanID = j.jobOrdScanID,
                                              consignmentNote = j.consignmentNote,
                                              countryTag = j.countryTag,
                                              deliveryRef = j.deliveryRef,
                                              jobID = j.jobID,
                                              jobMoID = j.jobMoID,
                                              ordRecNo = j.ordRecNo,
                                              scanDate = j.scanDate,
                                              moNo = j.moNo,
                                              businessChannelID = j.businessChannelID,
                                              businessChannelCode = j.businessChannelCode,
                                              exportDate = j.exportDate,
                                              loadInd = j.loadInd,
                                              doNo = j.doNo,
                                              createdDate = Convert.ToDateTime(createdDate),
                                              rangeTo = dataList[0].rangeTo
                                          }).ToList();

                        josList = jobordscan;
                        */
                        OdbcCommand cmd7 = oCon.CreateCommand();
                        cmd7.CommandText = "select * from wms_jobordscan jos where jos.job_id = '" + jobIDList[i] + "' " +
                            "and jos.exportdate > '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' and jos.exportdate <='" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";
                        cmd7.CommandTimeout = 300;//Change the commandTimeout value from 120s - WY-03.SEPT.2014
                        OdbcDataReader reader7 = cmd7.ExecuteReader();
                        while (reader7.Read())
                        {
                            JobOrdScan jos = new JobOrdScan();

                            jos.jobOrdScanID = (reader7.GetValue(0) == DBNull.Value) ? String.Empty : reader7.GetString(0);
                            jos.consignmentNote = (reader7.GetValue(1) == DBNull.Value) ? String.Empty : reader7.GetString(1);
                            jos.countryTag = (reader7.GetValue(2) == DBNull.Value) ? String.Empty : reader7.GetString(2);
                            jos.deliveryRef = (reader7.GetValue(3) == DBNull.Value) ? String.Empty : reader7.GetString(3);
                            jos.jobID = (reader7.GetValue(4) == DBNull.Value) ? String.Empty : reader7.GetString(4);
                            jos.jobMoID = (reader7.GetValue(5) == DBNull.Value) ? String.Empty : reader7.GetString(5);
                            jos.ordRecNo = (reader7.GetValue(6) == DBNull.Value) ? String.Empty : reader7.GetString(6);
                            jos.scanDate = (reader7.IsDBNull(7)) ? DateTime.Now : reader7.GetDateTime(7);
                            jos.moNo = (reader7.GetValue(8) == DBNull.Value) ? String.Empty : "SO-" + reader7.GetString(8);
                            jos.businessChannelID = (reader7.GetValue(9) == DBNull.Value) ? String.Empty : reader7.GetString(9);
                            jos.businessChannelCode = (reader7.GetValue(10) == DBNull.Value) ? String.Empty : reader7.GetString(10);
                            jos.exportDate = reader7.GetDateTime(12);
                            jos.loadInd = (reader7.GetValue(13) == DBNull.Value) ? String.Empty : reader7.GetString(13);
                            jos.doNo = (reader7.GetValue(14) == DBNull.Value) ? String.Empty : reader7.GetString(14);
                            jos.rangeTo = dataList[0].rangeTo;
                            josList.Add(jos);
                        }
                        reader7.Close();
                        cmd7.Dispose();
                        #endregion
                        #region JobOrdScanPack
                        OdbcCommand cmd2 = oCon.CreateCommand();
                        cmd2.CommandText = "select josp.*, jom.mono, jos.job_id, jos.ordrecno, jos.exportdate from wms_jobordscan_pack josp " +
                            "join wms_jobordmaster_pack jomp on jomp.jobordmaster_pack_id = josp.jobordmaster_pack_id " +
                            "join wms_jobordmaster jom on jomp.jobordmaster_id = jom.jobordmaster_id AND jomp.job_id = jom.job_id " +
                            "join wms_jobordscan jos on jom.mono = jos.mono and jom.job_id = jos.job_id and jom.ordrecno = jos.ordrecno " +
                            //"where jomp.job_id = '" + jobIDList[i] + "' and josp.ordfulfill > 0 " +
                            "where jomp.job_id = '" + jobIDList[i] + "'  " +
                            "and jos.exportdate > '" + dataList[0].rangeFrom + "' and jos.exportdate <='" + dataList[0].rangeTo + "'";
                        cmd2.CommandTimeout = 300;//Add the commandTimeout - WY-03.SEPT.2014
                        OdbcDataReader reader2 = cmd2.ExecuteReader();

                        while (reader2.Read())
                        {
                            JobOrdScanPack josp = new JobOrdScanPack();

                            josp.jobordmaster_pack_id = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                            josp.status = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                            josp.ordFulfill = (reader2.GetValue(2) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(2));
                            josp.ordPoint = (reader2.GetValue(3) == DBNull.Value) ? 0 : Convert.ToDecimal(reader2.GetValue(3));
                            josp.posted_ind = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                            josp.moNo = (reader2.GetValue(5) == DBNull.Value) ? String.Empty : "SO-" + reader2.GetString(5);
                            josp.jobID = (reader2.GetValue(6) == DBNull.Value) ? String.Empty : reader2.GetString(6);
                            josp.ordRecNo = (reader2.GetValue(7) == DBNull.Value) ? String.Empty : reader2.GetString(7);
                            josp.exportDate = reader2.GetDateTime(8);
                            josp.rangeTo = dataList[0].rangeTo;
                            jospList.Add(josp);
                        }
                        reader2.Close();
                        cmd2.Dispose();
                        #endregion
                        #region JobOrdMaster
                        /* //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        OdbcCommand cmd6 = oCon.CreateCommand();
                        //cmd6.CommandText = "select jobordmaster_id, job_id,ordrecno,ordstudent,clsid,mono,consignmentnote,processperiod,country,jobmo_id " +
                            //"from wms_jobordmaster where job_id = '" + jobIDList[i] + "'";
                        cmd6.CommandText = "select jobordmaster_id, job_id,ordrecno,ordstudent,clsid,mono,consignmentnote,processperiod,country,jobmo_id " +
                            "from wms_jobordmaster where job_id = '" + jobIDList[i] + "'";
                        OdbcDataReader reader6 = cmd6.ExecuteReader();

                        while (reader6.Read())
                        {
                            JobOrdMaster jom = new JobOrdMaster();

                            jom.ordMasterID = (reader6.GetValue(0) == DBNull.Value) ? String.Empty : reader6.GetString(0);
                            jom.jobID = (reader6.GetValue(1) == DBNull.Value) ? String.Empty : reader6.GetString(1);
                            jom.ordRecNo = (reader6.GetValue(2) == DBNull.Value) ? String.Empty : reader6.GetString(2);
                            jom.ordStudent = (reader6.GetValue(3) == DBNull.Value) ? String.Empty : reader6.GetString(3);
                            jom.clsID = (reader6.GetValue(4) == DBNull.Value) ? String.Empty : reader6.GetString(4);
                            jom.moNo = (reader6.GetValue(5) == DBNull.Value) ? String.Empty : reader6.GetString(5);
                            jom.consignmentNote = (reader6.GetValue(6) == DBNull.Value) ? String.Empty : reader6.GetString(6);
                            jom.processPeriod = (reader6.GetValue(7) == DBNull.Value) ? String.Empty : reader6.GetString(7);
                            jom.country = (reader6.GetValue(8) == DBNull.Value) ? String.Empty : reader6.GetString(8);
                            jom.jobMoID = (reader6.GetValue(9) == DBNull.Value) ? String.Empty : reader6.GetString(9);
                            jom.createdDate = Convert.ToDateTime(createdDate);
                            jom.rangeTo = dataList[0].rangeTo;
                            jomList.Add(jom);
                        }
                        reader6.Close();
                        cmd6.Dispose();*/
                        #endregion
                        #region JobOrdMasterPack
                        /* //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        OdbcCommand cmd3 = oCon.CreateCommand();
                        //cmd3.CommandText = "select * from wms_jobordmaster_pack where job_id = '" + jobIDList[i] + "'";
                        cmd3.CommandText = "select jomp.*,jom.mono from wms_jobordmaster jom join wms_jobordmaster_pack jomp on jom.jobordmaster_id = jomp.jobordmaster_id " +
                            "where jomp.job_id = '" + jobIDList[i] + "'";
                        OdbcDataReader reader3 = cmd3.ExecuteReader();

                        while (reader3.Read())
                        {
                            JobOrdMasterPack jomp = new JobOrdMasterPack();

                            jomp.ordMasterPackID = (reader3.GetValue(0) == DBNull.Value) ? String.Empty : reader3.GetString(0);
                            jomp.ordMasterID = (reader3.GetValue(1) == DBNull.Value) ? String.Empty : reader3.GetString(1);
                            jomp.jobID = (reader3.GetValue(2) == DBNull.Value) ? String.Empty : reader3.GetString(2);
                            jomp.ordNo = (reader3.GetValue(3) == DBNull.Value) ? String.Empty : reader3.GetString(3);
                            jomp.ordPack = (reader3.GetValue(4) == DBNull.Value) ? String.Empty : reader3.GetString(4);
                            jomp.ordQty = (reader3.GetValue(5) == DBNull.Value) ? 0 : Convert.ToInt32(reader3.GetValue(5));
                            jomp.ordPrice = (reader3.GetValue(6) == DBNull.Value) ? 0 : Convert.ToDouble(reader3.GetValue(6));
                            jomp.ordReplace = (reader3.GetValue(7) == DBNull.Value) ? String.Empty : reader3.GetString(7);
                            jomp.ofrCode = (reader3.GetValue(8) == DBNull.Value) ? String.Empty : reader3.GetString(8);
                            jomp.status = (reader3.GetValue(9) == DBNull.Value) ? String.Empty : reader3.GetString(9);
                            jomp.ordPackStatus = (reader3.GetValue(10) == DBNull.Value) ? String.Empty : reader3.GetString(10);
                            jomp.ordFulfill = (reader3.GetValue(11) == DBNull.Value) ? 0 : Convert.ToInt32(reader3.GetValue(11));
                            jomp.ordDetDate = (reader3.GetValue(12) == DBNull.Value) ? DateTime.Now : reader3.GetDateTime(12);
                            jomp.ordPoint = (reader3.GetValue(13) == DBNull.Value) ? 0 : Convert.ToDouble(reader3.GetValue(13));
                            jomp.skuCode = (reader3.GetValue(14) == DBNull.Value) ? String.Empty : reader3.GetString(14);
                            jomp.packTitle = (reader3.GetValue(15) == DBNull.Value) ? String.Empty : reader3.GetString(15);
                            jomp.ofrDesc = (reader3.GetValue(16) == DBNull.Value) ? String.Empty : reader3.GetString(16);
                            jomp.moNo = (reader3.GetValue(17) == DBNull.Value) ? String.Empty : reader3.GetString(17);
                            jomp.createdDate = Convert.ToDateTime(createdDate);
                            jomp.rangeTo = dataList[0].rangeTo;
                            jompList.Add(jomp);
                        }
                        reader2.Close();
                        cmd2.Dispose();*/
                        #endregion
                        #region JobOrdMasterPackDetail
                        /* //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        OdbcCommand cmd4 = oCon.CreateCommand();
                        cmd4.CommandText = "select * from wms_jobordmaster_packdetail where job_id = '" + jobIDList[i] + "'";
                        OdbcDataReader reader4 = cmd4.ExecuteReader();
                        while (reader4.Read())
                        {
                            JobOrdMasterPackDetail jompd = new JobOrdMasterPackDetail();
                            jompd.ordMasterPackDetailID = (reader4.GetValue(0) == DBNull.Value) ? String.Empty : reader4.GetString(0);
                            jompd.ordMasterPackID = (reader4.GetValue(1) == DBNull.Value) ? String.Empty : reader4.GetString(1);
                            jompd.jobID = (reader4.GetValue(2) == DBNull.Value) ? String.Empty : reader4.GetString(2);
                            jompd.ordPack = (reader4.GetValue(3) == DBNull.Value) ? String.Empty : reader4.GetString(3);
                            jompd.skuNo = (reader4.GetValue(4) == DBNull.Value) ? String.Empty : reader4.GetString(4);
                            jompd.isbn = (reader4.GetValue(5) == DBNull.Value) ? String.Empty : reader4.GetString(5);
                            jompd.isbnSecondary = (reader4.GetValue(6) == DBNull.Value) ? String.Empty : reader4.GetString(6);
                            jompd.skuQty = (reader4.GetValue(7) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(7));
                            jompd.itemID = (reader4.GetValue(8) == DBNull.Value) ? String.Empty : reader4.GetString(8);
                            jompd.totalQty = (reader4.GetValue(9) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(9));
                            jompd.scannedQty = (reader4.GetValue(10) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(10));
                            jompd.createdDate = Convert.ToDateTime(createdDate);
                            jompd.rangeTo = dataList[0].rangeTo;
                            jompdList.Add(jompd);
                        }
                        reader4.Close();
                        cmd4.Dispose();*/
                        #endregion
                        #region JobItem
                        /* //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        OdbcCommand cmd5 = oCon.CreateCommand();
                        cmd5.CommandText = "select ji.jobitem_id,ji.job_id,ji.createdby,ji.createddate,ii.item_isbn,ji.item_qty,ji.posting_type,ji.mono from wms_jobitem ji," +
                            "imas_item ii where ji.item_id = ii.item_id and job_id = '" + jobIDList[i] + "'";
                        OdbcDataReader reader5 = cmd5.ExecuteReader();

                        while (reader5.Read())
                        {
                            JobItem ji = new JobItem();
                            ji.jobItemID = (reader5.GetValue(0) == DBNull.Value) ? String.Empty : reader5.GetString(0);
                            ji.jobID = (reader5.GetValue(1) == DBNull.Value) ? String.Empty : reader5.GetString(1);
                            ji.createdBy = (reader5.GetValue(2) == DBNull.Value) ? String.Empty : reader5.GetString(2);
                            ji.createdDate = (reader5.GetValue(3) == DBNull.Value) ? DateTime.Now : reader5.GetDateTime(3);
                            ji.itemID = (reader5.GetValue(4) == DBNull.Value) ? String.Empty : reader5.GetString(4);
                            ji.itemQty = (reader5.GetValue(5) == DBNull.Value) ? 0 : Convert.ToInt32(reader5.GetValue(5));
                            ji.postingType = (reader5.GetValue(6) == DBNull.Value) ? String.Empty : reader5.GetString(6);
                            ji.moNo = (reader5.GetValue(7) == DBNull.Value) ? String.Empty : reader5.GetString(7);
                            ji.rangeTo = dataList[0].rangeTo;
                            jiList.Add(ji);
                        }
                        reader5.Close();
                        cmd5.Dispose();*/
                        #endregion
                    }
                    #endregion
                    #region Trade-ET
                    else if (bcList[i].Equals("ET"))
                    {
                        #region JobOrdScan
                        OdbcCommand cmd7 = oCon.CreateCommand();
                        cmd7.CommandText = "select * from wms_jobordscan where job_id = '" + jobIDList[i] + "' " +
                            "and exportdate > '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' and exportdate <='" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";
                        cmd7.CommandTimeout = 300;//Change the commandTimeout value from 120s - WY-03.SEPT.2014
                        OdbcDataReader reader7 = cmd7.ExecuteReader();

                        while (reader7.Read())
                        {
                            JobOrdScan jos = new JobOrdScan();

                            jos.jobOrdScanID = (reader7.GetValue(0) == DBNull.Value) ? String.Empty : reader7.GetString(0);
                            jos.consignmentNote = (reader7.GetValue(1) == DBNull.Value) ? String.Empty : reader7.GetString(1);
                            jos.countryTag = (reader7.GetValue(2) == DBNull.Value) ? String.Empty : reader7.GetString(2);
                            jos.deliveryRef = (reader7.GetValue(3) == DBNull.Value) ? String.Empty : reader7.GetString(3);
                            jos.jobID = (reader7.GetValue(4) == DBNull.Value) ? String.Empty : reader7.GetString(4);
                            jos.jobMoID = (reader7.GetValue(5) == DBNull.Value) ? String.Empty : reader7.GetString(5);
                            jos.ordRecNo = (reader7.GetValue(6) == DBNull.Value) ? String.Empty : reader7.GetString(6);
                            jos.scanDate = reader7.GetDateTime(7);
                            jos.moNo = (reader7.GetValue(8) == DBNull.Value) ? String.Empty : reader7.GetString(8);
                            jos.businessChannelID = (reader7.GetValue(9) == DBNull.Value) ? String.Empty : reader7.GetString(9);
                            jos.businessChannelCode = (reader7.GetValue(10) == DBNull.Value) ? String.Empty : reader7.GetString(10);
                            jos.exportDate = reader7.GetDateTime(12);
                            jos.loadInd = (reader7.GetValue(13) == DBNull.Value) ? String.Empty : reader7.GetString(13);
                            jos.doNo = (reader7.GetValue(14) == DBNull.Value) ? String.Empty : reader7.GetString(14);
                            jos.rangeTo = dataList[0].rangeTo;
                            josList.Add(jos);
                        }
                        reader7.Close();
                        cmd7.Dispose();
                        #endregion
                        #region JobOrdScanPack
                        OdbcCommand cmd2 = oCon.CreateCommand();
                        /*cmd2.CommandText = "select josp.* from wms_jobordscan_pack josp "+
                            "join wms_jobordmaster_pack jomp on jomp.jobordmaster_pack_id = josp.jobordmaster_pack_id " +
                            "where jomp.job_id = '" + jobIDList[i] + "' and josp.ordfulfill > 0";*/
                        cmd2.CommandText = "select josp.*, jom.mono, jos.job_id, jos.ordrecno, jos.exportdate from wms_jobordscan_pack josp " +
                            "join wms_jobordmaster_pack jomp on jomp.jobordmaster_pack_id = josp.jobordmaster_pack_id " +
                            "join wms_jobordmaster jom on jomp.jobordmaster_id = jom.jobordmaster_id AND jomp.job_id = jom.job_id " +
                            "join wms_jobordscan jos on jom.mono = jos.mono and jom.job_id = jos.job_id and jom.ordrecno = jos.ordrecno " +
                            //"where jomp.job_id = '" + jobIDList[i] + "' and josp.ordfulfill > 0 " +
                            "where jomp.job_id = '" + jobIDList[i] + "' " +
                            "and jos.exportdate > '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' and jos.exportdate <='" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";
                        cmd2.CommandTimeout = 300; //Add the commandTimeout - WY-03.SEPT.2014
                        OdbcDataReader reader2 = cmd2.ExecuteReader();
                        /*
                        while (reader2.Read())
                        {
                            JobOrdScanPack josp = new JobOrdScanPack();

                            josp.jobordmaster_pack_id = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                            josp.status = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                            josp.ordFulfill = (reader2.GetValue(2) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(2));
                            josp.ordPoint = (reader2.GetValue(3) == DBNull.Value) ? 0 : Convert.ToDecimal(reader2.GetValue(3));
                            josp.posted_ind = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                            josp.rangeTo = dataList[0].rangeTo;
                            jospList.Add(josp);
                        }
                        */
                        while (reader2.Read())
                        {
                            JobOrdScanPack josp = new JobOrdScanPack();

                            josp.jobordmaster_pack_id = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                            josp.status = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                            josp.ordFulfill = (reader2.GetValue(2) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(2));
                            josp.ordPoint = (reader2.GetValue(3) == DBNull.Value) ? 0 : Convert.ToDecimal(reader2.GetValue(3));
                            josp.posted_ind = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                            josp.moNo = (reader2.GetValue(5) == DBNull.Value) ? String.Empty : "SO-" + reader2.GetString(5);
                            josp.jobID = (reader2.GetValue(6) == DBNull.Value) ? String.Empty : reader2.GetString(6);
                            josp.ordRecNo = (reader2.GetValue(7) == DBNull.Value) ? String.Empty : reader2.GetString(7);
                            josp.exportDate = reader2.GetDateTime(8);
                            josp.rangeTo = dataList[0].rangeTo;
                            jospList.Add(josp);
                        }
                        reader2.Close();
                        cmd2.Dispose();
                        #endregion
                    }
                    #endregion
                }

                //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                /*String[] transactionType = new String[] { "MQPUSH-JOB ORD SCAN", "MQPUSH-JOB ORD SCAN PACK", "MQPUSH-JOB ORD MASTER PACK", "MQPUSH-JOB ORD MASTER PACK DETAIL",
                    "MQPUSH-JOB ITEM", "MQPUSH-JOB ORD MASTER","MQPUSH-JOB MO PACK" };*/
                String[] transactionType = new String[] { "MQPUSH-JOB ORD SCAN", "MQPUSH-JOB ORD SCAN PACK"};

                String[] queueLabel = new String[transactionType.Count()];
                for (int j = 0; j < transactionType.Count(); j++)
                {
                    Guid gjob_id = Guid.NewGuid();
                    String MQjob = gjob_id.ToString();
                    queueLabel[j] = "WAREHOUSE" + " - " + MQjob + " > " + transactionType[j];
                }

                bTranStatus = SOFulfillmentMQ(queueLabel, josList, jospList, jomList, jompList, jompdList, jiList, jmpList, transactionType);
                this.SSAPullPushMQLog.Debug("SOFulfillment: Sending SOFulfillment to MQ.(Inserted Successfully: " + bTranStatus + ")");
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error("SOFulfillment Exception: " + ex.ToString());
            }
            finally
            {
                oCon.Close();
            }

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }

            /*
            var results = Array.FindAll(bTranStatus, s => s == false);

            if (results.Count() > 0)
            {
                return "FAIL";
            }
            else
            {
                return "SUCCESS";
            }*/
        }
        //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
        private String JobOrdMasterExtraction(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_job.");

            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS; 
            oCon.Open();

            Boolean bTranStatus = false;
            String createdDate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            List<String> jobIDList = new List<String>(); 

            try
            {
                List<JobMoPack> jmpList = new List<JobMoPack>();
                List<JobOrdMaster> jomList = new List<JobOrdMaster>();
                List<JobOrdMasterPack> jompList = new List<JobOrdMasterPack>();
                List<JobOrdMasterPackDetail> jompdList = new List<JobOrdMasterPackDetail>();
                List<JobItem> jiList = new List<JobItem>();

                #region WMS_JOB
                //retrieve bcas
                OdbcCommand cmd1 = oCon.CreateCommand();
                cmd1.CommandText = "SELECT job_id FROM wms_job WHERE businesschannel_id = '365c2b8b-9a20-4840-ae50-cfc7a48a8e01' AND createddate > '" + dataList[0].rangeFrom + "' " +
                    "and createddate <='" + dataList[0].rangeTo + "'";   
                OdbcDataReader reader1 = cmd1.ExecuteReader();
                String str = null;
                while (reader1.Read())
                {
                    String jos_jobID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0); 

                    if (str == null || str != jos_jobID)
                    {
                        str = jos_jobID;
                        jobIDList.Add(str); 
                    }
                } 
                reader1.Close();
                cmd1.Dispose();
                this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_job.");
                #endregion

                for (int i = 0; i < jobIDList.Count(); i++)
                {
                    #region BCAS-BC
                    /* 
                    * wms_jobordscan                    contain mo              & export date/rangeTo             & job_ID 
                    * netsuite_jobmo_pack               contain mo              & nsjmp_jobmoPack_ID              & nsjmp_nsj_jobID    & rangeTo
                    * netsuite_jobitem                  contain mo              & nsji_nsj_jobID
                    */

                    /* 
                     * netsuite_jobordmaster_pack       contain mo              &  nsjomp_jobOrdMaster_pack_ID    & nsjomp_job_ID
                     * netsuite_jobmo_pack              contain mo              &  nsjmp_jobmoPack_ID             & nsjmp_nsj_jobID    &   rangeTo

                     * wms_jobordscan_pack              contain pack quantity   &  rangeTo                        & josp_pack_ID
                     * netsuite_jobordmaster_packdetail contain price           &  nsjompd_jobOrdMaster_pack_ID
                     * 
                    */

                        #region JobMOPack 
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobmo_pack.");
                        OdbcCommand cmd8 = oCon.CreateCommand();
                        cmd8.CommandText = "select * from wms_jobmo_pack where job_id = '" + jobIDList[i] + "'";
                        cmd8.CommandTimeout = 300;//Change the commandTimeout value from 120s - WY-03.SEPT.2014
                        OdbcDataReader reader8 = cmd8.ExecuteReader();
                        while (reader8.Read())
                        {
                            JobMoPack jmp = new JobMoPack();
                            jmp.jobMoPackID = (reader8.GetValue(1) == DBNull.Value) ? String.Empty : reader8.GetString(1);
                            jmp.jobID = (reader8.GetValue(2) == DBNull.Value) ? String.Empty : reader8.GetString(2);
                            jmp.period = (reader8.GetValue(3) == DBNull.Value) ? String.Empty : reader8.GetString(3);
                            jmp.moNo = (reader8.GetValue(4) == DBNull.Value) ? String.Empty : reader8.GetString(4);
                            jmp.schID = (reader8.GetValue(5) == DBNull.Value) ? String.Empty : reader8.GetString(5);
                            jmp.schName = (reader8.GetValue(6) == DBNull.Value) ? String.Empty : reader8.GetString(6);
                            jmp.packID = (reader8.GetValue(7) == DBNull.Value) ? String.Empty : reader8.GetString(7);
                            jmp.packTitles = (reader8.GetValue(8) == DBNull.Value) ? String.Empty : reader8.GetString(8);
                            jmp.packPrice = (reader8.GetValue(9) == DBNull.Value) ? 0 : Convert.ToDouble(reader8.GetValue(9));
                            jmp.qty = (reader8.GetValue(10) == DBNull.Value) ? 0 : Convert.ToInt32(reader8.GetValue(10));
                            jmp.amount = (reader8.GetValue(11) == DBNull.Value) ? 0 : Convert.ToDouble(reader8.GetValue(11));
                            jmp.packISBN = (reader8.GetValue(12) == DBNull.Value) ? String.Empty : reader8.GetString(12);
                            jmp.createdDate = DateTime.Now;
                            jmp.rangeTo = dataList[0].rangeTo;
                            jmpList.Add(jmp);
                            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobmo_pack. jobMoPackID = " + jmp.jobMoPackID + " ");
                        }
                        reader8.Close();
                        cmd8.Dispose(); 
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_jobmo_pack.");
                        #endregion
                        #region JobOrdMaster 
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster.");
                        OdbcCommand cmd6 = oCon.CreateCommand(); 
                        cmd6.CommandText = "select jobordmaster_id, job_id,ordrecno,ordstudent,clsid,mono,consignmentnote,processperiod,country,jobmo_id " +
                            "from wms_jobordmaster where job_id = '" + jobIDList[i] + "'";
                        cmd6.CommandTimeout = 300;//Add the commandTimeout - WY-03.SEPT.2014
                        OdbcDataReader reader6 = cmd6.ExecuteReader();

                        while (reader6.Read())
                        {
                            JobOrdMaster jom = new JobOrdMaster();

                            jom.ordMasterID = (reader6.GetValue(0) == DBNull.Value) ? String.Empty : reader6.GetString(0);
                            jom.jobID = (reader6.GetValue(1) == DBNull.Value) ? String.Empty : reader6.GetString(1);
                            jom.ordRecNo = (reader6.GetValue(2) == DBNull.Value) ? String.Empty : reader6.GetString(2);
                            jom.ordStudent = (reader6.GetValue(3) == DBNull.Value) ? String.Empty : reader6.GetString(3);
                            jom.clsID = (reader6.GetValue(4) == DBNull.Value) ? String.Empty : reader6.GetString(4);
                            jom.moNo = (reader6.GetValue(5) == DBNull.Value) ? String.Empty : reader6.GetString(5);
                            jom.consignmentNote = (reader6.GetValue(6) == DBNull.Value) ? String.Empty : reader6.GetString(6);
                            jom.processPeriod = (reader6.GetValue(7) == DBNull.Value) ? String.Empty : reader6.GetString(7);
                            jom.country = (reader6.GetValue(8) == DBNull.Value) ? String.Empty : reader6.GetString(8);
                            jom.jobMoID = (reader6.GetValue(9) == DBNull.Value) ? String.Empty : reader6.GetString(9);
                            jom.createdDate = Convert.ToDateTime(createdDate);
                            jom.rangeTo = dataList[0].rangeTo;
                            jomList.Add(jom);

                            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster. ordMasterID = " + jom.ordMasterID + " ");
                        }
                        reader6.Close();
                        cmd6.Dispose();
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_jobordmaster.");
                        #endregion
                        #region JobOrdMasterPack
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster_pack.");
                        OdbcCommand cmd3 = oCon.CreateCommand();
                        cmd3.CommandText = "select jomp.jobordmaster_pack_id,jomp.jobordmaster_id,jomp.job_id,jomp.ordno,jomp.ordpack,jomp.ordqty,jomp.ordprice, " +
                                            "jomp.ordreplace,jomp.ofrcode,jomp.status,jomp.ordpackstatus,jomp.ordfulfill,jomp.orddetdate,jomp.ordpoint,jomp.skucode," +
                                            "jomp.packtitle,jomp.ofrdesc,jom.mono, jomp.tax_code, jomp.gstamount " + 
                                            "from wms_jobordmaster jom join wms_jobordmaster_pack jomp on jom.jobordmaster_id = jomp.jobordmaster_id " +
                                            "where jomp.job_id = '" + jobIDList[i] + "'";
                        cmd3.CommandTimeout = 300;//Add the commandTimeout - WY-03.SEPT.2014
                        OdbcDataReader reader3 = cmd3.ExecuteReader();

                        while (reader3.Read())
                        {
                            JobOrdMasterPack jomp = new JobOrdMasterPack();

                            jomp.ordMasterPackID = (reader3.GetValue(0) == DBNull.Value) ? String.Empty : reader3.GetString(0);
                            jomp.ordMasterID = (reader3.GetValue(1) == DBNull.Value) ? String.Empty : reader3.GetString(1);
                            jomp.jobID = (reader3.GetValue(2) == DBNull.Value) ? String.Empty : reader3.GetString(2);
                            jomp.ordNo = (reader3.GetValue(3) == DBNull.Value) ? String.Empty : reader3.GetString(3);
                            jomp.ordPack = (reader3.GetValue(4) == DBNull.Value) ? String.Empty : reader3.GetString(4);
                            jomp.ordQty = (reader3.GetValue(5) == DBNull.Value) ? 0 : Convert.ToInt32(reader3.GetValue(5));
                            jomp.ordPrice = (reader3.GetValue(6) == DBNull.Value) ? 0 : Convert.ToDouble(reader3.GetValue(6));
                            jomp.ordReplace = (reader3.GetValue(7) == DBNull.Value) ? String.Empty : reader3.GetString(7);
                            jomp.ofrCode = (reader3.GetValue(8) == DBNull.Value) ? String.Empty : reader3.GetString(8);
                            jomp.status = (reader3.GetValue(9) == DBNull.Value) ? String.Empty : reader3.GetString(9);
                            jomp.ordPackStatus = (reader3.GetValue(10) == DBNull.Value) ? String.Empty : reader3.GetString(10);
                            jomp.ordFulfill = (reader3.GetValue(11) == DBNull.Value) ? 0 : Convert.ToInt32(reader3.GetValue(11));
                            jomp.ordDetDate = (reader3.GetValue(12) == DBNull.Value) ? DateTime.Now : reader3.GetDateTime(12);
                            jomp.ordPoint = (reader3.GetValue(13) == DBNull.Value) ? 0 : Convert.ToDouble(reader3.GetValue(13));
                            jomp.skuCode = (reader3.GetValue(14) == DBNull.Value) ? String.Empty : reader3.GetString(14);
                            jomp.packTitle = (reader3.GetValue(15) == DBNull.Value) ? String.Empty : reader3.GetString(15);
                            jomp.ofrDesc = (reader3.GetValue(16) == DBNull.Value) ? String.Empty : reader3.GetString(16);
                            jomp.moNo = (reader3.GetValue(17) == DBNull.Value) ? String.Empty : reader3.GetString(17);
                            jomp.createdDate = Convert.ToDateTime(createdDate);
                            jomp.rangeTo = dataList[0].rangeTo;

                            jomp.taxCode = (reader3.GetValue(18) == DBNull.Value) ? String.Empty : reader3.GetString(18);
                            jomp.gstAmount = (reader3.GetValue(19) == DBNull.Value) ? 0 : Convert.ToDouble(reader3.GetValue(19));

                            jompList.Add(jomp);

                            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster_pack. ordMasterPackID = " + jomp.ordMasterPackID + " ");
                        }
                        reader3.Close();
                        cmd3.Dispose();
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_jobordmaster_pack.");
                        #endregion
                        #region JobOrdMasterPackDetail
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster_packdetail.");
                        OdbcCommand cmd4 = oCon.CreateCommand();
                        cmd4.CommandText = "select jobordmaster_packdetail_id,jobordmaster_pack_id,job_id,ordpack,skuno,isbn,isbn_secondary,sku_qty,item_id,total_qty, scanned_qty, item_price, tax_code,gstamount,deliveryCharge,deliveryChargeGst from wms_jobordmaster_packdetail where job_id = '" + jobIDList[i] + "'";
                        cmd4.CommandTimeout = 300;//Add the commandTimeout - WY-03.SEPT.2014
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: " + cmd4.CommandText.ToString());

                        OdbcDataReader reader4 = cmd4.ExecuteReader();
                        while (reader4.Read())
                        {
                            JobOrdMasterPackDetail jompd = new JobOrdMasterPackDetail();
                            jompd.ordMasterPackDetailID = (reader4.GetValue(0) == DBNull.Value) ? String.Empty : reader4.GetString(0);
                            jompd.ordMasterPackID = (reader4.GetValue(1) == DBNull.Value) ? String.Empty : reader4.GetString(1);
                            jompd.jobID = (reader4.GetValue(2) == DBNull.Value) ? String.Empty : reader4.GetString(2);
                            jompd.ordPack = (reader4.GetValue(3) == DBNull.Value) ? String.Empty : reader4.GetString(3);
                            jompd.skuNo = (reader4.GetValue(4) == DBNull.Value) ? String.Empty : reader4.GetString(4);
                            jompd.isbn = (reader4.GetValue(5) == DBNull.Value) ? String.Empty : reader4.GetString(5);
                            jompd.isbnSecondary = (reader4.GetValue(6) == DBNull.Value) ? String.Empty : reader4.GetString(6);
                            jompd.skuQty = (reader4.GetValue(7) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(7));
                            jompd.itemID = (reader4.GetValue(8) == DBNull.Value) ? String.Empty : reader4.GetString(8);
                            jompd.totalQty = (reader4.GetValue(9) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(9));
                            jompd.scannedQty = (reader4.GetValue(10) == DBNull.Value) ? 0 : Convert.ToInt32(reader4.GetValue(10));
                            jompd.createdDate = Convert.ToDateTime(createdDate);
                            jompd.rangeTo = dataList[0].rangeTo;

                            jompd.itemPrice = (reader4.GetValue(11) == DBNull.Value) ? 0 : Convert.ToDouble(reader4.GetValue(11));
                            jompd.taxCode = (reader4.GetValue(12) == DBNull.Value) ? String.Empty : reader4.GetString(12);
                            jompd.gstAmount = (reader4.GetValue(13) == DBNull.Value) ? 0 : Convert.ToDouble(reader4.GetValue(13));
                            jompd.deliveryCharge = (reader4.GetValue(14) == DBNull.Value) ? 0 : Convert.ToDouble(reader4.GetValue(14));
                            jompd.deliveryChargeGst = (reader4.GetValue(15) == DBNull.Value) ? 0 : Convert.ToDouble(reader4.GetValue(15));

                            jompdList.Add(jompd);
                            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobordmaster_packdetail. ordMasterPackDetailID = " + jompd.ordMasterPackDetailID + " ");
                        }
                        reader4.Close();
                        cmd4.Dispose();
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_jobordmaster_packdetail.");
                        #endregion
                        #region JobItem
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobitem.");
                        OdbcCommand cmd5 = oCon.CreateCommand();
                        cmd5.CommandText = "select ji.jobitem_id,ji.job_id,ji.createdby,ji.createddate,ii.item_isbn,ji.item_qty,ji.posting_type,ji.mono from wms_jobitem ji," +
                            "imas_item ii where ji.item_id = ii.item_id and job_id = '" + jobIDList[i] + "'";
                        cmd5.CommandTimeout = 300;//Add the commandTimeout - WY-03.SEPT.2014
                        OdbcDataReader reader5 = cmd5.ExecuteReader();

                        while (reader5.Read())
                        {
                            JobItem ji = new JobItem();
                            ji.jobItemID = (reader5.GetValue(0) == DBNull.Value) ? String.Empty : reader5.GetString(0);
                            ji.jobID = (reader5.GetValue(1) == DBNull.Value) ? String.Empty : reader5.GetString(1);
                            ji.createdBy = (reader5.GetValue(2) == DBNull.Value) ? String.Empty : reader5.GetString(2);
                            ji.createdDate = (reader5.GetValue(3) == DBNull.Value) ? DateTime.Now : reader5.GetDateTime(3);
                            ji.itemID = (reader5.GetValue(4) == DBNull.Value) ? String.Empty : reader5.GetString(4);
                            ji.itemQty = (reader5.GetValue(5) == DBNull.Value) ? 0 : Convert.ToInt32(reader5.GetValue(5));
                            ji.postingType = (reader5.GetValue(6) == DBNull.Value) ? String.Empty : reader5.GetString(6);
                            ji.moNo = (reader5.GetValue(7) == DBNull.Value) ? String.Empty : reader5.GetString(7);
                            ji.rangeTo = dataList[0].rangeTo;
                            jiList.Add(ji);
                            this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Retrieving wms_jobitem. jobItemID = " + ji.jobItemID + " ");
                       
                        }
                        reader5.Close();
                        cmd5.Dispose();
                        this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Finished retrieve wms_jobitem.");
                        #endregion
                    
                    #endregion 
                }

                String[] transactionType = new String[] { "MQPUSH-JOB ORD MASTER PACK", "MQPUSH-JOB ORD MASTER PACK DETAIL", 
                                                          "MQPUSH-JOB ORD MASTER", "MQPUSH-JOB ITEM", "MQPUSH-JOB MO PACK" };

                String[] queueLabel = new String[transactionType.Count()];
                for (int j = 0; j < transactionType.Count(); j++)
                {
                    Guid gjob_id = Guid.NewGuid();
                    String MQjob = gjob_id.ToString();
                    queueLabel[j] = "WAREHOUSE" + " - " + MQjob + " > " + transactionType[j];
                }

                bTranStatus = JobOrdMasterExtractionMQ(queueLabel, jomList, jompList, jompdList, jiList, jmpList, transactionType);
                this.SSAPullPushMQLog.Debug("JobOrdMasterExtraction: Sending JobOrdMasterExtraction to MQ.(Inserted Successfully: " + bTranStatus + ")");
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error("JobOrdMasterExtraction Exception: " + ex.ToString());
            }
            finally
            {
                oCon.Close();
            }

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            } 
        }
        /*private String BCASStockPosting(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("BCASStockPosting: Retrieving wms_jobordscan_pack for bcas.");

            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            Boolean bTranStatus = false;
            List<BCASStockPosting> bspList = new List<BCASStockPosting>();
            OdbcCommand cmd2 = oCon.CreateCommand();
            //retrieve bcas only
            cmd2.CommandText = "select josp.jobordmaster_pack_id,josp.status,josp.ordfulfill,josp.ordpoint,josp.posted_ind, jomp.ordpack,jomp.ordprice,j.country_tag " +
                "from wms_jobordscan_pack josp, wms_jobordmaster_pack jomp,wms_job j " +
                "where josp.jobordmaster_pack_id = jomp.jobordmaster_pack_id " +
                "and jomp.job_id = j.job_id and josp.posted_ind = 'N' and j.businesschannel_id = '365c2b8b-9a20-4840-ae50-cfc7a48a8e01' " +
                "and j.createddate > '" + dataList[0].rangeFrom + "' and j.createddate <= '" + dataList[0].rangeTo + "'";
            OdbcDataReader reader2 = cmd2.ExecuteReader();
            try
            {
                while (reader2.Read())
                {
                    BCASStockPosting bsp = new BCASStockPosting();

                    bsp.jobordmaster_pack_id = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                    bsp.status = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                    bsp.ordFulfill = reader2.GetInt32(2);
                    bsp.ordPoint = (reader2.GetValue(3) == DBNull.Value) ? 0 : reader2.GetDouble(3);//reader2.GetDouble(3);
                    bsp.posted_ind = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                    bsp.ordPack = (reader2.GetValue(5) == DBNull.Value) ? String.Empty : reader2.GetString(5);
                    bsp.ordPrice = (reader2.GetValue(6) == DBNull.Value) ? 0 : reader2.GetDouble(6);
                    bsp.subsidiary = (reader2.GetValue(7) == DBNull.Value) ? String.Empty : reader2.GetString(7);
                    bsp.rangeTo = dataList[0].rangeTo;
                    bspList.Add(bsp);
                }
                if (bspList.Count() > 0)
                {
                    Guid gjob_id = Guid.NewGuid();
                    String MQjob = gjob_id.ToString();
                    String transactionType = "MQPUSH-BCAS STOCK POSTING";
                    String queueLabel = "WAREHOUSE" + " - " + MQjob + " > " + transactionType;
                    bTranStatus = BCASStockPostingMQ(queueLabel, bspList, transactionType);
                    this.SSAPullPushMQLog.Debug("Sending wms_jobordscan_pack to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");
                }
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.ToString());
            }
            //reader2.Close();
            //cmd2.Dispose();
            oCon.Close();

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }
        }*/
        private String POReceive(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("Retrieving wms_poreceive.");

            Boolean bTranStatus = false;
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            List<POReceive> porList = new List<POReceive>();
            OdbcCommand cmd2 = oCon.CreateCommand();
            //retrieve bcas only
            cmd2.CommandText = "select poreceive_id,poreceive_desc,pr_id,createdby,createddate,modifiedby,modifieddate,poreceive_number,poreceive_invoice,poreceive_id " +
                "from wms_poreceive where createddate between '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' and '" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";         
            /*cmd2.CommandText = "select po.poreceive_id,po.poreceive_desc,po.pr_id,po.createdby,max(poi.createddate),po.modifiedby,po.modifieddate,po.poreceive_number," +
                "po.poreceive_invoice,po.poreceive_id " +
                "from wms_poreceive po " +
                "join wms_poreceiveitem poi on po.poreceive_id = poi.poreceive_id " +
                "where poi.createddate between '" + dataList[0].rangeFrom + "' and '" + dataList[0].rangeTo + "' "+
                "group by po.poreceive_id";*/
            OdbcDataReader reader2 = cmd2.ExecuteReader();
            try
            {
                while (reader2.Read())
                {
                    POReceive por = new POReceive();

                    por.porID = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                    por.porDesc = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                    por.prID = (reader2.GetValue(2) == DBNull.Value) ? String.Empty : reader2.GetString(2);
                    por.createdBy = (reader2.GetValue(3) == DBNull.Value) ? String.Empty : reader2.GetString(3);
                    por.createdDate = reader2.GetDateTime(4);
                    por.modifiedBy = (reader2.GetValue(5) == DBNull.Value) ? String.Empty : reader2.GetString(5);
                    por.modifiedDate = reader2.GetDateTime(6);
                    por.porNumber = (reader2.GetValue(7) == DBNull.Value) ? String.Empty : reader2.GetString(7);
                    por.porInvoice = (reader2.GetValue(8) == DBNull.Value) ? String.Empty : reader2.GetString(8);
                    por.referenceID = (reader2.GetValue(9) == DBNull.Value) ? String.Empty : reader2.GetString(9);
                    por.rangeTo = dataList[0].rangeTo;
                    porList.Add(por);
                }
                if (porList.Count() > 0)
                {
                    Guid gjob_id = Guid.NewGuid();
                    String MQjob = gjob_id.ToString();
                    String transactionType = "MQPUSH-PO RECEIVE";
                    String queueLabel = "PO RECEIVE" + " - " + MQjob + " > " + transactionType;
                    bTranStatus = POReceiveMQ(queueLabel, porList, transactionType);
                    this.SSAPullPushMQLog.Debug("Sending wms_poreceive to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");
                }
                else
                {
                    bTranStatus = true;
                }
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.ToString());
            }
            oCon.Close();

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }
        }
        private String POReceiveItem(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("Retrieving wms_poreceive.");

            Boolean bTranStatus = false;
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            List<POReceiveItem> poriList = new List<POReceiveItem>();
            OdbcCommand cmd2 = oCon.CreateCommand();
            //retrieve bcas only
            cmd2.CommandText = "select pri.poreceiveitem_id,pri.pritem_id,i.item_isbn,pri.poreceiveitem_qty,pri.poreceive_id,pri.createddate,pri.sort,pri.location_code,pri.invoiceitem_qty,pri.damage_qty " +
                "from wms_poreceive pr join wms_poreceiveitem pri on pr.poreceive_id = pri.poreceive_id " +
                "join imas_item i on pri.item_id = i.item_id " +
                "where pr.createddate between '" + dataList[0].rangeFrom.ToString("MMM dd yyyy hh:mm tt") + "' and '" + dataList[0].rangeTo.ToString("MMM dd yyyy hh:mm tt") + "'";              
            OdbcDataReader reader2 = cmd2.ExecuteReader();
            try
            {
                while (reader2.Read())
                {
                    POReceiveItem pori = new POReceiveItem();

                    pori.poriID = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                    pori.priItemID = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                    pori.itemID = (reader2.GetValue(2) == DBNull.Value) ? String.Empty : reader2.GetString(2);
                    pori.porItemQty = (reader2.GetValue(3) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(3));
                    pori.porID = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                    pori.createdDate = reader2.GetDateTime(5);
                    pori.sort = (reader2.GetValue(6) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(6));
                    pori.locationCode = (reader2.GetValue(7) == DBNull.Value) ? String.Empty : reader2.GetString(7);
                    pori.invItemQty = (reader2.GetValue(8) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(8));
                    pori.dmgQty = (reader2.GetValue(9) == DBNull.Value) ? 0 : Convert.ToInt32(reader2.GetValue(9));
                    
                    poriList.Add(pori);
                }
                if (poriList.Count() > 0)
                {
                    Guid gjob_id = Guid.NewGuid();
                    String MQjob = gjob_id.ToString();
                    String transactionType = "MQPUSH-PO RECEIVE ITEM";
                    String queueLabel = "PO RECEIVE ITEM" + " - " + MQjob + " > " + transactionType;
                    bTranStatus = POReceiveItemMQ(queueLabel, poriList, transactionType);
                    this.SSAPullPushMQLog.Debug("Sending wms_poreceiveitem to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");
                }
                else
                {
                    bTranStatus = true;
                }
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.ToString());
            }
            oCon.Close();

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }
        }
        private List<SOReturn> SOReturnUpdate(List<requestDataForm> dataList)
        {
            this.SSAPullPushMQLog.Debug("Retrieving imas_return.");

            List<SOReturn> sorList = new List<SOReturn>();
            OdbcConnection oCon = new OdbcConnection();
            try
            {
                oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
                oCon.Open();
                OdbcCommand cmd1 = oCon.CreateCommand();
                //cmd1.CommandText = "select * from imas_return where rr_status = 1 and rr_return_date between '" + dataList[0].rangeFrom + "' and '" + dataList[0].rangeTo + "' and  rr_returnby is not null ";
                //Added by Brash Dev on 22-Apr-2021 Start
                cmd1.CommandText = "select * from imas_return where rr_status = 1 and rr_return_date between '" + dataList[0].rangeFrom.AddDays(-1) + "' and '" + dataList[0].rangeTo.AddDays(-1) + "' and  rr_returnby is not null ";
                //End
                OdbcDataReader reader1 = cmd1.ExecuteReader();

                while (reader1.Read())
                {
                    SOReturn sor = new SOReturn();
                    sor.rrID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                    sor.schID = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);
                    sor.rrNumber = (reader1.GetValue(2) == DBNull.Value) ? String.Empty : reader1.GetString(2);
                    sor.rrDate = reader1.GetDateTime(3);
                    sor.rrDesc = (reader1.GetValue(4) == DBNull.Value) ? String.Empty : reader1.GetString(4);
                    sor.rrCreatedBy = (reader1.GetValue(5) == DBNull.Value) ? String.Empty : reader1.GetString(5);
                    sor.rrReference = (reader1.GetValue(6) == DBNull.Value) ? String.Empty : reader1.GetString(6);
                    sor.rrStatus = (reader1.GetValue(7) == DBNull.Value) ? 0 : Convert.ToInt32(reader1.GetValue(7));
                    sor.rrActive = reader1.GetBoolean(8);
                    sor.rrReturnDate = reader1.GetDateTime(9);
                    sor.rrReturnBy = (reader1.GetValue(10) == DBNull.Value) ? String.Empty : reader1.GetString(10);
                    sor.rrCreatedDate = DateTime.Now;
                    sor.rrRangeTo = dataList[0].rangeTo;
                    sorList.Add(sor);
                }
                cmd1.Dispose();
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.Message.ToString());
            }
            finally
            {
                oCon.Close();
            }
            return sorList;
        }
        private String SOReturnItemUpdate(List<SOReturn> dataList)
        {
            this.SSAPullPushMQLog.Debug("Retrieving imas_returnitem.");

            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            List<SOReturnItem> sriList = new List<SOReturnItem>();
            OdbcCommand cmd1 = oCon.CreateCommand();

            String status = "FAIL";
            try
            {
                for (int i = 0; i < dataList.Count(); i++)
                {
                    cmd1.CommandText = "select * from imas_returnitem where rr_id = '" + dataList[i].rrID + "'";
                    this.SSAPullPushMQLog.Debug(cmd1.CommandText);
                    OdbcDataReader reader1 = cmd1.ExecuteReader();

                    while (reader1.Read())
                    {
                        SOReturnItem sri = new SOReturnItem();
                        sri.riID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                        sri.rrID = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);
                        sri.riInvoice = (reader1.GetValue(2) == DBNull.Value) ? String.Empty : reader1.GetString(2);
                        sri.riIsbn = (reader1.GetValue(3) == DBNull.Value) ? String.Empty : reader1.GetString(3);
                        sri.riIsbn2 = (reader1.GetValue(4) == DBNull.Value) ? String.Empty : reader1.GetString(4);
                        sri.riReturnQty = (reader1.GetValue(5) == DBNull.Value) ? 0 : Convert.ToInt32(reader1.GetValue(5));
                        sri.riCreatedDate = reader1.GetDateTime(6);
                        sri.riCreatedBy = (reader1.GetValue(7) == DBNull.Value) ? String.Empty : reader1.GetString(7);
                        sri.riStatus = reader1.GetInt32(8);
                        sri.riRemarks = (reader1.GetValue(9) == DBNull.Value) ? String.Empty : reader1.GetString(9);
                        sri.riMaxReturn = (reader1.GetValue(10) == DBNull.Value) ? 0 : Convert.ToInt32(reader1.GetValue(10));
                        sri.riItemID = (reader1.GetValue(11) == DBNull.Value) ? String.Empty : reader1.GetString(11);
                        sri.riPackID = (reader1.GetValue(12) == DBNull.Value) ? String.Empty : reader1.GetString(12);
                        sri.riReceiveQty = (reader1.GetValue(13) == DBNull.Value) ? 0 : Convert.ToInt32(reader1.GetValue(13));
                        sri.riPostingQty = (reader1.GetValue(14) == DBNull.Value) ? 0 : Convert.ToInt32(reader1.GetValue(14));
                        sri.riRangeTo = dataList[i].rrRangeTo;
                        sriList.Add(sri);
                    }
                    reader1.Close();
                }
                cmd1.Dispose();
                if (sriList.Count() > 0)
                {
                    Guid gjob_id1 = Guid.NewGuid();
                    String MQjob1 = gjob_id1.ToString();
                    String transactionType1 = "MQPUSH-SO RETURN UPDATE";
                    String queueLabel1 = "WMS" + " - " + MQjob1 + " > " + transactionType1;
                    Boolean bTranStatus1 = SOReturnUpdateMQ(queueLabel1, dataList, transactionType1);
                    this.SSAPullPushMQLog.Debug("Sending imas_return to MQ with label: " + queueLabel1 + ".(Inserted Successfully: " + bTranStatus1 + ")");


                    Guid gjob_id2 = Guid.NewGuid();
                    String MQjob2 = gjob_id2.ToString();
                    String transactionType2 = "MQPUSH-SO RETURN ITEM UPDATE";
                    String queueLabel2 = "WMS" + " - " + MQjob2 + " > " + transactionType2;
                    Boolean bTranStatus2 = SOReturnItemUpdateMQ(queueLabel2, sriList, transactionType2);
                    this.SSAPullPushMQLog.Debug("Sending imas_returnitem to MQ with label: " + queueLabel2 + ".(Inserted Successfully: " + bTranStatus2 + ")");

                    if (bTranStatus1 == true && bTranStatus2 == true)
                    {
                        status = "SUCCESS";
                    }
                    else
                    {
                        status = "FAIL";
                    }
                }
                else
                {
                    status = "SUCCESS";
                }
            }
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.Message.ToString());
            }
            finally
            {
                oCon.Close();
            }
            return status;
        }

        // ******************************************************************     
        private Boolean SOFulfillmentMQ(String[] queueLabel, List<JobOrdScan> josList, List<JobOrdScanPack> jospList, List<JobOrdMaster> jomList, List<JobOrdMasterPack> jompList,
            List<JobOrdMasterPackDetail> jompdList, List<JobItem> jiList, List<JobMoPack> jmpList, String[] transactionType)
        {
            Boolean bTranStatus = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            //string qname = @Resource.QUEUENAME_SDE;
            string qname = @Resource.QUEUENAME_SDE2; //Change to queue2 - WY-15.OCT.2014

            MessageQueue messageQueue = null;
            //if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE2))//Change to queue2 - WY-15.OCT.2014
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                //MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                //messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);

                //Change to queue2 - WY-15.OCT.2014
                MessageQueue.Create(@Resource.QUEUENAME_SDE2, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE2);
            }

            msgTx.Begin();

            #region Sending Queue
            try
            {
                Int32 size = Convert.ToInt32(@Resource.SEND_QUEUE_SIZE);
                for (int i = 0; i < transactionType.Count(); i++)
                {
                    //System.Messaging.Message message = new System.Messaging.Message("sde13771");
                    System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE2);//Change to queue2 - WY-15.OCT.2014
                    message.Priority = MessagePriority.Low;
                    message.Recoverable = true;
                    String type = transactionType[i];
                    switch (type)
                    {
                        case "MQPUSH-JOB ORD SCAN":
                            List<List<JobOrdScan>> list0 = new List<List<JobOrdScan>>();
                            Int32 c0 = 0;
                            while (josList.Count > 0)
                            {
                                Int32 count0 = josList.Count > size ? size : josList.Count;
                                list0.Add(josList.GetRange(0, count0));
                                message.Label = queueLabel[i];
                                message.Body = list0[c0];
                                messageQueue.Send(message, msgTx);
                                josList.RemoveRange(0, count0);
                                c0++;
                            }
                            this.SSAPullPushMQLog.Debug("Sending wms_jobordscan to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            break;
                        case "MQPUSH-JOB ORD SCAN PACK":
                            List<List<JobOrdScanPack>> list1 = new List<List<JobOrdScanPack>>();
                            Int32 c1 = 0;
                            while (jospList.Count > 0)
                            {
                                Int32 count1 = jospList.Count > size ? size : jospList.Count;
                                list1.Add(jospList.GetRange(0, count1));
                                message.Label = queueLabel[i];
                                message.Body = list1[c1];
                                messageQueue.Send(message, msgTx);
                                jospList.RemoveRange(0, count1);
                                c1++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordscan_pack to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        /*//Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
                        case "MQPUSH-JOB ORD MASTER PACK":
                            List<List<JobOrdMasterPack>> list2 = new List<List<JobOrdMasterPack>>();
                            Int32 c2 = 0;
                            while (jompList.Count > 0)
                            {
                                Int32 count2 = jompList.Count > size ? size : jompList.Count;
                                list2.Add(jompList.GetRange(0, count2));
                                message.Label = queueLabel[i];
                                message.Body = list2[c2];
                                messageQueue.Send(message, msgTx);
                                jompList.RemoveRange(0, count2);
                                c2++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster_pack to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ORD MASTER PACK DETAIL":
                            List<List<JobOrdMasterPackDetail>> list3 = new List<List<JobOrdMasterPackDetail>>();
                            Int32 c3 = 0;
                            while (jompdList.Count > 0)
                            {
                                Int32 count3 = jompdList.Count > size ? size : jompdList.Count;
                                list3.Add(jompdList.GetRange(0, count3));
                                message.Label = queueLabel[i];
                                message.Body = list3[c3];
                                messageQueue.Send(message, msgTx);
                                jompdList.RemoveRange(0, count3);
                                c3++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster_packdetail to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ITEM":
                            List<List<JobItem>> list4 = new List<List<JobItem>>();
                            Int32 c4 = 0;
                            while (jiList.Count > 0)
                            {
                                Int32 count4 = jiList.Count > size ? size : jiList.Count;
                                list4.Add(jiList.GetRange(0, count4));
                                message.Label = queueLabel[i];
                                message.Body = list4[c4];
                                messageQueue.Send(message, msgTx);
                                jiList.RemoveRange(0, count4);
                                c4++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobitem to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ORD MASTER":
                            List<List<JobOrdMaster>> list5 = new List<List<JobOrdMaster>>();
                            Int32 c5 = 0;
                            while (jomList.Count > 0)
                            {
                                Int32 count5 = jomList.Count > size ? size : jomList.Count;
                                list5.Add(jomList.GetRange(0, count5));
                                message.Label = queueLabel[i];
                                message.Body = list5[c5];
                                messageQueue.Send(message, msgTx);
                                jomList.RemoveRange(0, count5);
                                c5++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB MO PACK":
                            List<List<JobMoPack>> list6 = new List<List<JobMoPack>>();
                            Int32 c6 = 0;
                            while (jmpList.Count > 0)
                            {
                                Int32 count6 = jmpList.Count > size ? size : jmpList.Count;
                                list6.Add(jmpList.GetRange(0, count6));
                                message.Label = queueLabel[i];
                                message.Body = list6[c6];
                                messageQueue.Send(message, msgTx);
                                jmpList.RemoveRange(0, count6);
                                c6++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobmo_pack to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                         */
                    }
                }
                bTranStatus = true;
                msgTx.Commit();
            }
            #endregion
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.ToString());
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }

        //Split the jobordmaster,jobitem,jobmopack table with sofulfillment - WY-26.AUG.2014
        private Boolean JobOrdMasterExtractionMQ(String[] queueLabel, List<JobOrdMaster> jomList, List<JobOrdMasterPack> jompList,
                                                 List<JobOrdMasterPackDetail> jompdList, List<JobItem> jiList, List<JobMoPack> jmpList, String[] transactionType)
        {
            Boolean bTranStatus = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            //string qname = @Resource.QUEUENAME_SDE;
            string qname = @Resource.QUEUENAME_SDE2; //Change to queue2 - WY-15.OCT.2014

            MessageQueue messageQueue = null;
            //if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE2))//Change to queue2 - WY-15.OCT.2014
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                //MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                //messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);

                //Change to queue2 - WY-15.OCT.2014
                MessageQueue.Create(@Resource.QUEUENAME_SDE2, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE2);
            }

            msgTx.Begin();

            #region Sending Queue
            try
            {
                Int32 size = Convert.ToInt32(@Resource.SEND_QUEUE_SIZE);
                for (int i = 0; i < transactionType.Count(); i++)
                {
                    //System.Messaging.Message message = new System.Messaging.Message("sde13771");
                    System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE2);//Change to queue2 - WY-15.OCT.2014
                    message.Priority = MessagePriority.Low;
                    message.Recoverable = true;
                    String type = transactionType[i];
                    switch (type)
                    { 
                        case "MQPUSH-JOB ORD MASTER PACK":
                            List<List<JobOrdMasterPack>> list2 = new List<List<JobOrdMasterPack>>();
                            Int32 c2 = 0;
                            while (jompList.Count > 0)
                            {
                                Int32 count2 = jompList.Count > size ? size : jompList.Count;
                                list2.Add(jompList.GetRange(0, count2));
                                message.Label = queueLabel[i];
                                message.Body = list2[c2];
                                messageQueue.Send(message, msgTx);
                                jompList.RemoveRange(0, count2);
                                c2++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster_pack to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ORD MASTER PACK DETAIL":
                            List<List<JobOrdMasterPackDetail>> list3 = new List<List<JobOrdMasterPackDetail>>();
                            Int32 c3 = 0;
                            while (jompdList.Count > 0)
                            {
                                Int32 count3 = jompdList.Count > size ? size : jompdList.Count;
                                list3.Add(jompdList.GetRange(0, count3));
                                message.Label = queueLabel[i];
                                message.Body = list3[c3];
                                messageQueue.Send(message, msgTx);
                                jompdList.RemoveRange(0, count3);
                                c3++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster_packdetail to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ITEM":
                            List<List<JobItem>> list4 = new List<List<JobItem>>();
                            Int32 c4 = 0;
                            while (jiList.Count > 0)
                            {
                                Int32 count4 = jiList.Count > size ? size : jiList.Count;
                                list4.Add(jiList.GetRange(0, count4));
                                message.Label = queueLabel[i];
                                message.Body = list4[c4];
                                messageQueue.Send(message, msgTx);
                                jiList.RemoveRange(0, count4);
                                c4++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobitem to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB ORD MASTER":
                            List<List<JobOrdMaster>> list5 = new List<List<JobOrdMaster>>();
                            Int32 c5 = 0;
                            while (jomList.Count > 0)
                            {
                                Int32 count5 = jomList.Count > size ? size : jomList.Count;
                                list5.Add(jomList.GetRange(0, count5));
                                message.Label = queueLabel[i];
                                message.Body = list5[c5];
                                messageQueue.Send(message, msgTx);
                                jomList.RemoveRange(0, count5);
                                c5++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobordmaster to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                        case "MQPUSH-JOB MO PACK":
                            List<List<JobMoPack>> list6 = new List<List<JobMoPack>>();
                            Int32 c6 = 0;
                            while (jmpList.Count > 0)
                            {
                                Int32 count6 = jmpList.Count > size ? size : jmpList.Count;
                                list6.Add(jmpList.GetRange(0, count6));
                                message.Label = queueLabel[i];
                                message.Body = list6[c6];
                                messageQueue.Send(message, msgTx);
                                jmpList.RemoveRange(0, count6);
                                c6++;
                                this.SSAPullPushMQLog.Debug("Sending wms_jobmo_pack to MQ with label: " + queueLabel[i] + ".(Inserted Successfully: TRUE)");
                            }
                            break;
                    }
                }
                bTranStatus = true;
                msgTx.Commit();
            }
            #endregion
            catch (Exception ex)
            {
                this.SSAPullPushMQLog.Error(ex.ToString());
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean JobOrdScanPackMQ(String queueLabel, List<JobOrdScanPack> jospList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = jospList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean JobOrdMasterPackMQ(String queueLabel, List<JobOrdMasterPack> jompList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = jompList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean JobOrdMasterPackDetailMQ(String queueLabel, List<JobOrdMasterPackDetail> jompdList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = jompdList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean JobItemMQ(String queueLabel, List<JobItem> jiList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = jiList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        /*private Boolean BCASStockPostingMQ(String queueLabel, List<BCASStockPosting> jospList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = jospList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }*/
        private Boolean POReceiveMQ(String queueLabel, List<POReceive> porList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = porList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean POReceiveItemMQ(String queueLabel, List<POReceiveItem> poriList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = poriList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean SOReturnUpdateMQ(String queueLabel, List<SOReturn> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean SOReturnItemUpdateMQ(String queueLabel, List<SOReturnItem> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message(@Resource.MESSAGE_LABEL_QUEUENAME_SDE);
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        #endregion

        #region General
        public String SplitMoNo(String str)
        {
            String moNo = "";
            if (!String.IsNullOrEmpty(str))
            {
                String[] tempMoNo = str.Split('-');
                moNo = tempMoNo[1];
            }
            return moNo;
        }
        public Decimal? CheckIsZero(Decimal? dec)
        {
            Decimal? decValue = 0;
            if (dec != null)
            { 
                decValue = dec;
            }
            return decValue;
        }
        public String checkIsNull(String str)
        {
            if (String.IsNullOrEmpty(str))
            {
                str = "";
            }
            return str;
        }
        public String checkIsNullToZero(String str)
        {
            if (String.IsNullOrEmpty(str))
            {
                str = "0";
            }
            return str;
        }
        public String GetLastFewChars(string source, int tail_length)
        {
            if (tail_length >= source.Length)
                return source;
            return source.Substring(source.Length - tail_length);
        }

        #endregion

        public void DoWork()
        {
        }
    
    }
}


