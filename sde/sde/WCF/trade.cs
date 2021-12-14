using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MySql.Data.MySqlClient;
using log4net;
using System.Transactions;
using sde.Models;
//using sde.comNetsuiteSandboxServices;
using sde.comNetsuiteServices;
using System.Net;
using System.Collections;
using System.Configuration;
using System.Security.Cryptography;

namespace sde.WCF
{
    public class trade
    {
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");
        //NetSuiteService service = new NetSuiteService();
        //TBA
        string account = @Resource.NETSUITE_LOGIN_ACCOUNT;
        string appID = @Resource.NETSUITE_LOGIN_APPLICATIONID;
        string consumerKey = @Resource.NETSUITE_Consumer_Key;
        string consumerSecret = @Resource.NETSUITE_Consumer_Secret;
        string tokenId, tokenSecret;

        #region Netsuite
        //ANET-28 LIMIT To COMMIT
        public Boolean UpdateSaleOrderCommitTag(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("Update Slaes Order Commit Tag *****************");
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            Int32 soCount = 0;
            Int32 rowCount = 0;
            String jobID = string.Empty;

            this.DataFromNetsuiteLog.Info("Start Update Slaes Order Commit Tag ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };

                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();

                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("StartUpdateSlaesOrderCommitTag: Login Netsuite failed. Exception : " + ex.ToString());

                }

                if (loginStatus == true)
                {

                    using (sdeEntities entities = new sdeEntities())
                    {
                        var query1 = (from q1 in entities.wms_jobordscan
                                      join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                                      join q3 in entities.netsuite_syncso on q1.jos_moNo equals q3.nt2_moNo
                                      where (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                      && (q1.jos_businessChannel_code == "ET" || q1.jos_businessChannel_code == "BC")
                                      && q1.jos_netsuiteProgress == null
                                      select new
                                      {
                                          q1.jos_moNo,
                                          q2.nsjm_moNo_internalID,
                                          q3.nt2_customer_internalID,
                                          q1.jos_rangeTo
                                      }).Distinct().ToList();

                        if (query1.Count > 0)
                        {
                            // This condition for PM task. To check that there is no AM Sales order in PM data
                            if (rangeFrom.Hour > 12 && rangeTo.Hour > 12)
                            {
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour < 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour < 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                            else
                            {
                                // This condition for AM task. To check that there is no PM Sales order in AM data
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour > 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour > 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                        }

                        SalesOrder[] soList = new SalesOrder[query1.Count];
                        foreach (var q1 in query1)
                        {
                            if (q1.nsjm_moNo_internalID != null)
                            {
                                SalesOrder so = new SalesOrder();
                                so.internalId = q1.nsjm_moNo_internalID;
                                try
                                {
                                    var query = entities.netsuite_newso.Where(q => q.nt1_moNo_internalID == q1.nsjm_moNo_internalID).ToList();

                                    //var updateListItem = (from synced in entities.netsuite_syncupdateso
                                    //                      where synced.nt3_moNo == q1.jos_moNo
                                    //                      && synced.nt3_moNo_internalID == q1.nsjm_moNo_internalID
                                    //                      select synced).ToList();

                                    SalesOrderItem[] soii = new SalesOrderItem[query.Count()];
                                    SalesOrderItemList soil = new SalesOrderItemList();

                                    if (query.Count() > 0)
                                    {
                                        int itemCount = 0;
                                        foreach (var item in query)
                                        {
                                            if (item.nt1_itemID == "IC" || item.nt1_itemID == "Non-Inventory")
                                            {
                                            }
                                            else
                                            {
                                                SalesOrderItem soi = new SalesOrderItem();

                                                RecordRef refItem = new RecordRef();
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.nt1_item_internalID;
                                                soi.item = refItem;

                                                soi.line = Convert.ToInt32(item.nt1_itemLine);
                                                soi.lineSpecified = true;

                                                //ANET-28 Sales Order able to apply commit tag at item line.
                                                soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                                //soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                                soi.commitInventorySpecified = true;

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                        }

                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;
                                        rowCount = soCount + 1;

                                        if (rangeFrom.ToLongTimeString() == "4:40:00 PM" && rangeTo.ToLongTimeString() == "4:40:00 PM")
                                        {
                                            var insertTask1 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                          "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-UPDATE COMMIT TAG BEFORE PM', 'SSAUPDATECOMMITTAGBEFORE_TASK." + "SALESORDER_INTERNAlID" + q1.nsjm_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                          "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + insertTask1);
                                            entities.Database.ExecuteSqlCommand(insertTask1);
                                        }
                                        else
                                        {
                                            var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                           "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-UPDATE COMMIT TAG BEFORE', 'SSAUPDATECOMMITTAGBEFORE_TASK." + "SALESORDER_INTERNAlID" + q1.nsjm_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                           "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + insertTask2);
                                            entities.Database.ExecuteSqlCommand(insertTask2);
                                        }

                                        soCount++;
                                        status = true;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("StartUpdateSlaesOrderCommitTag Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }
                        }

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncUpdateList(soList);
                                jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateTaskjobId = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + updateTaskjobId);
                                    entities.Database.ExecuteSqlCommand(updateTaskjobId);

                                    var updateRequestNetsuiteJobID = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + updateRequestNetsuiteJobID);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuiteJobID);
                                }
                            }  
                        }
                        if (rowCount == 0)
                        {
                            if (rangeFrom.ToLongTimeString() == "4:40:00 PM" && rangeTo.ToLongTimeString() == "4:40:00 PM")
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UPDATE COMMIT TAG BEFORE PM' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            else
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UPDATE COMMIT TAG BEFORE' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTag: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        scope1.Complete();
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("StartUpdateSlaesOrderCommitTag: Login Netsuite failed.");
                }
            }

            return true;
        }

        //ANET-28 LIMIT To COMMIT
        public Boolean UpdateSaleOrderCommitTagAfterItemFulFillment(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("Update Slaes Order Commit Tag After Item FulFillment*****************");

            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            Int32 soCount = 0;
            Int32 rowCount = 0;

            this.DataFromNetsuiteLog.Info("Start Update Slaes Order Commit Tag  After Item FulFillment***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };

                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();

                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: Login Netsuite failed. Exception : " + ex.ToString());

                }

                if (loginStatus == true)
                {
                    using (sdeEntities entities = new sdeEntities())
                    {
                        var query1 = (from q1 in entities.wms_jobordscan
                                      join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                                      join q3 in entities.netsuite_syncso on q1.jos_moNo equals q3.nt2_moNo
                                      where (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                      && (q1.jos_businessChannel_code == "ET" || q1.jos_businessChannel_code == "BC")
                                      //&& q1.jos_netsuiteProgress != null
                                      select new
                                      {
                                          q1.jos_moNo,
                                          q2.nsjm_moNo_internalID,
                                          q3.nt2_customer_internalID,
                                          q1.jos_rangeTo
                                      }).Distinct().ToList();
                        if (query1.Count > 0)
                        {
                            // This condition for PM task. To check that there is no AM Sales order in PM data
                            if (rangeFrom.Hour > 12 && rangeTo.Hour > 12)
                            {
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour < 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour < 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                            else
                            {
                                // This condition for AM task. To check that there is no PM Sales order in AM data
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour > 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour > 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                        }
                        // var listForOnlyAMorPM = query1;
                        // This for adding removing the PM from AM set and vice versa.


                        SalesOrder[] soList = new SalesOrder[query1.Count];
                        foreach (var q1 in query1)
                        {
                            if (q1.nsjm_moNo_internalID != null)
                            {
                                SalesOrder so = new SalesOrder();
                                so.internalId = q1.nsjm_moNo_internalID;
                                try
                                {
                                    var query = entities.netsuite_newso.Where(q => q.nt1_moNo_internalID == q1.nsjm_moNo_internalID).ToList();

                                    //var updateListItem = (from synced in entities.netsuite_syncupdateso
                                    //                      where synced.nt3_moNo == q1.jos_moNo
                                    //                      && synced.nt3_moNo_internalID == q1.nsjm_moNo_internalID
                                    //                      select synced).ToList();

                                    SalesOrderItem[] soii = new SalesOrderItem[query.Count()];
                                    SalesOrderItemList soil = new SalesOrderItemList();

                                    if (query.Count() > 0)
                                    {
                                        int itemCount = 0;
                                        foreach (var item in query)
                                        {
                                            if (item.nt1_itemID == "IC" || item.nt1_itemID == "Non-Inventory")
                                            {
                                            }
                                            else
                                            {
                                                SalesOrderItem soi = new SalesOrderItem();

                                                RecordRef refItem = new RecordRef();
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.nt1_item_internalID;
                                                soi.item = refItem;

                                                soi.line = Convert.ToInt32(item.nt1_itemLine);
                                                soi.lineSpecified = true;

                                                //ANET-28 Sales Order able to apply commit tag at item line.
                                                soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                                //soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                                soi.commitInventorySpecified = true;

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }

                                        }

                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;
                                        rowCount = soCount + 1;
                                        if (rangeFrom.ToLongTimeString() == "4:40:00 PM" && rangeTo.ToLongTimeString() == "4:40:00 PM")
                                        {
                                            var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                          "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-UPDATE COMMIT TAG AFTER ITEM FULFILLMENT PM', 'SSAUPDATECOMMITTAGAFTERFULLFILLMENT_TASK." + "SALESORDER_INTERNAlID" + q1.nsjm_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                          "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillmentForPM: " + insertTask2);
                                            entities.Database.ExecuteSqlCommand(insertTask2);
                                        }
                                        else
                                        {
                                            var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                           "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-UPDATE COMMIT TAG AFTER ITEM FULFILLMENT', 'SSAUPDATECOMMITTAGAFTERFULLFILLMENT_TASK." + "SALESORDER_INTERNAlID" + q1.nsjm_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                           "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: " + insertTask2);
                                            entities.Database.ExecuteSqlCommand(insertTask2);
                                        }
                                        soCount++;
                                        status = true;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("StartUpdateSlaesOrderCommitTagAfterItemFulFillment Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }
                        }
                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncUpdateList(soList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateTaskjobId = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: " + updateTaskjobId);
                                    entities.Database.ExecuteSqlCommand(updateTaskjobId);

                                    var updateRequestNetsuiteJobID = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: " + updateRequestNetsuiteJobID);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuiteJobID);
                                }

                                scope1.Complete();
                            } 
                        }
                        if (rowCount == 0)
                        {
                            if (rangeFrom.ToLongTimeString() == "4:40:00 PM" && rangeTo.ToLongTimeString() == "4:40:00 PM")
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UPDATE COMMIT TAG AFTER ITEM FULFILLMENT PM' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillmentForPM: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            else
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                   "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UPDATE COMMIT TAG AFTER ITEM FULFILLMENT' " +
                                   "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            scope1.Complete();
                        }

                    }

                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("StartUpdateSlaesOrderCommitTagAfterItemFulFillment: Login Netsuite failed.");
                }
            }

            return true;
        }

        public Boolean SOFulfillmentUpdate(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate *****************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status1 = false;
            String refNo = null;
            List<ExcessFulfillment> exFulList = new List<ExcessFulfillment>();
            List<String> updateSOList = new List<String>();

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {

                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };


                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;

                Boolean loginStatus = false;

                try
                {
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SOFulfillmentUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //Boolean loginStatus = login();

                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {

                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        //Change to filter by mono only - WY-29.SEPT.2014
                        //var query1 = (from q1 in entities.wms_jobordscan
                        //              join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                        //              where q1.jos_businessChannel_code == "ET"
                        //              && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                        //              && q1.jos_netsuiteProgress == null
                        //              select new
                        //              {
                        //                  q1.jos_moNo,
                        //                  q2.nsjm_moNo_internalID 
                        //              })
                        //                .Distinct()
                        //                .ToList();

                        var query1 = (from q1 in entities.wms_jobordscan
                                      join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                                      join q3 in entities.netsuite_syncso on q1.jos_moNo equals q3.nt2_moNo //FAS order 09/10/2018 - Mohan
                                      where (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                      && (q1.jos_businessChannel_code == "ET" || q1.jos_businessChannel_code == "BC")
                                      && q1.jos_netsuiteProgress == null
                                      select new
                                      {
                                          q1.jos_moNo,
                                          q2.nsjm_moNo_internalID,
                                          q3.nt2_is_fas, //FAS order 09/10/2018 - Mohan
                                          //isFirstRun = q1.jos_netsuiteProgress == null ? "Y" : "N",
                                          //q1.jos_job_ID //to solve 2 sync per day - WY-10.MAR.2015
                                          q1.jos_rangeTo
                                      }).Distinct().ToList();

                        //ANET-28 LIMIT To COMMIT
                        // This condition for PM task. To check that there is no AM Sales order in PM data
                        if (query1.Count > 0)
                        {
                            if (rangeFrom.Hour > 12 && rangeTo.Hour > 12)
                            {
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour < 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour < 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                            else
                            {
                                // This condition for AM task.. To check that there is no PM Sales order in AM data
                                var list = query1.Where(x => x.jos_rangeTo.Value.Hour > 12).ToList();

                                for (int i = 0; i < list.Count; i++)
                                {
                                    int index = query1.FindIndex(l => l.nsjm_moNo_internalID == list[i].nsjm_moNo_internalID);
                                    if (query1[index].jos_rangeTo.Value.Hour > 12)
                                    {
                                        query1.RemoveAt(index);
                                    }
                                }
                            }
                        }
                        //**************************************
                        //to solve 2 sync per day - WY-10.MAR.2015
                        //List<string> _IDjob = new List<string>();
                        //foreach (var qJobID in queryAA)
                        //{
                        //    if (qJobID.isFirstRun == "Y")
                        //    {
                        //        _IDjob.Add(qJobID.jos_job_ID);
                        //    }
                        //}

                        //var query1 = (from d in queryAA
                        //               where d.isFirstRun == "Y"
                        //               select new {d.jos_moNo, d.nsjm_moNo_internalID}).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate: " + query1.Count() + " records to update.");
                        ItemFulfillment[] iffList = new ItemFulfillment[query1.Count()];

                        foreach (var q1 in query1)
                        {
                            if (q1.nt2_is_fas == "Y")
                            {
                                try
                                {
                                    Hashtable htWMSItemsQTY = new Hashtable(); // cpng added: item qty sync down to wms
                                    updateSOList.Add(q1.nsjm_moNo_internalID);

                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = q1.nsjm_moNo_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.itemFulfillment;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;

                                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                    ItemFulfillment iff2 = new ItemFulfillment();

                                    String consigmentNote = string.Empty; //To Add at Memo - WY-29.SEPT.2014
                                    Int32 countCNote = 0;
                                    if (iff1 != null)
                                    {
                                        ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                        iff2.createdFrom = refCreatedFrom;

                                        iff2.tranDate = DateTime.Now;
                                        iff2.tranDateSpecified = true;

                                        //Added for Advanced Inventory - WY-23.JUNE.2015
                                        iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                        iff2.shipStatusSpecified = true;

                                        var cNote = (from qcNote in entities.wms_jobordscan
                                                     where qcNote.jos_businessChannel_code == "BC"
                                                     && (qcNote.jos_rangeTo > rangeFrom && qcNote.jos_rangeTo <= rangeTo)
                                                     && qcNote.jos_netsuiteProgress == null
                                                     && qcNote.jos_moNo == q1.jos_moNo
                                                     select new
                                                     {
                                                         qcNote.jos_deliveryRef
                                                     }).Distinct().ToList();

                                        for (int i = 0; i < cNote.Count; i++)
                                        {
                                            consigmentNote = consigmentNote + cNote[i].jos_deliveryRef;
                                            countCNote = countCNote + 1;
                                            if (!(countCNote == cNote.Count))
                                            {
                                                consigmentNote = consigmentNote + ",";
                                            }
                                        }
                                        iff2.memo = consigmentNote; //To Add at Memo - WY-29.SEPT.2014

                                        ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];

                                        var queryALL = (from josp in entities.wms_jobordscan_pack
                                                        join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                                        join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                                        join jos in entities.wms_jobordscan on new { a = josp.josp_jobID, b = josp.josp_moNo } equals new { a = jos.jos_job_ID, b = jos.jos_moNo }
                                                        where josp.josp_moNo == q1.jos_moNo
                                                        && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                                        && jos.jos_netsuiteProgress == null
                                                        //&& _IDjob.Contains(josp.josp_jobID) //to solve 2 sync per day - WY-10.MAR.2015
                                                        select new
                                                        {
                                                            josp.josp_pack_ID,
                                                            jompd.nsjompd_item_internalID,
                                                            jomp.nsjomp_jobOrdMaster_ID,
                                                            qty = josp.josp_ordFulFill,
                                                            qtyWMS = jomp.nsjomp_ordQty,//cpng
                                                            qtyUnFulFill = josp.josp_ordUnFulFill //cpng
                                                        }).ToList();

                                        var groupNotFulfill = from p in queryALL
                                                              let k = new
                                                              {
                                                                  itemInternalID = p.nsjompd_item_internalID
                                                              }
                                                              group p by k into g
                                                              select new
                                                              {
                                                                  itemInternalID = g.Key.itemInternalID,
                                                                  fulFillQty = g.Sum(p => p.qty),
                                                                  SyncQty = g.Sum(p => p.qtyWMS),//cpng
                                                                  UnFulFillQty = g.Sum(p => p.qtyUnFulFill)//cpng
                                                              };

                                        foreach (var item in groupNotFulfill)
                                        {
                                            int diff = Convert.ToInt32(item.SyncQty) - Convert.ToInt32(item.fulFillQty) - Convert.ToInt32(item.UnFulFillQty);
                                            if (htWMSItemsQTY.Contains(item.itemInternalID))
                                            {
                                                int WMSQty = (int)htWMSItemsQTY[item.itemInternalID];
                                                WMSQty = WMSQty + diff;

                                                htWMSItemsQTY.Remove(item.itemInternalID);
                                                htWMSItemsQTY.Add(item.itemInternalID, WMSQty);
                                            }
                                            else
                                            {
                                                htWMSItemsQTY.Add(item.itemInternalID, diff);
                                            }
                                        }

                                        var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                                   " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                                   " join netsuite_jobordmaster_packdetail jompd on jomp.nsjomp_jobOrdMaster_pack_ID = jompd.nsjompd_jobOrdMaster_pack_ID " +
                                   " join wms_jobordscan jos ON jos.jos_job_ID = josp.josp_jobID AND jos.jos_moNo = josp.josp_moNo " +
                                   " set josp.josp_ordUnFulFill = jomp.nsjomp_ordQty - josp.josp_ordFulFill " +
                                   " where josp.josp_moNo =  '" + q1.jos_moNo + "' " +
                                   " and josp.josp_rangeTo > '" + convertDateToString(rangeFrom) + "' and josp.josp_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                   " AND jos.jos_netsuiteProgress IS NULL ";
                                        //" and josp.josp_jobID in ('" + jobIDUnfulfill + "') ";

                                        this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                                        entities.Database.ExecuteSqlCommand(updateUnfulfill2);

                                        var query2 = from p in queryALL
                                                     where p.qty > 0
                                                     select p;

                                        var groupQ2 = from p in query2
                                                      let k = new
                                                      {
                                                          //jospPackID = p.josp_pack_ID,
                                                          //jobOrdMasterID = p.nsjomp_jobOrdMaster_ID,
                                                          itemInternalID = p.nsjompd_item_internalID,
                                                          //fulFillQty = p.qty
                                                      }
                                                      group p by k into g
                                                      select new
                                                      {
                                                          //jospPackID = g.Key.jospPackID,
                                                          //jobOrdMasterID = g.Key.jobOrdMasterID,
                                                          itemInternalID = g.Key.itemInternalID,
                                                          fulFillQty = g.Sum(p => p.qty)
                                                      };

                                        List<String> deCommitItem = new List<String>();
                                        List<Int32> deCommitQty = new List<Int32>();
                                        Hashtable htDBItems = new Hashtable(); //Add all Netsuite items into Hash Table - WY-26.Dec.2014
                                        foreach (var item in groupQ2)
                                        {
                                            deCommitItem.Add(item.itemInternalID);
                                            deCommitQty.Add(Convert.ToInt32(item.fulFillQty));
                                            htDBItems.Add(item.itemInternalID, Convert.ToInt32(item.fulFillQty));//Add all Netsuite items into Hash Table - WY-26.Dec.2014
                                        }
                                        if (ifitems.Count() > 0)
                                        {
                                            for (int i = 0; i < ifitemlist.item.Length; i++)
                                            {
                                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                                Boolean isExist = false;

                                                iffi.item = ifitemlist.item[i].item;
                                                iffi.quantity = 0;
                                                iffi.quantitySpecified = true;
                                                iffi.itemIsFulfilled = false;
                                                iffi.itemIsFulfilledSpecified = true;
                                                iffi.orderLine = ifitemlist.item[i].orderLine;
                                                iffi.orderLineSpecified = true;


                                                isExist = htDBItems.Contains(ifitemlist.item[i].item.internalId);
                                                if (isExist)
                                                {
                                                    int fulfilQty = (int)htDBItems[ifitemlist.item[i].item.internalId];
                                                    fulfilQty = fulfilQty - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);

                                                    htDBItems.Remove(ifitemlist.item[i].item.internalId);
                                                    htDBItems.Add(ifitemlist.item[i].item.internalId, fulfilQty);
                                                }

                                                int j = deCommitItem.FindIndex(s => s == ifitemlist.item[i].item.internalId);
                                                if (j >= 0)
                                                {
                                                    if (ifitemlist.item[i].item.internalId.Equals(deCommitItem[j]))
                                                    {

                                                        //cpng start
                                                        RecordRef refLocation = new RecordRef();
                                                        refLocation.internalId = @Resource.TRADE_DEFAULT_LOCATION;
                                                        iffi.location = refLocation;
                                                        //cpng end

                                                        if (ifitemlist.item[i].quantityRemaining > deCommitQty[j])
                                                        {
                                                            iffi.quantity = Convert.ToInt32(deCommitQty[j]);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;

                                                            Double excessQty = Convert.ToDouble(ifitemlist.item[i].quantityRemaining - deCommitQty[j]);

                                                            refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                                            var insertExcess = "insert into unscan (ef_moNo,ef_nsjom_jobOrdMasterID,ef_josp_packID,ef_item_internalID,ef_excessQty,ef_refNo,ef_createdDate,ef_rangeTo) values " +
                                                                "('" + q1.jos_moNo + "', '', '','" + ifitemlist.item[i].item.internalId + "','" + excessQty + "','" + refNo + "'," +
                                                                "'" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "')";
                                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertExcess);
                                                            entities.Database.ExecuteSqlCommand(insertExcess);


                                                            //cpng start
                                                            InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                            InventoryAssignment IA = new InventoryAssignment();
                                                            InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                            InventoryDetail ID = new InventoryDetail();

                                                            IA.quantity = Convert.ToInt32(deCommitQty[j]);
                                                            IA.quantitySpecified = true;
                                                            IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                            IAA[0] = IA;
                                                            IAL.inventoryAssignment = IAA;
                                                            ID.inventoryAssignmentList = IAL;

                                                            iffi.inventoryDetail = ID;
                                                            //cpng end

                                                            deCommitQty[j] = deCommitQty[j] - deCommitQty[j];
                                                        }
                                                        else
                                                        {
                                                            iffi.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;

                                                            //cpng start
                                                            InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                            InventoryAssignment IA = new InventoryAssignment();
                                                            InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                            InventoryDetail ID = new InventoryDetail();

                                                            IA.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                            IA.quantitySpecified = true;
                                                            IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                            IAA[0] = IA;
                                                            IAL.inventoryAssignment = IAA;
                                                            ID.inventoryAssignmentList = IAL;

                                                            iffi.inventoryDetail = ID;
                                                            //cpng end

                                                            deCommitQty[j] = deCommitQty[j] - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                        }
                                                        //break;
                                                    }
                                                }
                                                ifitems[i] = iffi;
                                            }
                                            #region Adding Consignment No in Packages - WY-14.JAN.2015
                                            ItemFulfillmentPackage[] ifiPackage = new ItemFulfillmentPackage[1];
                                            ItemFulfillmentPackage iffp = new ItemFulfillmentPackage();
                                            iffp.packageTrackingNumber = consigmentNote;
                                            iffp.packageWeight = 0.1;//Default to 0.1 KGS
                                            iffp.packageWeightSpecified = true;

                                            ItemFulfillmentPackageList iffpl = new ItemFulfillmentPackageList();
                                            iffpl.replaceAll = false;
                                            ifiPackage[0] = iffp;
                                            iffpl.package = ifiPackage;
                                            iff2.packageList = iffpl;
                                            #endregion

                                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                            ifil1.replaceAll = false; //WY-10.NOV.2014
                                            ifil1.item = ifitems;
                                            iff2.itemList = ifil1;

                                            iffList[ordCount] = iff2;

                                            rowCount = ordCount + 1;

                                            refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-FULFILLMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + q1.nsjm_moNo_internalID + "')";

                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);
                                            //above
                                            var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                                       "and jos_moNo = '" + q1.jos_moNo + "' " +
                                                       "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                       "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateTask);
                                            entities.Database.ExecuteSqlCommand(updateTask);

                                            #region Compare NS and DB Items - WY-26.Dec.2014
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Start Compare NS and DB Items");
                                            foreach (DictionaryEntry entry in htDBItems)
                                            {
                                                if (Convert.ToInt32(entry.Value) > 0)
                                                {
                                                    var insertItem = "insert into unfulfillso (uf_transactiontype,uf_mono,uf_itemInternalID,uf_fulfillQty,uf_rangeFrom,uf_rangeTo,uf_createdDate) " +
                                                                     " values ('SSA-FULFILLMENT','" + q1.jos_moNo + "', '" + entry.Key.ToString() + "','" + entry.Value + "', " +
                                                                     " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "') ";
                                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertItem);
                                                    entities.Database.ExecuteSqlCommand(insertItem);
                                                }
                                            }
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: End Compare NS and DB Items");
                                            #endregion

                                            ordCount++;
                                            status1 = true;
                                            UnfulfillBinTransfer(htWMSItemsQTY, q1.jos_moNo);
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Sales order internalID_moNo: " + q1.nsjm_moNo_internalID + "_" + q1.jos_moNo);
                                        }

                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("SOFulfillmentUpdate Exception: " + ex.ToString());
                                    status1 = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }

                            }
                            else
                            {


                                try
                                {
                                    Hashtable htWMSItemsQTY = new Hashtable(); // cpng added: item qty sync down to wms
                                    updateSOList.Add(q1.nsjm_moNo_internalID);

                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = q1.nsjm_moNo_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.itemFulfillment;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;

                                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                    ItemFulfillment iff2 = new ItemFulfillment();

                                    String consigmentNote = string.Empty; //To Add at Memo - WY-29.SEPT.2014
                                    Int32 countCNote = 0;

                                    if (iff1 != null)
                                    {
                                        ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                        iff2.createdFrom = refCreatedFrom;

                                        iff2.tranDate = DateTime.Now;
                                        iff2.tranDateSpecified = true;

                                        //Added for Advanced Inventory - WY-23.JUNE.2015
                                        iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                        iff2.shipStatusSpecified = true;

                                        //Retrieve Consignment Note - WY-29.SEPT.2014
                                        var cNote = (from qcNote in entities.wms_jobordscan
                                                     where qcNote.jos_businessChannel_code == "ET"
                                                     && (qcNote.jos_rangeTo > rangeFrom && qcNote.jos_rangeTo <= rangeTo)
                                                     && qcNote.jos_netsuiteProgress == null
                                                     && qcNote.jos_moNo == q1.jos_moNo
                                                     select new
                                                     {
                                                         qcNote.jos_deliveryRef
                                                     }).Distinct().ToList();

                                        for (int i = 0; i < cNote.Count; i++)
                                        {
                                            consigmentNote = consigmentNote + cNote[i].jos_deliveryRef;
                                            countCNote = countCNote + 1;
                                            if (!(countCNote == cNote.Count))
                                            {
                                                consigmentNote = consigmentNote + ",";
                                            }
                                        }
                                        iff2.memo = consigmentNote; //To Add at Memo - WY-29.SEPT.2014

                                        ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];
                                        /*
                                        var query12 = (from jomp in entities.netsuite_jobordmaster_pack
                                                       join josp in entities.wms_jobordscan_pack on jomp.nsjomp_jobOrdMaster_pack_ID equals josp.josp_pack_ID
                                                       join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                                       where jomp.nsjomp_job_ID == q1.jos_job_ID && josp.josp_ordFulFill > 0
                                                       && jomp.nsjomp_moNo == q1.jos_moNo
                                                       select new { josp.josp_pack_ID, jompd.nsjompd_item_internalID, qty = josp.josp_ordFulFill }).ToList();//qty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty)
                                        */
                                        //var query2 = (from josp in entities.wms_jobordscan_pack
                                        //              join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                        //              join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                        //              where josp.josp_jobID == q1.jos_job_ID && josp.josp_ordFulFill > 0
                                        //              && josp.josp_moNo == q1.jos_moNo && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                        //              //select new { jompd.nsjompd_item_internalID, qty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty) }).ToList();
                                        //              select new { josp.josp_pack_ID, jompd.nsjompd_item_internalID, jomp.nsjomp_jobOrdMaster_ID, qty = josp.josp_ordFulFill/*qty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty)*/ }).ToList();

                                        /*cpng start*/
                                        var queryALL = (from josp in entities.wms_jobordscan_pack
                                                        join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                                        join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                                        join jos in entities.wms_jobordscan on new { a = josp.josp_jobID, b = josp.josp_moNo } equals new { a = jos.jos_job_ID, b = jos.jos_moNo }
                                                        where josp.josp_moNo == q1.jos_moNo
                                                        && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                                        && jos.jos_netsuiteProgress == null
                                                        //&& _IDjob.Contains(josp.josp_jobID) //to solve 2 sync per day - WY-10.MAR.2015
                                                        select new
                                                        {
                                                            josp.josp_pack_ID,
                                                            jompd.nsjompd_item_internalID,
                                                            jomp.nsjomp_jobOrdMaster_ID,
                                                            qty = josp.josp_ordFulFill, //98
                                                            qtyWMS = jomp.nsjomp_ordQty,//cpng 100
                                                            qtyUnFulFill = josp.josp_ordUnFulFill //cpng 0
                                                        }).ToList();

                                        var groupNotFulfill = from p in queryALL
                                                              let k = new
                                                              {
                                                                  itemInternalID = p.nsjompd_item_internalID
                                                              }
                                                              group p by k into g
                                                              select new
                                                              {
                                                                  itemInternalID = g.Key.itemInternalID,
                                                                  fulFillQty = g.Sum(p => p.qty),
                                                                  SyncQty = g.Sum(p => p.qtyWMS),//cpng
                                                                  UnFulFillQty = g.Sum(p => p.qtyUnFulFill)//cpng
                                                              };

                                        foreach (var item in groupNotFulfill)
                                        {
                                            int diff = Convert.ToInt32(item.SyncQty) - Convert.ToInt32(item.fulFillQty) - Convert.ToInt32(item.UnFulFillQty);
                                            if (htWMSItemsQTY.Contains(item.itemInternalID))
                                            {
                                                int WMSQty = (int)htWMSItemsQTY[item.itemInternalID];
                                                WMSQty = WMSQty + diff;

                                                htWMSItemsQTY.Remove(item.itemInternalID);
                                                htWMSItemsQTY.Add(item.itemInternalID, WMSQty);
                                            }
                                            else
                                            {
                                                htWMSItemsQTY.Add(item.itemInternalID, diff);
                                            }
                                        }


                                        //(from josp in entities.wms_jobordscan_pack
                                        // join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                        // join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                        // where josp.josp_moNo == q1.jos_moNo
                                        // && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                        // && _IDjob.Contains(josp.josp_jobID) //to solve 2 sync per day - WY-10.MAR.2015
                                        // select new { josp, jomp }).ToList().ForEach(x => x.josp.josp_ordUnFulFill = x.jomp.nsjomp_ordQty - x.josp.josp_ordFulFill);
                                        //entities.SaveChanges();

                                        //string jobIDUnfulfill = String.Join("', '", _IDjob.ToArray());

                                        //if (!string.IsNullOrEmpty(jobIDUnfulfill))
                                        //{
                                        var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                                        " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                                        " join netsuite_jobordmaster_packdetail jompd on jomp.nsjomp_jobOrdMaster_pack_ID = jompd.nsjompd_jobOrdMaster_pack_ID " +
                                        " join wms_jobordscan jos ON jos.jos_job_ID = josp.josp_jobID AND jos.jos_moNo = josp.josp_moNo " +
                                        " set josp.josp_ordUnFulFill = jomp.nsjomp_ordQty - josp.josp_ordFulFill " +
                                        " where josp.josp_moNo =  '" + q1.jos_moNo + "' " +
                                        " and josp.josp_rangeTo > '" + convertDateToString(rangeFrom) + "' and josp.josp_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                        " AND jos.jos_netsuiteProgress IS NULL ";
                                        //" and josp.josp_jobID in ('" + jobIDUnfulfill + "') ";

                                        this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                                        entities.Database.ExecuteSqlCommand(updateUnfulfill2);
                                        //}


                                        //string jobID = String.Join(", ", _IDjob.ToArray());

                                        //if (!string.IsNullOrEmpty(jobID))
                                        //{
                                        //    var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                                        //    " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                                        //    " set josp.josp_ordUnFulFill = jomp.nsjomp_ordQty - josp.josp_ordFulFill " +
                                        //    " where josp.josp_moNo =  '" + q1.jos_moNo + "' " +
                                        //    " and josp.josp_rangeTo > '" + rangeFrom + "' and josp.josp_rangeTo <= '" + rangeFrom + "' " +
                                        //    " and josp.josp_jobID in ('" + jobID + "') ";

                                        //    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                                        //    entities.Database.ExecuteSqlCommand(updateUnfulfill2);
                                        //}

                                        /*cpng end*/

                                        //Change to filter by mono only - WY-29.SEPT.2014
                                        //var query2 = (from josp in entities.wms_jobordscan_pack
                                        //              join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                        //              join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                        //              where josp.josp_ordFulFill > 0 && josp.josp_moNo == q1.jos_moNo
                                        //              && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                        //              && _IDjob.Contains(josp.josp_jobID) //to solve 2 sync per day - WY-10.MAR.2015
                                        //              select new
                                        //              {
                                        //                  josp.josp_pack_ID,
                                        //                  jompd.nsjompd_item_internalID,
                                        //                  jomp.nsjomp_jobOrdMaster_ID,
                                        //                  qty = josp.josp_ordFulFill,
                                        //                  qtyWMS = jomp.nsjomp_ordQty//cpng
                                        //              }).ToList();

                                        var query2 = from p in queryALL
                                                     where p.qty > 0
                                                     select p;

                                        var groupQ2 = from p in query2
                                                      let k = new
                                                      {
                                                          //jospPackID = p.josp_pack_ID,
                                                          //jobOrdMasterID = p.nsjomp_jobOrdMaster_ID,
                                                          itemInternalID = p.nsjompd_item_internalID,
                                                          //fulFillQty = p.qty
                                                      }
                                                      group p by k into g
                                                      select new
                                                      {
                                                          //jospPackID = g.Key.jospPackID,
                                                          //jobOrdMasterID = g.Key.jobOrdMasterID,
                                                          itemInternalID = g.Key.itemInternalID,
                                                          fulFillQty = g.Sum(p => p.qty)
                                                      };

                                        List<String> deCommitItem = new List<String>();
                                        List<Int32> deCommitQty = new List<Int32>();
                                        Hashtable htDBItems = new Hashtable(); //Add all Netsuite items into Hash Table - WY-26.Dec.2014
                                        foreach (var item in groupQ2)
                                        {
                                            deCommitItem.Add(item.itemInternalID);
                                            deCommitQty.Add(Convert.ToInt32(item.fulFillQty));
                                            htDBItems.Add(item.itemInternalID, Convert.ToInt32(item.fulFillQty));//Add all Netsuite items into Hash Table - WY-26.Dec.2014
                                        }



                                        /*
                                        var scanpack = (from josp in entities.wms_jobordscan_pack
                                                        join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                                        //where josp.josp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                        where josp.josp_pack_ID == jomp.nsjomp_jobOrdMaster_pack_ID && jomp.nsjomp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                        select new { jomp.nsjomp_ordPack, jomp.nsjomp_item_internalID, josp.josp_pack_ID, josp.josp_ordFulFill, jomp.nsjomp_location_internalID }).ToList();
                                        */

                                        if (ifitems.Count() > 0)
                                        {
                                            //String josp_packID = null;
                                            for (int i = 0; i < ifitemlist.item.Length; i++)
                                            {
                                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                                Boolean isExist = false;

                                                iffi.item = ifitemlist.item[i].item;
                                                iffi.quantity = 0;
                                                iffi.quantitySpecified = true;
                                                iffi.itemIsFulfilled = false;
                                                iffi.itemIsFulfilledSpecified = true;
                                                iffi.orderLine = ifitemlist.item[i].orderLine;
                                                iffi.orderLineSpecified = true;

                                                /*cpng start*/
                                                //var updateUnfulfill = "update netsuite_syncso set nt2_unfulfilledQty = 0 " +
                                                //" where nt2_moNo_internalID = '" + q1.nsjm_moNo_internalID + "' " +
                                                //" and nt2_item_internalID = '" + ifitemlist.item[i].item.internalId + "' ";

                                                //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill);
                                                //entities.Database.ExecuteSqlCommand(updateUnfulfill);
                                                /*cpng end*/

                                                isExist = htDBItems.Contains(ifitemlist.item[i].item.internalId);
                                                if (isExist)
                                                {
                                                    int fulfilQty = (int)htDBItems[ifitemlist.item[i].item.internalId];
                                                    fulfilQty = fulfilQty - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);

                                                    htDBItems.Remove(ifitemlist.item[i].item.internalId);
                                                    htDBItems.Add(ifitemlist.item[i].item.internalId, fulfilQty);
                                                }

                                                int j = deCommitItem.FindIndex(s => s == ifitemlist.item[i].item.internalId);
                                                if (j >= 0)
                                                {
                                                    if (ifitemlist.item[i].item.internalId.Equals(deCommitItem[j]))
                                                    {
                                                        //josp_packID = item.jospPackID;

                                                        /*
                                                        RecordRef refLocation = new RecordRef();
                                                        refLocation.internalId = item.nsjomp_location_internalID;
                                                        iffi.location = refLocation;
                                                        */




                                                        //cpng start
                                                        RecordRef refLocation = new RecordRef();
                                                        refLocation.internalId = @Resource.TRADE_DEFAULT_LOCATION;
                                                        iffi.location = refLocation;
                                                        //cpng end

                                                        if (ifitemlist.item[i].quantityRemaining > deCommitQty[j])
                                                        {
                                                            iffi.quantity = Convert.ToInt32(deCommitQty[j]);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;

                                                            Double excessQty = Convert.ToDouble(ifitemlist.item[i].quantityRemaining - deCommitQty[j]);

                                                            refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                                            var insertExcess = "insert into unscan (ef_moNo,ef_nsjom_jobOrdMasterID,ef_josp_packID,ef_item_internalID,ef_excessQty,ef_refNo,ef_createdDate,ef_rangeTo) values " +
                                                                "('" + q1.jos_moNo + "', '', '','" + ifitemlist.item[i].item.internalId + "','" + excessQty + "','" + refNo + "'," +
                                                                "'" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "')";
                                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertExcess);
                                                            entities.Database.ExecuteSqlCommand(insertExcess);

                                                            /*
                                                            ExcessFulfillment exFul = new ExcessFulfillment();
                                                            exFul.moNo = q1.jos_moNo;
                                                            exFul.josp_packID = item.jospPackID;
                                                            exFul.jobOrdMasterID = q1.nsjom_jobOrdMaster_ID;
                                                            exFul.itemInternalID = ifitemlist.item[i].item.internalId;
                                                            //exFul.locationInternalID = item.nsjomp_location_internalID;
                                                            exFul.excessQty = ifitemlist.item[i].quantityRemaining - item.fulFillQty;
                                                            exFul.refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                                            exFulList.Add(exFul);
                                                             * */

                                                            //cpng start
                                                            InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                            InventoryAssignment IA = new InventoryAssignment();
                                                            InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                            InventoryDetail ID = new InventoryDetail();

                                                            IA.quantity = Convert.ToInt32(deCommitQty[j]);
                                                            IA.quantitySpecified = true;
                                                            IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                            IAA[0] = IA;
                                                            IAL.inventoryAssignment = IAA;
                                                            ID.inventoryAssignmentList = IAL;

                                                            iffi.inventoryDetail = ID;
                                                            //cpng end

                                                            deCommitQty[j] = deCommitQty[j] - deCommitQty[j];
                                                        }
                                                        else
                                                        {
                                                            iffi.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;

                                                            //cpng start
                                                            InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                            InventoryAssignment IA = new InventoryAssignment();
                                                            InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                            InventoryDetail ID = new InventoryDetail();

                                                            IA.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                            IA.quantitySpecified = true;
                                                            IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                            IAA[0] = IA;
                                                            IAL.inventoryAssignment = IAA;
                                                            ID.inventoryAssignmentList = IAL;

                                                            iffi.inventoryDetail = ID;
                                                            //cpng end

                                                            deCommitQty[j] = deCommitQty[j] - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                        }
                                                        //break;
                                                    }
                                                }
                                                ifitems[i] = iffi;
                                            }

                                            #region Adding Consignment No in Packages - WY-14.JAN.2015
                                            ItemFulfillmentPackage[] ifiPackage = new ItemFulfillmentPackage[1];
                                            ItemFulfillmentPackage iffp = new ItemFulfillmentPackage();
                                            iffp.packageTrackingNumber = consigmentNote;
                                            iffp.packageWeight = 0.1;//Default to 0.1 KGS
                                            iffp.packageWeightSpecified = true;

                                            ItemFulfillmentPackageList iffpl = new ItemFulfillmentPackageList();
                                            iffpl.replaceAll = false;
                                            ifiPackage[0] = iffp;
                                            iffpl.package = ifiPackage;
                                            iff2.packageList = iffpl;
                                            #endregion

                                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                            ifil1.replaceAll = false; //WY-10.NOV.2014
                                            ifil1.item = ifitems;
                                            iff2.itemList = ifil1;

                                            iffList[ordCount] = iff2;

                                            rowCount = ordCount + 1;
                                            //refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                            //Change to filter by mono only - WY-29.SEPT.2014
                                            refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-FULFILLMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + q1.nsjm_moNo_internalID + "')";

                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            //var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                            //                    "and jos_job_ID = '" + q1.jos_job_ID + "' " +
                                            //                    "and jos_moNo = '" + q1.jos_moNo + "' " +
                                            //                    "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                            //                    "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                            //Change to filter by mono only - WY-29.SEPT.2014
                                            var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                                             "and jos_moNo = '" + q1.jos_moNo + "' " +
                                                             "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                             "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateTask);
                                            entities.Database.ExecuteSqlCommand(updateTask);

                                            #region Compare NS and DB Items - WY-26.Dec.2014
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Start Compare NS and DB Items");
                                            foreach (DictionaryEntry entry in htDBItems)
                                            {
                                                if (Convert.ToInt32(entry.Value) > 0)
                                                {
                                                    var insertItem = "insert into unfulfillso (uf_transactiontype,uf_mono,uf_itemInternalID,uf_fulfillQty,uf_rangeFrom,uf_rangeTo,uf_createdDate) " +
                                                                     " values ('SSA-FULFILLMENT','" + q1.jos_moNo + "', '" + entry.Key.ToString() + "','" + entry.Value + "', " +
                                                                     " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "') ";
                                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertItem);
                                                    entities.Database.ExecuteSqlCommand(insertItem);
                                                }
                                            }
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: End Compare NS and DB Items");
                                            #endregion

                                            ordCount++;
                                            status1 = true;
                                            UnfulfillBinTransfer(htWMSItemsQTY, q1.jos_moNo);
                                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Sales order internalID_moNo: " + q1.nsjm_moNo_internalID + "_" + q1.jos_moNo);
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("SOFulfillmentUpdate Exception: " + ex.ToString());
                                    status1 = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }
                            //end of else
                        }//end of ordMaster

                        if (status1 == true)
                        {
                            if (rowCount > 0)
                            {
                                //WriteResponse[] res = service.addList(iffList);
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(iffList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateOrdScan = "update wms_jobordscan set jos_netsuiteProgress = '" + jobID + "' where jos_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateOrdScan);
                                    entities.Database.ExecuteSqlCommand(updateOrdScan);

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("SOFulfillmentUpdate: Login Netsuite failed.");
                }
            }//end of scopeOuter
            //logout();
            return status1;
        }
        public Boolean InventoryAdjustmentUpdate(DateTime rangeFrom, DateTime rangeTo)
        {
            //Run inventory adjustment to adjust out item quantity after TRADE sales orders has excess fulfillment
            //public Boolean InventoryAdjustmentUpdate(List<ExcessFulfillment> exFulList)
            this.DataFromNetsuiteLog.Info("InventoryAdjustmentUpdate ***************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;


                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;

                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("InventoryAdjustmentUpdate: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("InventoryAdjustmentUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("InventoryAdjustmentUpdate: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        //var exJOMGroup = exFulList.Select(x => x.jobOrdMasterID).Distinct().ToList();
                        //var exPackGroup = exFulList.Select(x => x.josp_packID).Distinct().ToList();

                        var exJOMGroup = (from ef in entities.unscans
                                          where ef.ef_createdDate > rangeFrom
                                          && ef.ef_createdDate <= rangeTo
                                          select new { ef.ef_nsjom_jobOrdMasterID }).Distinct().ToList();

                        var exPackGroup = (from ef in entities.unscans
                                           where ef.ef_createdDate > rangeFrom
                                           && ef.ef_createdDate <= rangeTo
                                           select new { ef.ef_josp_packID }).Distinct().ToList();

                        //status = true;
                        InventoryAdjustment[] invAdjList = new InventoryAdjustment[exJOMGroup.Count()];

                        //for (int i = 0; i < exJOMGroup.Count(); i++)
                        Int32 count = 0;
                        foreach (var i in exJOMGroup)
                        {
                            String jobOrdMasterID = i.ef_nsjom_jobOrdMasterID;//exJOMGroup[i];
                            var query1 = (from jom in entities.netsuite_jobordmaster
                                          join j in entities.netsuite_job on jom.nsjom_nsj_job_ID equals j.nsj_jobID
                                          join b in entities.map_businesschannel on j.nsj_businessChannel_ID equals b.mb_imas_businessChannel_ID
                                          //join s in entities.map_subsidiary on j.nsj_country_tag equals s.ms_countryName
                                          where jom.nsjom_jobOrdMaster_ID == jobOrdMasterID
                                          select new { b.mb_businessChannel_internalID }).ToList();

                            this.DataFromNetsuiteLog.Info("InventoryAdjustmentUpdate: " + query1.Count() + " records to update.");
                            foreach (var q1 in query1)
                            {
                                try
                                {
                                    Int32 itemCount = 0;
                                    InventoryAdjustment invAdj = new InventoryAdjustment();
                                    InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                                    RecordRef refAccount = new RecordRef();
                                    refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_WRITEOFFDMG;

                                    RecordRef refSubsidiary = new RecordRef();
                                    refSubsidiary.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refBusinessChannel = new RecordRef();
                                    refBusinessChannel.internalId = q1.mb_businessChannel_internalID;

                                    invAdj.account = refAccount;
                                    invAdj.tranDate = DateTime.Now;
                                    invAdj.subsidiary = refSubsidiary;
                                    invAdj.@class = refBusinessChannel;

                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[exPackGroup.Count()];

                                    var itemGroup = (from ig in entities.unscans
                                                     where ig.ef_nsjom_jobOrdMasterID == jobOrdMasterID
                                                     select ig).ToList();

                                    String refNo = null;
                                    foreach (var ig in itemGroup)
                                    {
                                        refNo = ig.ef_refNo;

                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = ig.ef_item_InternalID;

                                        RecordRef refLoc = new RecordRef();
                                        refLoc.internalId = @Resource.TRADE_EXCESSFULFILLMENTLOCATION_INTERNALID;

                                        //RecordRef refLocation = new RecordRef();
                                        //refLocation.internalId = ig.locationInternalID;

                                        InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();
                                        item.item = refItem;
                                        item.location = refLoc;//refLocation;
                                        item.adjustQtyBy = -(Convert.ToDouble(ig.ef_excessQty));
                                        item.adjustQtyBySpecified = true;

                                        //cpng start
                                        InventoryAssignment[] IAA = new InventoryAssignment[1];
                                        InventoryAssignment IA = new InventoryAssignment();
                                        InventoryAssignmentList IAL = new InventoryAssignmentList();
                                        InventoryDetail ID = new InventoryDetail();

                                        IA.quantity = -(Convert.ToDouble(ig.ef_excessQty));
                                        IA.quantitySpecified = true;
                                        IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN };
                                        IAA[0] = IA;
                                        IAL.inventoryAssignment = IAA;
                                        ID.inventoryAssignmentList = IAL;

                                        item.inventoryDetail = ID;
                                        //cpng end

                                        items[itemCount] = item;
                                        itemCount++;
                                    }

                                    iail.inventory = items;
                                    invAdj.inventoryList = iail;
                                    invAdjList[count] = invAdj;

                                    rowCount = count + 1;

                                    refNo = "UNSCANS.EF_NSJOM_JOBORDMASTERID." + jobOrdMasterID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-UNSCAN', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("InventoryAdjustmentUpdate: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    count++;
                                    status = true;
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("InventoryAdjustmentUpdate Exception: (" + q1.mb_businessChannel_internalID + ")" + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }
                        }
                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(invAdjList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("InventoryAdjustmentUpdate: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UNSCAN' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-UNSCAN' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("InventoryAdjustmentUpdate: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("InventoryAdjustmentUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }
        public Boolean POUpdate(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("POUpdate *****************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            DateTime createdDate = DateTime.Now;
            List<ExcessPO> exPOList = new List<ExcessPO>();
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;


                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("POUpdate: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("POUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("POUpdate: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        try
                        {
                            AsyncStatusResult job = new AsyncStatusResult();
                            Int32 poCount = 0;
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();

                            var por = (from po in entities.wms_poreceive
                                       join pr in entities.netsuite_pr on po.po_pr_ID equals pr.nspr_pr_ID
                                       where po.po_rangeTo > rangeFrom && po.po_rangeTo <= rangeTo
                                       && po.po_netsuitePO == null
                                       select new { pr.nspr_pr_ID, pr.nspr_pr_internalID, pr.nspr_pr_location_internalID, pr.nspr_businessChannel_internalID }).Distinct().ToList();

                            this.DataFromNetsuiteLog.Info("POUpdate: " + por.Count() + " records to update.");
                            //status = true;
                            ItemReceipt[] irList = new ItemReceipt[por.Count()];

                            foreach (var p in por)
                            {
                                status = true;

                                InitializeRef refPO = new InitializeRef();
                                refPO.type = InitializeRefType.purchaseOrder;
                                refPO.internalId = p.nspr_pr_internalID;
                                refPO.typeSpecified = true;

                                InitializeRecord recPO = new InitializeRecord();
                                recPO.type = InitializeType.itemReceipt;
                                recPO.reference = refPO;

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                ReadResponse rrPO = netsuiteService.initialize(recPO);
                                Record rPO = rrPO.record;

                                ItemReceipt ir1 = (ItemReceipt)rPO;
                                ItemReceipt ir2 = new ItemReceipt();

                                if (ir1 != null)
                                {
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = ir1.createdFrom.internalId;
                                    ir2.createdFrom = refCreatedFrom;

                                    ItemReceiptItemList iril1 = new ItemReceiptItemList();
                                    iril1 = ir1.itemList;

                                    for (int i = 0; i < iril1.item.Count(); i++)
                                    {

                                        String itemID = iril1.item[i].item.name;
                                        String itemInternalID = iril1.item[i].item.internalId;

                                        var query_poreceiveID = (from po in entities.wms_poreceive
                                                                 join pr in entities.netsuite_pr on po.po_pr_ID equals pr.nspr_pr_ID
                                                                 where po.po_rangeTo > rangeFrom && po.po_rangeTo <= rangeTo
                                                                 && po.po_netsuitePO == null
                                                                 && po.po_pr_ID == p.nspr_pr_ID
                                                                 select new
                                                                 {
                                                                     po.po_poreceive_ID
                                                                 }).Distinct().ToList();

                                        List<string> _IDjob = new List<string>();
                                        foreach (var qporeceiveID in query_poreceiveID)
                                        {

                                            _IDjob.Add(qporeceiveID.po_poreceive_ID);
                                        }

                                        var poi = (from j in entities.wms_poreceiveitem
                                                   where j.poi_createdDate > rangeFrom
                                                   && j.poi_createdDate <= rangeTo
                                                   && _IDjob.Contains(j.poi_poreceive_ID)
                                                       //&& j.poi_item_ID == itemID
                                                   && j.poi_item_internalID == itemInternalID
                                                   && j.poi_netsuitePO == null
                                                   select j).ToList();

                                        if (poi.Count() > 0)
                                        {
                                            Double receiveQty = 0;
                                            String poiItemID = null;
                                            String poiItemInternalID = null;
                                            foreach (var item in poi)
                                            {
                                                receiveQty += Convert.ToDouble(item.poi_poreceiveItem_qty);
                                                poiItemID = item.poi_item_ID;
                                                poiItemInternalID = item.poi_item_internalID;
                                            }

                                            if (iril1.item[i].quantityRemaining >= receiveQty)
                                            {
                                                iril1.item[i].quantity = receiveQty;
                                                iril1.item[i].itemReceive = true;
                                                iril1.item[i].itemReceiveSpecified = true;

                                                //cpng start
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = Convert.ToInt32(receiveQty);
                                                IA.quantitySpecified = true;
                                                if (p.nspr_businessChannel_internalID == "101")// if business channel = Book Clubs
                                                {
                                                    IA.binNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN };
                                                }
                                                else // else business channel = Trade
                                                {
                                                    IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN };
                                                }
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iril1.item[i].inventoryDetail = ID;
                                                //cpng end
                                            }
                                            else if (iril1.item[i].quantityRemaining < receiveQty)
                                            {
                                                Double excessQty = receiveQty - iril1.item[i].quantityRemaining;

                                                //iril1.item[i].item.name = poiItemID;
                                                iril1.item[i].item.internalId = poiItemInternalID;
                                                iril1.item[i].quantity = iril1.item[i].quantityRemaining;
                                                iril1.item[i].itemReceive = true;
                                                iril1.item[i].itemReceiveSpecified = true;

                                                //cpng start
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = Convert.ToInt32(iril1.item[i].quantityRemaining);
                                                IA.quantitySpecified = true;
                                                if (p.nspr_businessChannel_internalID == "101")// if business channel = Book Clubs
                                                {
                                                    IA.binNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN };
                                                }
                                                else // else business channel = Trade
                                                {
                                                    IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN };
                                                }
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iril1.item[i].inventoryDetail = ID;
                                                //cpng end

                                                var insertExcessPO = "insert into excesspo (ep_priD,ep_poreceiveItemID,ep_itemInternalID,ep_excessQty,ep_createdDate,ep_rangeTo) " +
                                                    "values ('" + p.nspr_pr_ID + "','" + poiItemInternalID + "','" + iril1.item[i].item.internalId + "'," +
                                                    "'" + excessQty + "','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "')";
                                                this.DataFromNetsuiteLog.Debug("POUpdate: " + insertExcessPO);
                                                entities.Database.ExecuteSqlCommand(insertExcessPO);
                                            }

                                            //#601 -Begin
                                            string jobIDPoreceive = String.Join("', '", _IDjob.ToArray());
                                            var updateTask = "update wms_poreceiveitem set poi_netsuitePO = '" + gjob_id.ToString() + "' where poi_netsuitePO is null " +
                                                             "and poi_createdDate > '" + convertDateToString(rangeFrom) + "' " +
                                                             "and poi_createdDate <= '" + convertDateToString(rangeTo) + "' " +
                                                             "and poi_poreceive_ID in ('" + jobIDPoreceive + "') " +
                                                             "and poi_item_ID = '" + itemID + "' ";

                                            this.DataFromNetsuiteLog.Debug("POUpdate: " + updateTask);
                                            entities.Database.ExecuteSqlCommand(updateTask);
                                            //#601 -End
                                        }
                                        else
                                        {
                                            iril1.item[i].quantity = 0;
                                            iril1.item[i].itemReceive = false;
                                            iril1.item[i].itemReceiveSpecified = true;
                                        }

                                        //RecordRef refLocation = new RecordRef();
                                        //refLocation.internalId = p.nspr_pr_location_internalID;
                                        //iril1.item[i].location = refLocation;
                                    }

                                    ir2.itemList = iril1;
                                    irList[poCount] = ir2;

                                    rowCount = poCount + 1;

                                    //#601 -Begin
                                    var updateTaskH = "update wms_poreceive set po_netsuitePO = '" + gjob_id.ToString() + "' where po_netsuitePO is null " +
                                                     "and po_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                     "and po_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                     "and po_pr_ID = '" + p.nspr_pr_ID + "'  ";

                                    this.DataFromNetsuiteLog.Debug("POUpdate: " + updateTaskH);
                                    entities.Database.ExecuteSqlCommand(updateTaskH);
                                    //#601 -End

                                    String refNo = "WMSPORECEIVE.PO_PORCEIVE_ID." + p.nspr_pr_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-PURCHASE ORDER', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + p.nspr_pr_internalID + "')";
                                    this.DataFromNetsuiteLog.Debug("POUpdate: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    poCount++;
                                }
                            } //end of por

                            if (status == true)
                            {
                                if (rowCount <= 0)
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO PENDING RECEIPT', rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where (rn_sche_transactionType = 'SSA-PURCHASE ORDER' or rn_sche_transactionType = 'SSA-PURCHASE ORDER PM') " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("POUpdate: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }

                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res = service.addList(irList);
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(irList);
                                    String jobID = job.jobId;

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("POUpdate: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where (rn_sche_transactionType = 'SSA-PURCHASE ORDER' or rn_sche_transactionType = 'SSA-PURCHASE ORDER PM') " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("POUpdate: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }

                                scope1.Complete();
                            }
                            else
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA', rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where (rn_sche_transactionType = 'SSA-PURCHASE ORDER' or rn_sche_transactionType = 'SSA-PURCHASE ORDER PM') " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("POUpdate: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("POUpdate Exception: (" + rangeFrom.ToString() + "," + rangeFrom.ToString() + ")" + ex.ToString());
                            //status = false;
                            //if (rowCount == 0)
                            //{
                            //    rowCount++;
                            //}
                            //break;
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("POUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            /*if (exPOList.Count() > 0)
            {
                PurchaseOrder(exPOList, rangeFrom, rangeTo);
            }*/
            return status;
        }
        public Boolean PurchaseOrder(DateTime rangeFrom, DateTime rangeTo)
        {
            Boolean status = false;
            this.DataFromNetsuiteLog.Info("Excess Purchase Order ***************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("Excess Purchase Order: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("Excess Purchase Order: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("Excess Purchase Order: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 poCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        //var exPoGroup = exPOList.Select(x => x.prID).Distinct().ToList();
                        //var exPORGroup = exPOList.Select(x => x.poreceiveID).Distinct().ToList();

                        var exPoGroup = (from e in entities.excesspoes
                                         where e.ep_createdDate > rangeFrom && e.ep_createdDate <= rangeTo
                                         select new { e.ep_prID }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("Excess Purchase Order: " + exPoGroup.Count() + " records to update.");
                        PurchaseOrder[] poList = new PurchaseOrder[exPoGroup.Count()];

                        //for (int h = 0; h < exPoGroup.Count(); h++)
                        foreach (var expo in exPoGroup)
                        {
                            String prID = expo.ep_prID;//exPoGroup[h];

                            var excessPOR = (from pr in entities.netsuite_pr
                                             where pr.nspr_pr_ID == prID
                                             select pr).ToList();

                            foreach (var p in excessPOR)
                            {
                                try
                                {
                                    PurchaseOrder po = new PurchaseOrder();

                                    //Form 
                                    RecordRef refForm = new RecordRef();
                                    refForm.internalId = @Resource.PO_2014_inv_2_internal_id;
                                    po.customForm = refForm;

                                    RecordRef refEntity = new RecordRef();
                                    refEntity.internalId = p.nspr_pr_supplier_internalID;
                                    po.entity = refEntity;

                                    RecordRef refEmployee = new RecordRef();
                                    refEmployee.internalId = @Resource.PURCHASEORDER_EMPLOYEE_INTERNALID;
                                    po.employee = refEmployee;

                                    RecordRef refSubsidiary = new RecordRef();
                                    refSubsidiary.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;
                                    po.subsidiary = refSubsidiary;

                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = p.nspr_pr_internalID;
                                    po.createdFrom = refCreatedFrom;

                                    po.memo = p.nspr_pr_desc;

                                    po.message = "Excess PO";

                                    po.tranDate = DateTime.Now;
                                    po.tranDateSpecified = true;

                                    po.orderStatus = PurchaseOrderOrderStatus._pendingReceipt;
                                    po.orderStatusSpecified = true;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = p.nspr_businessChannel_internalID;
                                    po.@class = refClass;

                                    RecordRef refLocation = new RecordRef();
                                    refLocation.internalId = p.nspr_pr_location_internalID;
                                    po.location = refLocation;

                                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                    scfr.value = "2";
                                    cfrList[0] = scfr;

                                    po.customFieldList = cfrList;

                                    var itemList = (from e in entities.excesspoes
                                                    where e.ep_prID == prID
                                                    && e.ep_createdDate > rangeFrom && e.ep_createdDate <= rangeTo
                                                    select e).ToList();
                                    PurchaseOrderItemList poil = new PurchaseOrderItemList();



                                    if (itemList.Count() > 0)
                                    {
                                        PurchaseOrderItem[] poii = new PurchaseOrderItem[itemList.Count()];

                                        Int32 line = 0;
                                        Int32 itemCount = 0;
                                        //for (int j = 0; j < itemList.Count(); j++)
                                        foreach (var j in itemList)
                                        {
                                            //int itemCount = 0;
                                            PurchaseOrderItem poi = new PurchaseOrderItem();

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = j.ep_itemInternalID;//itemList[j].itemInternalID;//item.poi_item_internalID;
                                            poi.item = refItem;

                                            poi.line = line + 1;
                                            poi.lineSpecified = true;

                                            poi.quantity = Convert.ToDouble(j.ep_excessQty);// Convert.ToDouble(item.poi_excessQty);
                                            poi.quantitySpecified = true;

                                            //poi.taxRate1 = 0;
                                            //poi.taxRate1Specified = true;

                                            //poi.taxRate2 = 0;
                                            //poi.taxRate2Specified = true;

                                            //poi.tax1Amt = 0;
                                            //poi.tax1AmtSpecified = true;

                                            poi.grossAmt = 0;
                                            poi.grossAmtSpecified = true;

                                            poi.amount = 0;
                                            poi.amountSpecified = true;

                                            RecordRef refClass2 = new RecordRef();
                                            refClass2.internalId = p.nspr_businessChannel_internalID;
                                            poi.@class = refClass2;

                                            // ANET-32 Intergartion to create PO to include territory rights
                                            var salesTerritoryInternalID = (from poItem in entities.netsuite_pritem
                                                                            join pr in entities.netsuite_pr on poItem.nspi_nspr_pr_ID equals pr.nspr_pr_ID
                                                                            where pr.nspr_pr_ID == prID
                                                                            select new
                                                                            {
                                                                                poItem.nspi_item_internalID,
                                                                                poItem.nspi_nspr_pr_ID,
                                                                                poItem.nspi_salesTerritorySCH
                                                                            }).ToList();


                                            CustomFieldRef[] cfrPOIList = new CustomFieldRef[1];
                                            StringCustomFieldRef scfrPOI = new StringCustomFieldRef();
                                            scfrPOI.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                            scfrPOI.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                            foreach (var i in salesTerritoryInternalID)
                                            {
                                                if (i.nspi_item_internalID == j.ep_itemInternalID)
                                                {
                                                    scfrPOI.value = i.nspi_salesTerritorySCH;
                                                }
                                            }

                                            cfrPOIList[0] = scfrPOI;
                                            poi.customFieldList = cfrPOIList;

                                            poii[line] = poi;
                                            // ANET-32 Intergartion to create PO to include territory rights
                                            itemCount++;
                                            line++;
                                        }
                                        poil.item = poii;
                                        po.itemList = poil;
                                        poList[poCount] = po;

                                        rowCount = poCount + 1;

                                        String refNo = "EXCESSPO.EP_PRID." + prID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-EXCESS PURCHASE ORDER', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("Excess Purchase Order:  " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);


                                        status = true;
                                        poCount++;


                                    }



                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("Excess Purchase Order Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }
                        }

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(poList);

                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("Excess Purchase Order:  " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-EXCESS PURCHASE ORDER' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("Excess Purchase Order:  " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-EXCESS PURCHASE ORDER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("Excess Purchase Order:  " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Excess Purchase Order:  Login Netsuite failed.");
                }
            }//end of scope1

            //TBA
            //logout();
            return status;
        }
        public Boolean ReturnAuthorizationReceiveUpdate(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("ReturnAuthorizationReceiveUpdate ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {

                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Fatal("ReturnAuthorizationReceiveUpdate: Login Netsuite Success");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("ReturnAuthorizationReceiveUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 raCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var returnAuthorization = (from ra in entities.wms_rareceive
                                                   where ra.nsr_rr_status == 1
                                                   && (ra.nsr_rangeTo > rangeFrom && ra.nsr_rangeTo <= rangeTo)
                                                   && ra.nsr_netsuiteRA == null
                                                   select ra).ToList();

                        this.DataFromNetsuiteLog.Info("ReturnAuthorizationReceiveUpdate: " + returnAuthorization.Count() + " records to update.");
                        //status = true;
                        ItemReceipt[] irList = new ItemReceipt[returnAuthorization.Count()];

                        foreach (var r in returnAuthorization)
                        {
                            try
                            {
                                InitializeRef refRA = new InitializeRef();
                                refRA.type = InitializeRefType.returnAuthorization;
                                refRA.internalId = r.nsr_rr_internalID;
                                refRA.typeSpecified = true;

                                InitializeRecord recRA = new InitializeRecord();
                                recRA.type = InitializeType.itemReceipt;
                                recRA.reference = refRA;

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                ReadResponse rrRA = netsuiteService.initialize(recRA);
                                Record rRA = rrRA.record;

                                ItemReceipt ir1 = (ItemReceipt)rRA;
                                ItemReceipt ir2 = new ItemReceipt();

                                if (ir1 != null)
                                {
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = ir1.createdFrom.internalId;
                                    ir2.createdFrom = refCreatedFrom;

                                    ir2.tranDate = DateTime.Now;
                                    ir2.tranDateSpecified = true;

                                    ItemReceiptItemList iril1 = new ItemReceiptItemList();
                                    iril1 = ir1.itemList;

                                    var returnAuthorizationItem = (from rai in entities.wms_rareceiveitem
                                                                   where rai.nsri_nsr_rr_ID == r.nsr_rr_ID
                                                                   select rai).ToList();

                                    if (returnAuthorizationItem.Count() > 0)
                                    {
                                        Int32[] receiveQty = new Int32[returnAuthorizationItem.Count()];
                                        String[] itemID = new String[returnAuthorizationItem.Count()];
                                        for (int i = 0; i < iril1.item.Count(); i++)
                                        {
                                            int itemIndex = returnAuthorizationItem.FindIndex(s => s.nsri_item_internalID == iril1.item[i].item.internalId);

                                            if (itemIndex >= 0)
                                            {
                                                iril1.item[i].quantity = Convert.ToInt32(returnAuthorizationItem[itemIndex].nsri_rritem_receive_qty);
                                                iril1.item[i].quantitySpecified = true;

                                                iril1.item[i].itemReceive = true;
                                                iril1.item[i].itemReceiveSpecified = true;

                                                //cpng start
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = Convert.ToInt32(returnAuthorizationItem[itemIndex].nsri_rritem_receive_qty);
                                                IA.quantitySpecified = true;
                                                IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN };
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iril1.item[i].inventoryDetail = ID;
                                                //cpng end
                                            }
                                            else
                                            {
                                                iril1.item[i].quantity = 0;
                                                iril1.item[i].itemReceive = false;
                                                iril1.item[i].itemReceiveSpecified = true;
                                            }
                                        }
                                        ir2.itemList = iril1;
                                        irList[raCount] = ir2;

                                        rowCount = raCount + 1;

                                        String refNo = "NETSUITE_RETURN.NSR_RR_ID." + r.nsr_rr_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-RETURN AUTHORIZATION', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + r.nsr_rr_internalID + "')";
                                        this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateTask = "update wms_rareceive set nsr_netsuiteRA = '" + gjob_id.ToString() + "' where nsr_netsuiteRA is null " +
                                                         "and nsr_rr_internalID = '" + r.nsr_rr_internalID + "' " +
                                                         "and nsr_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                         "and nsr_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                        this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        raCount++;
                                        status = true;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("ReturnAuthorizationReceiveUpdate Exception: (" + r.nsr_rr_internalID + "," + r.nsr_rr_number + ")" + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of returnauthorization

                        if (status == true)
                        {
                            if (rowCount > 0)
                            //if (irList.Count() > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(irList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateOrdScan = "update wms_rareceive set nsr_netsuiteRA = '" + jobID + "' where nsr_netsuiteRA = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + updateOrdScan);
                                    entities.Database.ExecuteSqlCommand(updateOrdScan);

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-RETURN AUTHORIZATION' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-RETURN AUTHORIZATION' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("ReturnAuthorizationReceiveUpdate: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("ReturnAuthorizationReceiveUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }
        public Boolean InventoryTransferUpdate(DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("InventoryTransferUpdate ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA

                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;

                try
                {
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("InventoryTransferUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }



                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 dtCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var directTransfer = (from dt in entities.wms_directtransfer
                                              where dt.dt_rangeTo == rangeTo
                                              select dt).ToList();

                        this.DataFromNetsuiteLog.Info("InventoryTransferUpdate: " + directTransfer.Count() + " records to update.");
                        //status = true;
                        InventoryTransfer[] invTfrList = new InventoryTransfer[directTransfer.Count()];

                        foreach (var d in directTransfer)
                        {
                            try
                            {
                                Int32 itemCount = 0;
                                InventoryTransfer invTfr = new InventoryTransfer();
                                InventoryTransferInventoryList itil = new InventoryTransferInventoryList();

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = "1";

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = d.dt_businessChannel;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = d.dt_locationFrom;

                                RecordRef refTrasLocation = new RecordRef();
                                refTrasLocation.internalId = d.dt_locationTo;

                                invTfr.tranDate = DateTime.Now;
                                invTfr.subsidiary = refSubsidiary;
                                invTfr.@class = refBusinessChannel;
                                invTfr.location = refLocation;
                                invTfr.transferLocation = refTrasLocation;

                                var directTransferItem = (from dti in entities.wms_directtransferitem
                                                          where dti.dti_dt_directTransfer_ID == d.dt_directTransfer_ID
                                                          select dti).ToList();

                                if (directTransferItem.Count() > 0)
                                {
                                    InventoryTransferInventory[] items = new InventoryTransferInventory[directTransferItem.Count()];

                                    foreach (var i in directTransferItem)
                                    {
                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = i.dti_productCode;

                                        InventoryTransferInventory item = new InventoryTransferInventory();

                                        item.item = refItem;
                                        item.adjustQtyBy = Convert.ToDouble(i.dti_quantity);
                                        item.adjustQtyBySpecified = true;
                                        items[itemCount] = item;
                                        itemCount++;
                                    }
                                    itil.inventory = items;
                                    invTfr.inventoryList = itil;
                                    invTfrList[dtCount] = invTfr;

                                    rowCount = dtCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-INVENTORY TRANSFER', 'WMSDIRECTTRANSFER.DT_DIRECTTRANSFER_ID." + d.dt_directTransfer_ID + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    dtCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("InventoryTransferUpdate Exception: " + ex.ToString());
                                status = false;
                            }
                        }//end of directtransfer

                        if (status == true)
                        {
                            if (rowCount > 0)
                            //if (invTfrList.Count() > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(invTfrList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-INVENTORY TRANSFER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-INVENTORY TRANSFER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("InventoryTransferUpdate: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("InventoryTransferUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }
        public Boolean CashSalesUpdate(DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CashSalesUpdate ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {

                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("CashSalesUpdate: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("CashSalesUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //  Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CashSalesUpdate: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 csCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var cashsales = (from cs in entities.wms_cashsale
                                         where cs.cs_rangeTo == rangeTo
                                         select cs).ToList();

                        this.DataFromNetsuiteLog.Info("CashSalesUpdate: " + cashsales.Count() + " records to update.");
                        status = true;
                        CashSale[] csList = new CashSale[cashsales.Count()];

                        foreach (var c in cashsales)
                        {
                            try
                            {
                                Int32 itemCount = 0;
                                CashSale cs = new CashSale();
                                CashSaleItemList csil = new CashSaleItemList();

                                RecordRef refEntity = new RecordRef();
                                refEntity.internalId = c.cs_entity_internalID;

                                RecordRef refSub = new RecordRef();
                                refSub.internalId = c.cs_subsidiary_internalID;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = c.cs_businessChannel_internalID;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = c.cs_location_internalID;

                                cs.entity = refEntity;
                                cs.tranDate = Convert.ToDateTime(c.cs_tranDate);
                                cs.subsidiary = refSub;
                                cs.@class = refClass;
                                cs.location = refLocation;

                                var cashSaleItem = (from csi in entities.wms_cashsaleitem
                                                    where csi.csi_cs_cashsaleID == c.cs_cashsaleID
                                                    select csi).ToList();

                                if (cashSaleItem.Count() > 0)
                                {
                                    CashSaleItem[] items = new CashSaleItem[cashSaleItem.Count()];

                                    foreach (var i in cashSaleItem)
                                    {
                                        CashSaleItem item = new CashSaleItem();

                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = i.csi_item_internalID;
                                        item.item = refItem;
                                        item.quantity = Convert.ToDouble(i.csi_qty);
                                        item.quantitySpecified = true;

                                        items[itemCount] = item;
                                        itemCount++;
                                    }
                                    csil.item = items;
                                    cs.itemList = csil;
                                    csList[csCount] = cs;

                                    rowCount = csCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-CASH SALE', 'WMSCASHSALE.CS_CASHSALE_ID." + c.cs_cashsaleID + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CashSalesUpdate: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    csCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CashSalesUpdate Exception: (" + c.cs_entity_internalID + "," + c.cs_cashsaleID + ")" + ex.ToString());
                                status = false;
                            }
                        }//end of cashsales

                        if (status == true)
                        {
                            if (csList[0] != null && csList.Count() > 0)
                            //if (csList.Count() > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(csList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CashSalesUpdate: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-CASH SALE' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CashSalesUpdate: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            else
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-CASH SALE' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CashSalesUpdate: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CashSalesUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }

        public void UnfulfillBinTransfer(Hashtable htWMSItemsQTY, string moNo)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            /*cpng start*/
            if (htWMSItemsQTY.Count > 0)
            {
                BinTransfer BT = new BinTransfer();
                BinTransferInventory[] BTIA = new BinTransferInventory[htWMSItemsQTY.Count];
                BinTransferInventoryList BTIL = new BinTransferInventoryList();
                int binItemCount = 0;
                //Assign Inventory Details 
                foreach (DictionaryEntry entry in htWMSItemsQTY)
                {
                    if (Convert.ToInt32(entry.Value) > 0)
                    {
                        BinTransferInventory BTI = new BinTransferInventory();

                        //Declaration For inventory details
                        InventoryAssignment[] IAA = new InventoryAssignment[1];
                        InventoryAssignment IA = new InventoryAssignment();
                        InventoryAssignmentList IAL = new InventoryAssignmentList();
                        InventoryDetail ID = new InventoryDetail();

                        IA.quantity = Convert.ToInt32(entry.Value);
                        IA.quantitySpecified = true;
                        IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                        IA.toBinNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_UNFULFILL };
                        IAA[0] = IA;
                        IAL.inventoryAssignment = IAA;
                        ID.inventoryAssignmentList = IAL;

                        //Assign Bin Transfer
                        BTI.quantity = Convert.ToInt32(entry.Value);
                        BTI.quantitySpecified = true;
                        BTI.item = new RecordRef { internalId = entry.Key.ToString() };
                        BTI.inventoryDetail = ID;
                        BTIA[binItemCount] = BTI;
                        binItemCount++;
                    }
                }
                BTIL.replaceAll = false;
                BTIL.inventory = BTIA;
                BT.inventoryList = BTIL;
                BT.location = new RecordRef { internalId = @Resource.TRADE_DEFAULT_LOCATION };// cpng temp
                BT.memo = "WIP " + moNo;
                if (binItemCount > 0)
                {

                    string loginEmail = "";
                    loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                    tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                    tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                    ItemSearchBasic basic = new ItemSearchBasic()
                    {
                        internalId = new SearchMultiSelectField()
                        {
                            @operator = SearchMultiSelectFieldOperator.anyOf,
                            operatorSpecified = true,
                            searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                        }
                    };
                    Boolean loginStatus = false;

                    netsuiteService.Timeout = 820000000;
                    netsuiteService.CookieContainer = new CookieContainer();
                    ApplicationInfo appinfo = new ApplicationInfo();
                    //  appinfo.applicationId = appID;
                    netsuiteService.applicationInfo = appinfo;
                    try
                    {
                        Console.WriteLine("Success");
                        netsuiteService.tokenPassport = createTokenPassport();
                        SearchResult status = netsuiteService.search(basic);
                        if (status.status.isSuccess == true)
                        {
                            this.DataFromNetsuiteLog.Debug("Trade Bin Transfer Unfulfill: Login Netsuite success.");
                            loginStatus = true;
                        }
                        else
                        {
                            loginStatus = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        loginStatus = false;
                        this.DataFromNetsuiteLog.Fatal("Trade Bin Transfer Unfulfill: Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    if (loginStatus == true)
                    {
                        //TBA
                        netsuiteService.tokenPassport = createTokenPassport();
                        var btResponse = netsuiteService.add(BT);// perform bin transfer for reverse
                        if (!btResponse.status.isSuccess)
                        {
                            this.DataFromNetsuiteLog.Fatal("Trade Bin Transfer Unfulfill Exception: " + moNo + ": " + btResponse.status.statusDetail[0].message.ToString() + " " + DateTime.Now.ToString());
                        }
                    }

                }

            }
            /*cpng end*/
        }
        /*
        public List<ExcessFulfillment> SOFulfillmentUpdate(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate *****************");
            Boolean status1 = false;
            String refNo = null;
            List<ExcessFulfillment> exFulList = new List<ExcessFulfillment>();
            List<String> updateSOList = new List<String>();

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();

                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("Login Netsuite");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var query1 = (from q1 in entities.requestmqs
                                      join q2 in entities.requestnetsuites on q1.rmq_rangeTo equals q2.rn_rangeTo
                                      where q1.rmq_jobID != null
                                      select q1.rmq_status).FirstOrDefault();

                        if (query1.Equals("UPLOADED"))
                        {
                            var ordMaster = (from jom in entities.netsuite_jobordmaster
                                             join jomp in entities.netsuite_jobordmaster_pack on jom.nsjom_jobOrdMaster_ID equals jomp.nsjomp_jobOrdMaster_ID
                                             join josp in entities.wms_jobordscan_pack on jomp.nsjomp_jobOrdMaster_pack_ID equals josp.josp_pack_ID
                                             where josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                             select new { jom.nsjom_moNo_internalID, jom.nsjom_moNo, jom.nsjom_jobOrdMaster_ID }).Distinct().ToList();

                            this.DataFromNetsuiteLog.Info(ordMaster.Count() + " records to update.");

                            ItemFulfillment[] iffList = new ItemFulfillment[ordMaster.Count()];

                            foreach (var order in ordMaster)
                            {
                                try
                                {
                                    updateSOList.Add(order.nsjom_moNo_internalID);

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
                                                    //where josp.josp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                    where josp.josp_pack_ID == jomp.nsjomp_jobOrdMaster_pack_ID && jomp.nsjomp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                    select new { jomp.nsjomp_ordPack, jomp.nsjomp_item_internalID, josp.josp_pack_ID, josp.josp_ordFulFill, jomp.nsjomp_location_internalID }).ToList();

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

                                                    iffi.item = ifitemlist.item[i].item;

                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = item.nsjomp_location_internalID;
                                                    iffi.location = refLocation;

                                                    iffi.quantity = Convert.ToInt32(item.josp_ordFulFill);
                                                    iffi.quantitySpecified = true;

                                                    iffi.itemIsFulfilled = true;
                                                    iffi.itemIsFulfilledSpecified = true;

                                                    if (ifitemlist.item[i].quantityRemaining > item.josp_ordFulFill)
                                                    {
                                                        ExcessFulfillment exFul = new ExcessFulfillment();
                                                        exFul.moNo = order.nsjom_moNo;
                                                        exFul.josp_packID = item.josp_pack_ID;
                                                        exFul.jobOrdMasterID = order.nsjom_jobOrdMaster_ID;
                                                        exFul.itemInternalID = ifitemlist.item[i].item.internalId;
                                                        exFul.locationInternalID = item.nsjomp_location_internalID;
                                                        exFul.excessQty = ifitemlist.item[i].quantityRemaining - item.josp_ordFulFill;
                                                        exFul.refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                                        exFulList.Add(exFul);
                                                    }
                                                    break;
                                                }
                                            }
                                            ifitems[i] = iffi;
                                        }
                                        ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                        ifil1.item = ifitems;
                                        iff2.itemList = ifil1;

                                        iffList[ordCount] = iff2;

                                        rowCount = ordCount + 1;
                                        refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-FULFILLMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + order.nsjom_moNo_internalID + "')";
                                        this.DataFromNetsuiteLog.Debug(insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        ordCount++;
                                        status1 = true;

                                        this.DataFromNetsuiteLog.Debug("Sales order internalID_moNo: " + order.nsjom_moNo_internalID + "_" + order.nsjom_moNo);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error(ex.ToString());
                                    status1 = false;
                                }
                            }//end of ordMaster

                            if (status1 == true)
                            {
                                job = service.asyncAddList(iffList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug(updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                scope1.Complete();


                            }
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return exFulList;
        }
        */
        /*
        public Boolean InvoiceUpdate(DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("InvoiceUpdate ***************");
            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("Login Netsuite");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var invoice = (from inv in entities.netsuite_invoice
                                       where inv.nsinv_rangeTo == rangeTo
                                       select inv).ToList();

                        this.DataFromNetsuiteLog.Info(invoice.Count() + " records to update.");

                        Invoice[] invList = new Invoice[invoice.Count()];

                        foreach (var i in invoice)
                        {
                            try
                            {
                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.salesOrder;
                                refSO.internalId = i.nsinv_moNo_internalID;
                                refSO.typeSpecified = true;

                                InitializeRecord recSO = new InitializeRecord();
                                recSO.type = InitializeType.invoice;
                                recSO.reference = refSO;

                                ReadResponse rrSO = service.initialize(recSO);
                                Record rSO = rrSO.record;

                                Invoice inv1 = (Invoice)rSO;
                                if (inv1 != null)
                                {
                                    //Invoice inv2 = new Invoice();
                                    inv1.subTotalSpecified = false;
                                    inv1.discountTotalSpecified = false;
                                    inv1.shippingTax1RateSpecified = false;
                                    inv1.totalSpecified = false;
                                    inv1.taxRateSpecified = false;
                                    inv1.timeTaxRate1Specified = false;

                                    for (int j = 0; j < inv1.itemList.item.Count(); j++)
                                    {
                                        inv1.itemList.item[j].quantityRemainingSpecified = false;
                                        inv1.itemList.item[j].quantityOrderedSpecified = false;
                                        inv1.itemList.item[j].quantityAvailableSpecified = false;
                                        inv1.itemList.item[j].quantityFulfilledSpecified = false;
                                        inv1.itemList.item[j].quantityOnHandSpecified = false;
                                    }

                                    
                                    //RecordRef refEntity = new RecordRef();
                                    //refEntity.internalId = i.nsinv_entity_internalID;
                                    //inv2.entity = refEntity;
                                
                                    //RecordRef refCreatedFrom = new RecordRef();
                                    //refCreatedFrom.internalId = i.nsinv_moNo_internalID;
                                    //inv2.createdFrom = refCreatedFrom;
                                
                                    //RecordRef refLocation = new RecordRef();
                                    //refLocation.internalId = i.nsinv_location_internalID;
                                    //inv2.location = refLocation;

                                    //InvoiceItemList iil = new InvoiceItemList();
                                    //iil = inv1.itemList;
                                
                                    //inv2.itemList = iil;
                                    invList[invCount] = inv1;

                                    //////
                                    rowCount = invCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (" +
                                    "rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                    "rnt_seqNO) values (" +
                                    "'add', 'invoice update', '" + i.nsinv_moNo_internalID.ToString() +
                                    "', '" + gjob_id.ToString() + "', 'START', '" + convertDateToString(DateTime.Now) + "', '" +
                                    rowCount + "'" +
                                    ")";
                                    this.DataFromNetsuiteLog.Debug(insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);
                                    //////

                                    invCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error(ex.ToString());
                                status = false;
                            }
                        }

                        if (status == true)
                        {
                            job = service.asyncAddList(invList);
                            String jobID = job.jobId;

                            var updateTask = "update requestnetsuite_task set " +
                                "rnt_jobID = '" + jobID + "' where " +
                                "rnt_jobID = '" + gjob_id.ToString() + "'";
                            this.DataFromNetsuiteLog.Debug(updateTask);
                            entities.Database.ExecuteSqlCommand(updateTask);

                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-INVOICE' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        else
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_status='UPLOADED', rn_jobID='0000', " +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'SSA-INVOICE' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Login Netsuite failed.");
                }
            }
            logout();
            return status;
        }*/
        /*
        public List<ExcessFulfillment> SOFulfillmentUpdate(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("SOFulfillmentUpdate *****************");
            Boolean status1 = false;
            String refNo = null;
            List<ExcessFulfillment> exFulList = new List<ExcessFulfillment>();
            List<String> updateSOList = new List<String>();

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();

                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("Login Netsuite");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var query1 = (from q1 in entities.requestmqs
                                      join q2 in entities.requestnetsuites on q1.rmq_rangeTo equals q2.rn_rangeTo
                                      where q1.rmq_jobID != null
                                      select q1.rmq_status).FirstOrDefault();

                        if (query1.Equals("UPLOADED"))
                        {
                            var ordMaster = (from jom in entities.netsuite_jobordmaster
                                             join jomp in entities.netsuite_jobordmaster_pack on jom.nsjom_jobOrdMaster_ID equals jomp.nsjomp_jobOrdMaster_ID
                                             join josp in entities.wms_jobordscan_pack on jomp.nsjomp_jobOrdMaster_pack_ID equals josp.josp_pack_ID
                                             where josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                             select new { jom.nsjom_moNo_internalID, jom.nsjom_moNo, jom.nsjom_jobOrdMaster_ID }).Distinct().ToList();

                            this.DataFromNetsuiteLog.Info(ordMaster.Count() + " records to update.");

                            ItemFulfillment[] iffList = new ItemFulfillment[ordMaster.Count()];

                            foreach (var order in ordMaster)
                            {
                                try
                                {
                                    updateSOList.Add(order.nsjom_moNo_internalID);

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
                                                    //where josp.josp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                    where josp.josp_pack_ID == jomp.nsjomp_jobOrdMaster_pack_ID && jomp.nsjomp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                                    select new { jomp.nsjomp_ordPack, jomp.nsjomp_item_internalID, josp.josp_pack_ID, josp.josp_ordFulFill, jomp.nsjomp_location_internalID }).ToList();

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

                                                    iffi.item = ifitemlist.item[i].item;

                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = item.nsjomp_location_internalID;
                                                    iffi.location = refLocation;

                                                    iffi.quantity = Convert.ToInt32(item.josp_ordFulFill);
                                                    iffi.quantitySpecified = true;

                                                    iffi.itemIsFulfilled = true;
                                                    iffi.itemIsFulfilledSpecified = true;

                                                    if (ifitemlist.item[i].quantityRemaining > item.josp_ordFulFill)
                                                    {
                                                        ExcessFulfillment exFul = new ExcessFulfillment();
                                                        exFul.moNo = order.nsjom_moNo;
                                                        exFul.josp_packID = item.josp_pack_ID;
                                                        exFul.jobOrdMasterID = order.nsjom_jobOrdMaster_ID;
                                                        exFul.itemInternalID = ifitemlist.item[i].item.internalId;
                                                        exFul.locationInternalID = item.nsjomp_location_internalID;
                                                        exFul.excessQty = ifitemlist.item[i].quantityRemaining - item.josp_ordFulFill;
                                                        exFul.refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                                        exFulList.Add(exFul);
                                                    }
                                                    break;
                                                }
                                            }
                                            ifitems[i] = iffi;
                                        }
                                        ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                        ifil1.item = ifitems;
                                        iff2.itemList = ifil1;

                                        iffList[ordCount] = iff2;

                                        rowCount = ordCount + 1;
                                        refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-FULFILLMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','" + order.nsjom_moNo_internalID + "')";
                                        this.DataFromNetsuiteLog.Debug(insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        ordCount++;
                                        status1 = true;

                                        this.DataFromNetsuiteLog.Debug("Sales order internalID_moNo: " + order.nsjom_moNo_internalID + "_" + order.nsjom_moNo);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error(ex.ToString());
                                    status1 = false;
                                }
                            }//end of ordMaster

                            if (status1 == true)
                            {
                                job = service.asyncAddList(iffList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug(updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                scope1.Complete();


                            }
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return exFulList;
        }
        */
        /*
        public Boolean InventoryAdjustmentUpdate(DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("InventoryAdjustmentUpdate ***************");

            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 daCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var directAdj = (from da in entities.wms_directadjustment
                                         //where da.da_rangeTo == rangeTo
                                         select da).ToList();

                        this.DataFromNetsuiteLog.Info(directAdj.Count() + " records to update.");

                        InventoryAdjustment[] invAdjList = new InventoryAdjustment[directAdj.Count()];

                        foreach (var d in directAdj)
                        {
                            try
                            {
                                Int32 itemCount = 0;
                                InventoryAdjustment invAdj = new InventoryAdjustment();
                                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                                RecordRef refAccount = new RecordRef();
                                refAccount.internalId = d.da_account_internalID;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = d.da_subsidiary_internalID;

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = d.da_businessChannel_internalID;

                                invAdj.account = refAccount;
                                invAdj.tranDate = Convert.ToDateTime(d.da_tranDate);
                                invAdj.subsidiary = refSubsidiary;
                                invAdj.@class = refBusinessChannel;

                                var directAdjItem = (from dai in entities.wms_directadjustmentitem
                                                     where dai.dai_da_directAdjID == d.da_directAdjID
                                                     select dai).ToList();

                                if (directAdjItem.Count() > 0)
                                {
                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[directAdjItem.Count()];

                                    foreach (var i in directAdjItem)
                                    {
                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = i.dai_item_internalID;

                                        RecordRef refLocation = new RecordRef();
                                        refLocation.internalId = i.dai_location_internalID;

                                        InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                                        item.item = refItem;
                                        item.location = refLocation;
                                        item.adjustQtyBy = Convert.ToDouble(i.dai_qty);
                                        item.adjustQtyBySpecified = true;
                                        items[itemCount] = item;
                                        itemCount++;
                                    }
                                    iail.inventory = items;
                                    invAdj.inventoryList = iail;
                                    invAdjList[daCount] = invAdj;

                                    rowCount = daCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-INVENTORY ADJUSTMENT', 'NETSUITEDIRECTADJUSTMENT.NSDA_ADJUSTMENT_ID." + d.da_directAdjID + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug(insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    daCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error(ex.ToString());
                                status = false;
                            }
                        }//end of directAdj

                        if (status == true)
                        {
                            job = service.asyncAddList(invAdjList);
                            String jobID = job.jobId;

                            var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                            this.DataFromNetsuiteLog.Debug(updateTask);
                            entities.Database.ExecuteSqlCommand(updateTask);

                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' where rn_sche_transactionType = 'SSA-INVENTORY ADJUSTMENT' " +
                                "and rn_rangeTo = '" + rangeTo.ToString("yyyy-MM-dd HH:mm:ss") + "'";
                            this.DataFromNetsuiteLog.Debug(updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            scope1.Complete();
                        }

                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
         * */

        public Boolean TradeUpdateNetsuite(String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("TradeUpdateNetsuite:  ***************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);


            String[] array_tranType = transactionType.Split('.');
            String searchTran = array_tranType[1].ToString();

            Boolean status = false;
            DateTime createdDate = DateTime.Now;
            String jobID = "";
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("TradeUpdateNetsuite: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("TradeUpdateNetsuite: Login Netsuite failed. Exception : " + ex.ToString());

                }

                // Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("TradeUpdateNetsuite: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        var updateList = (from sync_to_wms in entities.requestnetsuite_task
                                          where sync_to_wms.rnt_updatedDate > rangeFrom
                                          && sync_to_wms.rnt_updatedDate <= rangeTo
                                          && sync_to_wms.rnt_description == searchTran
                                          && sync_to_wms.rnt_status == "TRUE"
                                          select new { sync_to_wms.rnt_description, sync_to_wms.rnt_nsInternalId, sync_to_wms.rnt_createdFromInternalId })
                                   .OrderBy(x => x.rnt_description)
                                   .ToList();

                        this.DataFromNetsuiteLog.Info("TradeUpdateNetsuite: " + updateList.Count() + " records to update.");
                        if (updateList.Count() > 0)
                        {
                            Int32 rec = 0;
                            String[] array_netsuiteID = new String[updateList.Count()];
                            try
                            {
                                switch (transactionType)
                                {
                                    case "UPD-STATUS.NS-CASH SALES":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            rec++;
                                        }
                                        jobID = SyncToWms_Cs_Update(transactionType, entities, array_netsuiteID, "3", rangeTo);
                                        break;
                                    case "UPD-STATUS.NS-PURCHASE REQUEST":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            rec++;
                                        }
                                        //jobID = SyncToWms_Po_Update(transactionType, entities, array_netsuiteID, "3", rangeTo);
                                        jobID = SyncHistory_Po_Add(transactionType, entities, array_netsuiteID, "3", rangeTo); //Sync history
                                        break;
                                    case "UPD-STATUS.NS-SALES ORDER":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            rec++;
                                        }
                                        //jobID = SyncToWms_So_Update(transactionType, entities, array_netsuiteID, "3", rangeTo);
                                        jobID = SyncHistory_So_Add(transactionType, entities, array_netsuiteID, "3", rangeTo);//sync history
                                        break;
                                    case "UPD-STATUS.NS-RETURN AUTHORIZATION (RECEIVE)":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            rec++;
                                        }
                                        jobID = SyncToWms_Ra_Update(transactionType, entities, array_netsuiteID, "3", rangeTo);
                                        break;

                                    case "UPD-STATUS.SSA-EXCESS PURCHASE ORDER":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            rec++;
                                        }
                                        jobID = ExcessPOUpdate(transactionType, entities, array_netsuiteID, "2", rangeTo);
                                        break;
                                    case "UPD-STATUS.SSA-FULFILLMENT":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_createdFromInternalId;
                                            rec++;
                                        }
                                        //jobID = SyncToWms_So_Update(transactionType, entities, array_netsuiteID, "2", rangeTo);
                                        jobID = SyncHistory_So_Add(transactionType, entities, array_netsuiteID, "2", rangeTo);//sync history   
                                        break;
                                    case "UPD-STATUS.SSA-PURCHASE ORDER":
                                        foreach (var p in updateList)
                                        {
                                            //array_netsuiteID[rec] = p.rnt_nsInternalId;
                                            array_netsuiteID[rec] = p.rnt_createdFromInternalId;
                                            rec++;
                                        }
                                        //jobID = SyncToWms_Po_Update(transactionType, entities, array_netsuiteID, "2", rangeTo);
                                        jobID = SyncHistory_Po_Add(transactionType, entities, array_netsuiteID, "2", rangeTo);//sync history
                                        break;
                                    /*case "UPD-STATUS.SSA-RETURN AUTHORIZATION":
                                        foreach (var p in updateList)
                                        {
                                            array_netsuiteID[rec] = p.rnt_createdFromInternalId;
                                            rec++;
                                        }
                                        jobID = ReturnAuthorizationRefundUpdate(transactionType, entities, array_netsuiteID, "2", rangeTo);
                                        break;
                                     */
                                }

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("TradeUpdateNetsuite: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("TradeUpdateNetsuite Exception: " + ex.ToString());
                                status = false;
                            }
                        }
                        else
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA', rn_status='NO-DATA', " +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("TradeUpdateNetsuite: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("TradeUpdateNetsuite: Login Netsuite failed.");
                }
            }//end of scope1

            //logout();
            return status;
        }

        private String SyncToWms_Ra_Update(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;
            try
            {
                this.DataFromNetsuiteLog.Debug("SyncToWms_Ra_Update: " + array_netsuiteID.Count() + " to update.");

                ReturnAuthorization[] updateList = new ReturnAuthorization[array_netsuiteID.Count()];

                Int32 rowCount = 0;
                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                    scfr.value = updateValue;
                    cfrList[0] = scfr;

                    ReturnAuthorization tran = new ReturnAuthorization();

                    tran.internalId = array_netsuiteID[i];
                    tran.customFieldList = cfrList;
                    updateList[i] = tran;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                    this.DataFromNetsuiteLog.Debug("SyncToWms_Ra_Update: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }

                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        loginStatus = true;
                        //WriteResponse[] res = service.updateList(updateList);
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncUpdateList(updateList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncToWms_Ra_Update: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SyncToWms_Ra_Update: Login Netsuite failed. Exception : " + ex.ToString());

                }
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncToWms_Ra_Update Exception: " + ex.ToString());
            }
            return jobID;
        }
        private String SyncToWms_Cs_Update(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                this.DataFromNetsuiteLog.Debug("SyncToWms_Cs_Update: " + array_netsuiteID.Count() + " to update.");

                CashSale[] updateList = new CashSale[array_netsuiteID.Count()];

                Int32 rowCount = 0;
                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                    scfr.value = updateValue;
                    cfrList[0] = scfr;

                    CashSale tran = new CashSale();

                    tran.internalId = array_netsuiteID[i];
                    tran.customFieldList = cfrList;
                    updateList[i] = tran;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";

                    this.DataFromNetsuiteLog.Debug("SyncToWms_Cs_Update: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }

                //TBA

                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        loginStatus = true;
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncUpdateList(updateList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncToWms_Cs_Update: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SyncToWms_Cs_Update: Login Netsuite failed. Exception : " + ex.ToString());

                }


                //TBA
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncToWms_Cs_Update Exception: " + ex.ToString());
            }
            return jobID;
        }
        private String SyncToWms_So_Update(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + array_netsuiteID.Count() + " to update.");

                SalesOrder[] updateList = new SalesOrder[array_netsuiteID.Count()];

                Int32 rowCount = 0;
                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                    scfr.value = updateValue;
                    cfrList[0] = scfr;

                    SalesOrder tran = new SalesOrder();

                    tran.internalId = array_netsuiteID[i];
                    tran.customFieldList = cfrList;
                    updateList[i] = tran;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";

                    this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }

                //WriteResponse[] res = service.updateList(updateList);
                //TBA

                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                //Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {

                    Boolean loginStatus = false;

                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        loginStatus = true;
                        this.DataFromNetsuiteLog.Info("SyncToWms_So_Update: Login Netsuite success.");
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncUpdateList(updateList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }



                }
                catch (Exception ex)
                {

                    this.DataFromNetsuiteLog.Fatal("SyncToWms_So_Update: Login Netsuite failed. Exception : " + ex.ToString());

                }

            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncToWms_So_Update Exception: " + ex.ToString());
            }
            return jobID;
        }
        private String SyncToWms_Po_Update(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                this.DataFromNetsuiteLog.Debug("SyncToWms_Po_Update: " + array_netsuiteID.Count() + " to update.");

                PurchaseOrder[] updateList = new PurchaseOrder[array_netsuiteID.Count()];

                Int32 rowCount = 0;
                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                    scfr.value = updateValue;
                    cfrList[0] = scfr;

                    PurchaseOrder tran = new PurchaseOrder();

                    tran.internalId = array_netsuiteID[i];
                    tran.customFieldList = cfrList;
                    updateList[i] = tran;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";

                    this.DataFromNetsuiteLog.Debug("SyncToWms_Po_Update: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        loginStatus = true;
                        //WriteResponse[] res = service.updateList(updateList);
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncUpdateList(updateList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncToWms_Po_Update: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SOFulfillmentUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }

            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncToWms_Po_Update Exception: " + ex.ToString());
            }
            return jobID;
        }
        private String ExcessPOUpdate(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("ExcessPOUpdate *****************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            DateTime createdDate = DateTime.Now;

            AsyncStatusResult job = new AsyncStatusResult();

            Int32 poCount = 0;
            Int32 rowCount = 0;
            Guid gjob_id = Guid.NewGuid();

            this.DataFromNetsuiteLog.Info("ExcessPOUpdate: " + array_netsuiteID.Count() + " records to update.");
            ItemReceipt[] irList = new ItemReceipt[array_netsuiteID.Count()];

            for (int p = 0; p < array_netsuiteID.Count(); p++)
            {
                try
                {
                    //Get PO location
                    TransactionSearchBasic prtsb = new TransactionSearchBasic();

                    SearchEnumMultiSelectField poType = new SearchEnumMultiSelectField();
                    poType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                    poType.operatorSpecified = true;
                    poType.searchValue = new String[] { "_purchaseOrder" };

                    prtsb.type = poType;

                    RecordRef r1 = new RecordRef();
                    r1.internalId = array_netsuiteID[p];  //p.rnt_nsInternalId
                    SearchMultiSelectField poInternalID = new SearchMultiSelectField();
                    poInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                    poInternalID.operatorSpecified = true;
                    poInternalID.searchValue = new RecordRef[] { r1 };

                    prtsb.internalId = poInternalID;

                    //TBA
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult res = netsuiteService.search(prtsb);
                    Record[] rsRecord = res.recordList;

                    RecordRef refLocation = new RecordRef();
                    for (int i = 0; i < rsRecord.Count(); i++)
                    {
                        PurchaseOrder po2 = (PurchaseOrder)rsRecord[i];
                        refLocation.internalId = po2.location.internalId;
                    }

                    InitializeRef refPO = new InitializeRef();
                    refPO.type = InitializeRefType.purchaseOrder;
                    refPO.internalId = array_netsuiteID[p]; //p.rnt_nsInternalId
                    refPO.typeSpecified = true;

                    InitializeRecord recPO = new InitializeRecord();
                    recPO.type = InitializeType.itemReceipt;
                    recPO.reference = refPO;

                    netsuiteService.tokenPassport = createTokenPassport();
                    ReadResponse rrPO = netsuiteService.initialize(recPO);
                    Record rPO = rrPO.record;

                    ItemReceipt ir1 = (ItemReceipt)rPO;
                    ItemReceipt ir2 = new ItemReceipt();

                    RecordRef refCreatedFrom = new RecordRef();
                    refCreatedFrom.internalId = ir1.createdFrom.internalId;
                    ir2.createdFrom = refCreatedFrom;

                    ItemReceiptItemList iril1 = new ItemReceiptItemList();
                    iril1 = ir1.itemList;
                    for (int i = 0; i < iril1.item.Count(); i++)
                    {
                        iril1.item[i].location = refLocation;
                    }

                    ir2.itemList = iril1;
                    irList[poCount] = ir2;

                    rowCount = poCount + 1;
                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO) values ('ADD', 'SSA-EXCESS PURCHASE ORDER (SSA-EXCESS PURCHASE REQUEST)', 'REQUESTNETSUITETASK.RNT_NSINTERNALID." + array_netsuiteID[p] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "')";
                    this.DataFromNetsuiteLog.Debug("ExcessPOUpdate: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);

                    poCount++;
                    status = true;
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("ExcessPOUpdate Exception: " + ex.ToString());
                    status = false;
                }
            }//end of por

            String jobID = "";
            if (status == true)
            {
                if (rowCount > 0)
                {
                    string loginEmail = "";
                    loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                    tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                    tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                    ItemSearchBasic basic = new ItemSearchBasic()
                    {
                        internalId = new SearchMultiSelectField()
                        {
                            @operator = SearchMultiSelectFieldOperator.anyOf,
                            operatorSpecified = true,
                            searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                        }
                    };
                    Boolean loginStatus = false;
                    netsuiteService.Timeout = 820000000;
                    netsuiteService.CookieContainer = new CookieContainer();
                    ApplicationInfo appinfo = new ApplicationInfo();
                    //  appinfo.applicationId = appID;
                    netsuiteService.applicationInfo = appinfo;
                    try
                    {
                        Console.WriteLine("Success");
                        netsuiteService.tokenPassport = createTokenPassport();
                        SearchResult status1 = netsuiteService.search(basic);
                        if (status1.status.isSuccess == true)
                        {
                            loginStatus = true;
                            netsuiteService.tokenPassport = createTokenPassport();
                            job = netsuiteService.asyncAddList(irList);
                            jobID = job.jobId;

                            var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                            this.DataFromNetsuiteLog.Debug("ExcessPOUpdate: " + updateTask);
                            entities.Database.ExecuteSqlCommand(updateTask);
                        }
                        else
                        {
                            loginStatus = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        loginStatus = false;
                        this.DataFromNetsuiteLog.Fatal("ExcessPOUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                    }

                    //
                }
            }
            else if (rowCount == 0)
            {
                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                this.DataFromNetsuiteLog.Debug("ExcessPOUpdate: " + updateRequestNetsuite);
                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
            }
            return jobID;
        }
        private String ReturnAuthorizationRefundUpdate(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("ReturnAuthorizationRefundUpdate ***************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);


            Boolean status = false;

            AsyncStatusResult job = new AsyncStatusResult();
            Int32 raCount = 0;
            Int32 rowCount = 0;
            Guid gjob_id = Guid.NewGuid();

            //TBA
            string loginEmail = "";
            loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
            tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
            tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

            ItemSearchBasic basic = new ItemSearchBasic()
            {
                internalId = new SearchMultiSelectField()
                {
                    @operator = SearchMultiSelectFieldOperator.anyOf,
                    operatorSpecified = true,
                    searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                }
            };

            Boolean loginStatus = false;
            netsuiteService.Timeout = 820000000;
            netsuiteService.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            //  appinfo.applicationId = appID;
            netsuiteService.applicationInfo = appinfo;
            try
            {
                Console.WriteLine("Success");
                netsuiteService.tokenPassport = createTokenPassport();
                SearchResult status1 = netsuiteService.search(basic);
                if (status1.status.isSuccess == true)
                {
                    loginStatus = true;
                }
                else
                {
                    loginStatus = false;
                }
            }
            catch (Exception ex)
            {
                loginStatus = false;
                this.DataFromNetsuiteLog.Fatal("SOFulfillmentUpdate: Login Netsuite failed. Exception : " + ex.ToString());

            }

            this.DataFromNetsuiteLog.Info("ReturnAuthorizationRefundUpdate: " + array_netsuiteID.Count() + " records to update.");
            CreditMemo[] cmList = new CreditMemo[array_netsuiteID.Count()];

            for (int c = 0; c < array_netsuiteID.Count(); c++)
            {
                try
                {
                    InitializeRef refRA = new InitializeRef();
                    refRA.type = InitializeRefType.returnAuthorization;
                    refRA.internalId = array_netsuiteID[c];  //c.rnt_createdFromInternalId
                    refRA.typeSpecified = true;

                    InitializeRecord recRA = new InitializeRecord();
                    recRA.type = InitializeType.creditMemo;
                    recRA.reference = refRA;

                    //TBA
                    netsuiteService.tokenPassport = createTokenPassport();
                    ReadResponse rrRA = netsuiteService.initialize(recRA);
                    Record rRA = rrRA.record;

                    CreditMemo cm1 = (CreditMemo)rRA;
                    CreditMemo cm2 = new CreditMemo();

                    if (cm1 != null)
                    {
                        RecordRef refCreatedFrom = new RecordRef();
                        refCreatedFrom.internalId = cm1.createdFrom.internalId;
                        cm2.createdFrom = refCreatedFrom;

                        cm2.@class = cm1.@class;

                        CreditMemoItemList cmil = new CreditMemoItemList();
                        cmil = cm1.itemList;

                        for (int i = 0; i < cmil.item.Count(); i++)
                        {
                            cmil.item[i].taxRate1Specified = false;
                        }

                        cm2.itemList = cmil;
                        cmList[raCount] = cm2;

                        rowCount = raCount + 1;
                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-CREDIT MEMO (SSA-RETURN AUTHORIZATION (RECEIVE))', 'REQUESTNETSUITETASK.RNT_NSINTERNALID." + array_netsuiteID[c] + "', '" + gjob_id.ToString() + "'," +
                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + array_netsuiteID[c] + "')";
                        this.DataFromNetsuiteLog.Debug("ReturnAuthorizationRefundUpdate: " + insertTask);
                        entities.Database.ExecuteSqlCommand(insertTask);

                        raCount++;
                        status = true;
                    }
                    /*else
                    {
                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                        this.DataFromNetsuiteLog.Debug("ReturnAuthorizationRefundUpdate: " + updateRequestNetsuite);
                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        status = false;
                    }*/
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("ReturnAuthorizationRefundUpdate Exception: " + ex.ToString());
                    status = false;
                }
            }//end of creditmemo

            String jobID = "";
            if (status == true)
            {
                if (rowCount > 0)
                {
                    //TBA
                    netsuiteService.tokenPassport = createTokenPassport();
                    job = netsuiteService.asyncAddList(cmList);
                    jobID = job.jobId;

                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                    this.DataFromNetsuiteLog.Debug("ReturnAuthorizationRefundUpdate: " + updateTask);
                    entities.Database.ExecuteSqlCommand(updateTask);

                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                    this.DataFromNetsuiteLog.Debug("ReturnAuthorizationRefundUpdate: " + updateRequestNetsuite);
                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                }
            }
            else if (rowCount == 0)
            {
                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                this.DataFromNetsuiteLog.Debug("ReturnAuthorizationRefundUpdate: " + updateRequestNetsuite);
                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
            }
            //scope1.Complete();
            return jobID;
        }
        //To extract update synced list - WY-04.NOV.2014
        //ANET-37 - Sales Order - Auto stop back order
        //Added conditions for No Back Order by Brash Developer
        public Boolean TradeSOUpdateSynced(String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;
            Boolean status = false;

            this.DataFromNetsuiteLog.Info("TradeSOUpdateSynced ***************");
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            //using (TransactionScope scope1 = new TransactionScope()) 
            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("TradeSOUpdateSynced: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("TradeSOUpdateSynced: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //TBA

                // Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("TradeSOUpdateSynced: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {

                        var updateList = (from synced in entities.netsuite_syncupdateso
                                          where synced.nt3_progressStatus == null
                                          //&& synced.nt3_moNo == "SO-MY007428"//temp
                                          select new { synced.nt3_moNo, synced.nt3_moNo_internalID, synced.nt3_rangeTo }).Distinct().Take(200).ToList();

                        SalesOrder[] updateSOList = new SalesOrder[updateList.Count()];
                        this.DataFromNetsuiteLog.Info("TradeSOUpdateSynced: " + updateList.Count() + " records to update.");
                        Int32 rowCount = 0;

                        try
                        {
                            for (int i = 0; i < updateList.Count(); i++)
                            {
                                rowCount = i + 1;
                                Int32 itemCount = 0;
                                Int32 custListSeq = 0;
                                String moInternalID = updateList[i].nt3_moNo_internalID;
                                String moNo = updateList[i].nt3_moNo;
                                DateTime prevRangeFrom = Convert.ToDateTime(updateList[i].nt3_rangeTo).AddHours(-1);
                                DateTime prevRangeTo = Convert.ToDateTime(updateList[i].nt3_rangeTo);

                                SalesOrder tran = new SalesOrder();
                                tran.internalId = moInternalID;

                                var updateListItem = (from synced in entities.netsuite_syncupdateso
                                                      where synced.nt3_progressStatus == null
                                                      && synced.nt3_moNo == moNo
                                                      && synced.nt3_moNo_internalID == moInternalID
                                                      select synced).ToList();

                                SalesOrderItem[] soii = new SalesOrderItem[updateListItem.Count()];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                CustomFieldRef[] custList = new CustomFieldRef[1];
                                for (int a = 0; a < updateListItem.Count(); a++)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();

                                    RecordRef refItem = new RecordRef();
                                    refItem.type = RecordType.inventoryItem;
                                    refItem.typeSpecified = true;
                                    refItem.internalId = updateListItem[a].nt3_item_internalID;
                                    soi.item = refItem;

                                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                    LongCustomFieldRef scfr = new LongCustomFieldRef();
                                    scfr.scriptId = @Resource.CUSTOMFIELD_ITEMSYNCED_SCRIPTID;//custcol_synced
                                    scfr.internalId = @Resource.CUSTOMFIELD_ITEMSYNCED_INTERNALID;//2219
                                    scfr.value = Convert.ToInt32(updateListItem[a].nt3_syncedtoNS);
                                    cfrList[0] = scfr;
                                    soi.customFieldList = cfrList;

                                    soi.line = Convert.ToInt32(updateListItem[a].nt3_itemLine);
                                    soi.lineSpecified = true;

                                    soi.amount = Convert.ToDouble(updateListItem[a].nt3_amount);
                                    soi.amountSpecified = true;

                                    soii[itemCount] = soi;
                                    itemCount++;
                                }
                                soil.item = soii;
                                tran.itemList = soil;

                                //ANET-37 - Sales Order - Auto stop back order
                                /***    START: Brash Developer Set Sync to WMS to No if No back order is checked - 30-APR-2021 ***/
                                #region Sync to WMS equal to No for No Back Order
                                String updateValueSyncWMS = string.Empty;
                                var queryBO = (from so in entities.netsuite_newso
                                               where so.nt1_moNo_internalID == moInternalID
                                               && so.nt1_nobackorder == "Y" && so.nt1_synctowms == "1"
                                               select so).ToList();
                                #endregion
                                /***      END: Brash Developer Set Sync to WMS to No if No back order is checked - 30-APR-2021 ***/


                                /***    START: Set Sync to WMS = NO for Incomplete SO - WY-02.MAR.2015 ***/
                                #region Set Sync to WMS = No for Incomplete SO
                                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                                MySqlConnection mysqlCon = new MySqlConnection(connStr);

                                mysqlCon.Open();

                                var query1 = "SELECT COUNT(a.nt2_moNo) " +
                                             "FROM netsuite_syncso a  " +
                                             "WHERE a.nt2_moNo  ='" + moNo + "' and a.nt2_lastfulfilleddate > '" + convertDateToString(prevRangeFrom) + "' " +
                                             "AND a.nt2_lastfulfilleddate <= '" + convertDateToString(prevRangeTo) + "' AND a.nt2_unfulfilledqty > 0 ";

                                MySqlCommand cmd = new MySqlCommand(query1, mysqlCon);
                                String tolRec = checkIsNull(Convert.ToString(cmd.ExecuteScalar()));

                                //ANET-37 - Sales Order - Auto stop back order
                                //Added below if else by Brash developer on 30-Apr-2021
                                if (tolRec != string.Empty && tolRec != "0" && queryBO.Count > 0)
                                {
                                    custList = new CustomFieldRef[3];
                                }
                                else if (queryBO.Count > 0)
                                {
                                    custList = new CustomFieldRef[2];
                                }
                                else if (tolRec != string.Empty && tolRec != "0")
                                {
                                    custList = new CustomFieldRef[2];
                                }

                                if (tolRec != string.Empty && tolRec != "0")
                                {
                                    //custList = new CustomFieldRef[2];
                                    //commented above condition by Brash developer on 30-Apr-2021
                                    StringCustomFieldRef scfrSyncToWMS = new StringCustomFieldRef();
                                    scfrSyncToWMS.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                    scfrSyncToWMS.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                    scfrSyncToWMS.value = "2";//Sync to WMS = No
                                    custList[custListSeq] = scfrSyncToWMS;
                                    custListSeq = 1; //If got records to set Sync to WMS = No, then Sync Indicator array position need set to 1
                                }

                                cmd.Dispose();
                                mysqlCon.Close();
                                mysqlCon.Dispose();
                                #endregion
                                /***      END: Set Sync to WMS = NO for Incomplete SO - WY-02.MAR.2015 ***/

                                /***    START: Set sync indicator to LOCK/UNLOCK - WY-06.JAN.2015 ***/
                                #region SYNC Indicator - Lock/Unlock
                                String updateValue = string.Empty;
                                var qSalesOrder = (from so in entities.netsuite_syncso
                                                   where so.nt2_moNo_internalID == moInternalID
                                                   && so.nt2_lastfulfilledDate == null
                                                   && so.nt2_qtyForWMS > 0
                                                   select so).ToList();

                                if (qSalesOrder.Count() > 0)
                                {
                                    updateValue = "1";//Lock
                                }
                                else
                                {
                                    updateValue = "2";//Unlock
                                }

                                StringCustomFieldRef scfrLock = new StringCustomFieldRef();
                                scfrLock.scriptId = @Resource.CUSTOMFIELD_SYNCINDICATOR_SCRIPTID;
                                scfrLock.internalId = @Resource.CUSTOMFIELD_SYNCINDICATOR_INTERNALID;
                                scfrLock.value = updateValue;
                                custList[custListSeq] = scfrLock;
                                #endregion
                                /***      END: Set sync indicator to LOCK/UNLOCK - WY-06.JAN.2015 ***/

                                //ANET-37 - Sales Order - Auto stop back order
                                /***    START: Brash Developer Set Sync to WMS to No if No back order is checked - 30-APR-2021 ***/
                                #region Sync to WMS - No  for No Back Order
                                if (queryBO.Count() > 0)
                                {
                                    updateValueSyncWMS = "2"; //Sync to WMS = No

                                    StringCustomFieldRef scfrRefSyncToWMS = new StringCustomFieldRef();
                                    scfrRefSyncToWMS.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                    scfrRefSyncToWMS.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                    scfrRefSyncToWMS.value = updateValueSyncWMS;
                                    custListSeq = custListSeq + 1;
                                    custList[custListSeq] = scfrRefSyncToWMS;

                                    var updateItem = "update netsuite_newso set nt1_synctowms='2' " +
                                                "where nt1_moNo_internalID = '" + moInternalID + "' ";
                                    this.DataFromNetsuiteLog.Debug("SalesOrdersStopBackOrder: " + updateItem);
                                    entities.Database.ExecuteSqlCommand(updateItem);
                                }

                                #endregion
                                /***      END: Brash Developer Set Sync to WMS to No if No back order is checked - 30-APR-2021 ***/

                                tran.customFieldList = custList;
                                updateSOList[i] = tran;

                                var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                 "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + moInternalID + "', '" + gjob_id.ToString() + "'," +
                                                 "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + insertTask);
                                entities.Database.ExecuteSqlCommand(insertTask);

                                var updateSyncUpdateSo = "update netsuite_syncupdateso set nt3_progressStatus='" + gjob_id.ToString() + "' " +
                                " where nt3_moNo_internalID ='" + moInternalID + "'";
                                this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + updateSyncUpdateSo);
                                entities.Database.ExecuteSqlCommand(updateSyncUpdateSo);

                                status = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced Exception: " + ex.ToString());
                            status = false;
                            if (rowCount == 0)
                            {
                                rowCount++;
                            }
                        }

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncUpdateList(updateSOList);
                                jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var updateSyncUpdateSo = "update netsuite_syncupdateso set nt3_progressStatus='" + jobID + "' " +
                                " where nt3_moNo_internalID ='" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + updateSyncUpdateSo);
                                entities.Database.ExecuteSqlCommand(updateSyncUpdateSo);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = '" + transactionType + "' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("TradeSOUpdateSynced: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                        }
                        scope1.Complete();
                    }
                    //logout();
                }
                else
                {

                    this.DataFromNetsuiteLog.Error("TradeSOUpdateSynced: Login Netsuite failed.");
                }
            }
            return status;
        }
        private String SyncHistory_So_Add(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + array_netsuiteID.Count() + " to add.");

                CustomRecord[] addList = new CustomRecord[array_netsuiteID.Count()];
                Int32 rowCount = 0;
                String syncTypeDesc = string.Empty;
                String syncDesc = "[From Netsuite] : Sync to Warehouse/WMS";
                String fulfillDesc = "[From Warehouse/WMS] : Sales Order Fulfillment";

                switch (updateValue)
                {
                    case "2":
                        syncTypeDesc = fulfillDesc;
                        break;
                    case "3":
                        syncTypeDesc = syncDesc;
                        break;
                }

                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;
                    CustomRecord custSyncHistoryRef = new CustomRecord();
                    RecordRef recRef = new RecordRef();
                    recRef.internalId = @Resource.CUSTOMREC_SYNCHISTORY_INTERNALID;
                    custSyncHistoryRef.recType = recRef;

                    StringCustomFieldRef custSynType = new StringCustomFieldRef();
                    custSynType.scriptId = @Resource.CUSTOMREC_SYNCTYPE_SCRIPTID;
                    custSynType.value = syncTypeDesc;

                    DateCustomFieldRef custSyncDate = new DateCustomFieldRef();
                    custSyncDate.scriptId = @Resource.CUSTOMREC_SYNCDATE_SCRIPTID;
                    custSyncDate.value = DateTime.Now;

                    ListOrRecordRef trxInternalID = new ListOrRecordRef();
                    trxInternalID.internalId = array_netsuiteID[i];

                    SelectCustomFieldRef custTrxSync = new SelectCustomFieldRef();
                    custTrxSync.scriptId = @Resource.CUSTOMREC_TRXSYNC_SCRIPTID;
                    custTrxSync.value = trxInternalID;

                    custSyncHistoryRef.customFieldList = new CustomFieldRef[] { custSynType, custSyncDate, custTrxSync };
                    addList[i] = custSyncHistoryRef;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + array_netsuiteID[i] + "')";

                    this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }

                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;


                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {
                        loginStatus = true;
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncAddList(addList);
                        //service.addList(addList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncToWms_So_Update: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SyncToWms_So_Update: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncToWms_So_Update Exception: " + ex.ToString());
            }
            return jobID;
        }
        private String SyncHistory_Po_Add(String transactionType, sdeEntities entities, String[] array_netsuiteID, String updateValue, DateTime rangeTo)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;

            try
            {
                this.DataFromNetsuiteLog.Debug("SyncHistory_Po_Add: " + array_netsuiteID.Count() + " to add.");

                CustomRecord[] addList = new CustomRecord[array_netsuiteID.Count()];
                Int32 rowCount = 0;
                String syncTypeDesc = string.Empty;
                String syncDesc = "[From Netsuite] : Sync to Warehouse/WMS";
                String fulfillDesc = "[From Warehouse/WMS] : Purchase Order Item Receipt";

                switch (updateValue)
                {
                    case "2":
                        syncTypeDesc = fulfillDesc;
                        break;
                    case "3":
                        syncTypeDesc = syncDesc;
                        break;
                }

                for (int i = 0; i < array_netsuiteID.Count(); i++)
                {
                    rowCount = i + 1;
                    CustomRecord custSyncHistoryRef = new CustomRecord();
                    RecordRef recRef = new RecordRef();
                    recRef.internalId = @Resource.CUSTOMREC_SYNCHISTORY_INTERNALID;
                    custSyncHistoryRef.recType = recRef;

                    StringCustomFieldRef custSynType = new StringCustomFieldRef();
                    custSynType.scriptId = @Resource.CUSTOMREC_SYNCTYPE_SCRIPTID;
                    custSynType.value = syncTypeDesc;

                    DateCustomFieldRef custSyncDate = new DateCustomFieldRef();
                    custSyncDate.scriptId = @Resource.CUSTOMREC_SYNCDATE_SCRIPTID;
                    custSyncDate.value = DateTime.Now;

                    ListOrRecordRef trxInternalID = new ListOrRecordRef();
                    trxInternalID.internalId = array_netsuiteID[i];

                    SelectCustomFieldRef custTrxSync = new SelectCustomFieldRef();
                    custTrxSync.scriptId = @Resource.CUSTOMREC_TRXSYNC_SCRIPTID;
                    custTrxSync.value = trxInternalID;

                    custSyncHistoryRef.customFieldList = new CustomFieldRef[] { custSynType, custSyncDate, custTrxSync };
                    addList[i] = custSyncHistoryRef;

                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + array_netsuiteID[i] + "')";

                    this.DataFromNetsuiteLog.Debug("SyncHistory_Po_Add: " + insertTask);
                    entities.Database.ExecuteSqlCommand(insertTask);
                }
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {

                        loginStatus = true;
                        netsuiteService.tokenPassport = createTokenPassport();
                        job = netsuiteService.asyncAddList(addList);
                        //service.addList(addList);
                        jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("SyncHistory_Po_Add: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("SOFulfillmentUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //TBA

            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error("SyncHistory_Po_Add Exception: " + ex.ToString());
            }
            return jobID;
        }
        #region Auto Dropshipment - WY-12.MAR.2015
        //Dropship SO To GMY SO Auto Creation 
        public Boolean DropshipGMYSalesOrderCreation(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                //Boolean loginStatus = login();

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };

                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {

                        this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("DropshipGMYSalesOrderCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }

                //TBA
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        String subsidiaryTH = "GROLIER THAILAND";
                        String subsidiarySEIS = "SEIS";

                        var qDropshipSO = (from c in entities.netsuite_newso
                                           where !(from o in entities.netsuite_dropshipso select o.ds_mono).Contains(c.nt1_moNo)
                                            && (c.nt1_subsidiary == subsidiarySEIS || c.nt1_subsidiary == subsidiaryTH)
                                            && c.nt1_synctowms == "1"// 1=yes
                                            && c.nt1_status == "PENDING FULFILLMENT"
                                           select new
                                           {
                                               c.nt1_moNo_internalID,
                                               c.nt1_moNo,
                                               c.nt1_customer,
                                               c.nt1_discount,
                                               c.nt1_memo,
                                               c.nt1_ponumber,
                                               c.nt1_podate,
                                               c.nt1_subsidiary,
                                               c.nt1_forwarderTo,
                                               c.nt1_forwarderAdd1,
                                               c.nt1_businessChannel_internalID,
                                               c.nt1_customer_booked,
                                               c.nt1_is_fas,
                                               c.nt1_teacher_name,
                                               c.nt1_credit_hold//credit hold
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: " + qDropshipSO.Count() + " records to update.");

                        SalesOrder[] soList = new SalesOrder[qDropshipSO.Count()];
                        foreach (var con in qDropshipSO)
                        {
                            try
                            {
                                Boolean isValid = true;
                                String dropshipCustID = string.Empty;
                                String forwardToInternalID = string.Empty;

                                if (con.nt1_subsidiary == subsidiarySEIS && con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                {
                                    dropshipCustID = @Resource.SEIS_CUSTOMER_MY_EDUGENERAL_INTERNALID;
                                }
                                // Added Code for online store on 23-Mar-2021 by Brash Developer
                                else if (con.nt1_subsidiary == subsidiarySEIS && con.nt1_businessChannel_internalID == @Resource.LOB_ONLINESTORE_INTERNALID)
                                {
                                    dropshipCustID = @Resource.SEIS_CUSTOMER_MY_ONLINESTORE_INTERNALID;
                                }
                                else if (con.nt1_subsidiary == subsidiarySEIS)
                                {
                                    dropshipCustID = @Resource.SEIS_CUSTOMER_MY_INTERNALID;
                                }
                                else if (con.nt1_subsidiary == subsidiaryTH && con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                {
                                    dropshipCustID = @Resource.TH_CUSTOMER_MY_EDUGENERAL_INTERNALID;
                                }
                                // Added Code for online store on 23-Mar-2021 by Brash Developer
                                else if (con.nt1_subsidiary == subsidiaryTH && con.nt1_businessChannel_internalID == @Resource.LOB_ONLINESTORE_INTERNALID)
                                {
                                    dropshipCustID = @Resource.TH_CUSTOMER_MY_ONLINESTORE_INTERNALID;
                                }
                                else if (con.nt1_subsidiary == subsidiaryTH)
                                {
                                    dropshipCustID = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;
                                }



                                if (!String.IsNullOrEmpty(con.nt1_forwarderTo))
                                {
                                    var qForwarder = (from f in entities.forwarderadds
                                                      where f.Address1 == con.nt1_forwarderAdd1 && f.Name == con.nt1_forwarderTo
                                                      && f.CustomerInternalID == dropshipCustID
                                                      select new { f.InternalID }).FirstOrDefault();
                                    if (qForwarder != null)
                                    {
                                        if (qForwarder.InternalID.Count() > 0)
                                        {
                                            forwardToInternalID = qForwarder.InternalID.ToString();
                                        }
                                        else
                                        {
                                            this.DataFromNetsuiteLog.Fatal("This Forwarder To : " + con.nt1_forwarderTo + " , ForwarderAdd : " + con.nt1_forwarderAdd1 + " doesn't exist in TH Dropship " + @Resource.SUBSIDIARY_NAME_MY + " customer.");
                                            isValid = false;
                                        }
                                    }
                                    else
                                    {
                                        this.DataFromNetsuiteLog.Fatal("This Forwarder To : " + con.nt1_forwarderTo + " , ForwarderAdd : " + con.nt1_forwarderAdd1 + " doesn't exist in TH Dropship " + @Resource.SUBSIDIARY_NAME_MY + " customer.");
                                        isValid = false;
                                    }
                                }

                                if (isValid == true)
                                {
                                    #region Sales Order Main Information
                                    this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: Assign Sales Order Main Information : " + con.nt1_moNo + "");
                                    SalesOrder so = new SalesOrder();
                                    String poDate = convertDateToString(Convert.ToDateTime(con.nt1_podate));

                                    //Form 
                                    RecordRef refForm = new RecordRef();
                                    refForm.internalId = @Resource.TRADE_SALES_CUSTOMFORM_MY;
                                    so.customForm = refForm;

                                    //Customer ID
                                    RecordRef refEntity = new RecordRef();
                                    refEntity.internalId = dropshipCustID;
                                    so.entity = refEntity;

                                    //Sales Order Status
                                    so.orderStatus = SalesOrderOrderStatus._pendingApproval;
                                    so.orderStatusSpecified = true;

                                    //Memo
                                    so.memo = con.nt1_memo;

                                    //PO#
                                    so.otherRefNum = con.nt1_ponumber;

                                    //Subsidiary
                                    RecordRef refSubsidiary = new RecordRef();
                                    refSubsidiary.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;
                                    so.subsidiary = refSubsidiary;

                                    //Line of Business
                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = @Resource.LOB_TRADE_INTERNALID;
                                    if (con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                    {
                                        refClass.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;
                                    }
                                    // Added Code for online store on 23-Mar-2021 by Brash Developer
                                    else if (con.nt1_businessChannel_internalID == @Resource.LOB_ONLINESTORE_INTERNALID)
                                    {
                                        refClass.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;
                                    }
                                    so.@class = refClass;

                                    //Location 
                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = @Resource.TRADE_EXCESSFULFILLMENTLOCATION_INTERNALID;
                                    so.location = refLocationSO;

                                    //Sync to WMS
                                    CustomFieldRef[] cfrList = new CustomFieldRef[15];
                                    //if (poDate != "0001-01-01 00:00:00")
                                    //{
                                    //    cfrList = new CustomFieldRef[7];
                                    //}
                                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                                    scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                    scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                    scfr.value = "1";
                                    cfrList[0] = scfr;

                                    //PO Date  
                                    if (poDate != "0001-01-01 00:00:00")
                                    {
                                        DateCustomFieldRef custPoDate = new DateCustomFieldRef();
                                        custPoDate.scriptId = @Resource.CUSTOMFIELD_PODATE_SCRIPTID;
                                        custPoDate.value = Convert.ToDateTime(con.nt1_podate);
                                        cfrList[6] = custPoDate;
                                    }

                                    //SOR Tag
                                    if (con.nt1_subsidiary == subsidiarySEIS && con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                    {
                                        BooleanCustomFieldRef sorTag = new BooleanCustomFieldRef();
                                        sorTag.scriptId = @Resource.CUSTBODY_SORTAG_SCRIPTID;
                                        sorTag.value = true;
                                        cfrList[7] = sorTag;
                                    }

                                    //Customer Booking
                                    if (con.nt1_customer_booked == "Y")
                                    {
                                        BooleanCustomFieldRef custBooking = new BooleanCustomFieldRef();
                                        custBooking.scriptId = @Resource.CUSTOMFIELD_CUST_BOOKING_SCRIPTID;
                                        custBooking.value = true;
                                        cfrList[8] = custBooking;
                                    }
                                    //FAS order 09/10/2018 - Mohan
                                    if (con.nt1_is_fas == "Y")
                                    {
                                        BooleanCustomFieldRef isFas = new BooleanCustomFieldRef();
                                        isFas.scriptId = @Resource.CUSTOMFIELD_FAS_ORDER_INTERNAL_SCRIPTID;
                                        isFas.value = true;
                                        cfrList[9] = isFas;
                                    }
                                    //FAS order 09/10/2018 - Mohan end

                                    //crerdit hold -02052019 - Mohan
                                    if (con.nt1_credit_hold == 1)
                                    {
                                        BooleanCustomFieldRef creditHold = new BooleanCustomFieldRef();
                                        creditHold.scriptId = @Resource.SALES_ORDER_CREDIT_HOLD_FIELDID;
                                        creditHold.value = true;
                                        cfrList[11] = creditHold;
                                    }
                                    #endregion

                                    #region Sales Order Shipping Information
                                    this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: Assign Sales Order Shipping Information");
                                    //Dropship Customer Name
                                    StringCustomFieldRef scfrDropshipCustName = new StringCustomFieldRef();
                                    scfrDropshipCustName.scriptId = @Resource.CUSTOMFIELD_DROPSHIPCUSTOMERNAME_SCRIPTID;
                                    scfrDropshipCustName.internalId = @Resource.CUSTOMFIELD_DROPSHIPCUSTOMERNAME_INTERNALID;
                                    scfrDropshipCustName.value = con.nt1_customer;
                                    cfrList[1] = scfrDropshipCustName;

                                    //Dropship Sales Order Number
                                    StringCustomFieldRef scfrDropshipSONo = new StringCustomFieldRef();
                                    scfrDropshipSONo.scriptId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_SCRIPTID;
                                    scfrDropshipSONo.internalId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_INTERNALID;
                                    scfrDropshipSONo.value = con.nt1_moNo;
                                    cfrList[2] = scfrDropshipSONo;

                                    //Dropship SO Disc
                                    String dropshipSoDisc = string.Empty;
                                    if (con.nt1_discount > 0)
                                    {
                                        dropshipSoDisc = "Discount " + Convert.ToString(con.nt1_discount) + "%";
                                    }
                                    StringCustomFieldRef scfrDropshipSODisc = new StringCustomFieldRef();
                                    scfrDropshipSODisc.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSODISC_SCRIPTID;
                                    scfrDropshipSODisc.internalId = @Resource.CUSTOMFIELD_DROPSHIPSODISC_INTERNALID;
                                    scfrDropshipSODisc.value = dropshipSoDisc;
                                    cfrList[3] = scfrDropshipSODisc;

                                    //Dropship SO Internal ID
                                    StringCustomFieldRef scfrDropshipSOInternalID = new StringCustomFieldRef();
                                    scfrDropshipSOInternalID.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_SCRIPTID;
                                    scfrDropshipSOInternalID.internalId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_INTERNALID;
                                    scfrDropshipSOInternalID.value = con.nt1_moNo_internalID;
                                    cfrList[4] = scfrDropshipSOInternalID;

                                    //FAS order 09/10/2018 - Mohan
                                    //Dropship SO FAS ID
                                    StringCustomFieldRef teacherName = new StringCustomFieldRef();
                                    teacherName.scriptId = @Resource.CUSTOMFIELD_OFFER_DESC_INTERNAL_SCRIPTID;
                                    // teacherName.internalId = @Resource.CUSTOMFIELD_OFFER_DESC_INTERNAL_ID;
                                    teacherName.value = con.nt1_teacher_name;
                                    cfrList[10] = teacherName;

                                    //FAS order 09/10/2018 - Mohan end


                                    //Forwarder Address     
                                    ListOrRecordRef DropshipForwardToInternalID = new ListOrRecordRef();
                                    switch (con.nt1_subsidiary)
                                    {
                                        case "SEIS":
                                            //Added By Brash developer on 20-Apr-2021 for online store
                                            if (con.nt1_businessChannel_internalID == @Resource.LOB_ONLINESTORE_INTERNALID)
                                            {
                                                forwardToInternalID = @Resource.CUSTEC_SEISFORDWARTO_INTERNALID_OS;
                                            }
                                            //Added By Brash developer on 20-Apr-2021 for online store
                                            else if (con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                            {
                                                forwardToInternalID = @Resource.CUSTEC_SEISFORDWARTO_EDUGENERAL_INTERNALID;
                                            }
                                            else
                                            {
                                                forwardToInternalID = @Resource.CUSTEC_SEISFORDWARTO_INTERNALID;
                                            }
                                            break;
                                        case "GROLIER THAILAND":
                                            if (!String.IsNullOrEmpty(con.nt1_forwarderTo))
                                            {
                                                var qForwarder = (from f in entities.forwarderadds
                                                                  where f.Address1 == con.nt1_forwarderAdd1 && f.Name == con.nt1_forwarderTo
                                                                  && f.CustomerInternalID == dropshipCustID
                                                                  select new { f.InternalID }).FirstOrDefault();
                                                if (qForwarder.InternalID.Count() > 0)
                                                {
                                                    forwardToInternalID = qForwarder.InternalID.ToString();
                                                }
                                            }
                                            break;
                                        default:
                                            forwardToInternalID = "";
                                            break;
                                    }
                                    //if (con.nt1_subsidiary == "SEIS" && con.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                    //{
                                    //    forwardToInternalID = @Resource.CUSTEC_SEISFORDWARTO_EDUGENERAL_INTERNALID;
                                    //}

                                    DropshipForwardToInternalID.internalId = forwardToInternalID;

                                    SelectCustomFieldRef scfrForwardTo = new SelectCustomFieldRef();
                                    scfrForwardTo.scriptId = @Resource.CUSTOMFIELD_FORWARDERTO_SCRIPTID;
                                    scfrForwardTo.internalId = @Resource.CUSTOMFIELD_FORWARDERTO_INTERNALID;
                                    scfrForwardTo.value = DropshipForwardToInternalID;
                                    cfrList[5] = scfrForwardTo;
                                    #endregion

                                    #region Sales Order Item Information
                                    this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: Assign Sales Order Items Information");
                                    var qDropshipSOItem = (from c in entities.netsuite_newso
                                                           where c.nt1_moNo == con.nt1_moNo
                                                           select c).ToList();

                                    SalesOrderItem[] soii = new SalesOrderItem[qDropshipSOItem.Count()];
                                    SalesOrderItemList soil = new SalesOrderItemList();

                                    if (qDropshipSOItem.Count() > 0)
                                    {
                                        int itemCount = 0;
                                        foreach (var item in qDropshipSOItem)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();
                                            Double dsprice = 0;

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.nt1_item_internalID;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.nt1_ordQty);
                                            soi.quantitySpecified = true;

                                            //Dropship price
                                            CustomFieldRef[] cfrItemList = new CustomFieldRef[2];//FAS order 09/10/2018 - Mohan
                                            DoubleCustomFieldRef dropshipPrice = new DoubleCustomFieldRef();
                                            dropshipPrice.scriptId = @Resource.CUSTOMFIELD_ITEMDROPSHIPPRICE_SCRIPTID;
                                            dropshipPrice.internalId = @Resource.CUSTOMFIELD_ITEMDROPSHIPPRICE_INTERNALID;
                                            if (con.nt1_discount > 0)
                                            {
                                                dropshipPrice.value = Convert.ToDouble(item.nt1_amount);//Disc Total From Dropship Sales Order
                                            }
                                            else
                                            {
                                                //To handle no discount case
                                                dsprice = Convert.ToDouble(item.nt1_ordQty) * Convert.ToDouble(item.nt1_rate);
                                                dropshipPrice.value = dsprice;
                                            }
                                            cfrItemList[0] = dropshipPrice;
                                            soi.customFieldList = cfrItemList;
                                            this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: dropship price information");
                                            //FAS order 09/10/2018 - Mohan
                                            StringCustomFieldRef className = new StringCustomFieldRef();
                                            className.scriptId = @Resource.CUSTOMFIELD_BOOK_CLUB_CLASS_INTERNAL_SCRIPTID;
                                            // className.internalId = @Resource.CUSTOMFIELD_BOOK_CLUB_CLASS_INTERNAL_ID;
                                            className.value = item.nt1_class_name;
                                            cfrItemList[1] = className;
                                            soi.customFieldList = cfrItemList;

                                            this.DataFromNetsuiteLog.Info("DropshipGMYSalesOrderCreation: dropship class information");
                                            //FAS order 09/10/2018 - Mohan end


                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }

                                        so.customFieldList = cfrList;
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;
                                        rowCount = soCount + 1;

                                        var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', '" + transactionType + "', 'DROPSHIPGMYSOCREATION.DROPSHIPNO." + con.nt1_moNo + '.' + con.nt1_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + insertTask2);
                                        entities.Database.ExecuteSqlCommand(insertTask2);

                                        var insertTask3 = "insert into netsuite_dropshipso (ds_mono, ds_mono_InternalID, ds_createdDate, ds_rangeTo, ds_netsuiteGMYSOProgress) " +
                                                          "values ('" + con.nt1_moNo + "', '" + con.nt1_moNo_internalID + "', " +
                                                          "'" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "', '" + gjob_id.ToString() + "')";
                                        this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + insertTask3);
                                        entities.Database.ExecuteSqlCommand(insertTask3);

                                        soCount++;
                                        status = true;
                                    }
                                    #endregion
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("DropshipGMYSalesOrderCreation Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.addList(soList); 
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(soList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where  rn_id = '" + rn_id + "'";
                                        this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "'  rn_id = '" + rn_id + "'";
                                    this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where  rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("DropshipGMYSalesOrderCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("DropshipGMYSalesOrderCreation Exception:  rn_id = '" + rn_id + "', rn_sche_transactionType= " + transactionType + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("DropshipGMYSalesOrderCreation: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }
        //Close Dropship SO  
        public Boolean CloseDropshipSalesOrders(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            AsyncStatusResult job = new AsyncStatusResult();
            Guid gjob_id = Guid.NewGuid();
            String jobID = null;
            Boolean status = false;

            this.DataFromNetsuiteLog.Info("CloseDropshipSalesOrders ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                //Boolean loginStatus = login();
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("CloseDropshipSalesOrders: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("CloseDropshipSalesOrders: Login Netsuite failed. Exception : " + ex.ToString());

                }
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CloseDropshipSalesOrders: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {

                        var updateList = (from dropShip in entities.netsuite_dropshipso
                                          where dropShip.ds_createdDate > rangeFrom
                                          && dropShip.ds_createdDate <= rangeTo
                                          && dropShip.ds_netsuiteClosedSOProgress == null
                                          select new { dropShip.ds_mono, dropShip.ds_mono_InternalID }).Distinct().ToList();

                        SalesOrder[] updateSOList = new SalesOrder[updateList.Count()];
                        this.DataFromNetsuiteLog.Info("CloseDropshipSalesOrders: " + updateList.Count() + " records to update.");
                        Int32 rowCount = 0;

                        try
                        {
                            for (int i = 0; i < updateList.Count(); i++)
                            {
                                rowCount = i + 1;
                                Int32 itemCount = 0;
                                String moInternalID = updateList[i].ds_mono_InternalID;
                                String moNo = updateList[i].ds_mono;

                                SalesOrder tran = new SalesOrder();
                                tran.internalId = moInternalID;

                                var dropshipSOList = (from newso in entities.netsuite_newso
                                                      where newso.nt1_moNo == moNo
                                                      select new { newso.nt1_itemLine }).ToList();

                                SalesOrderItem[] soii = new SalesOrderItem[dropshipSOList.Count()];
                                SalesOrderItemList soil = new SalesOrderItemList();

                                foreach (var item in dropshipSOList)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();

                                    soi.isClosed = true;
                                    soi.isClosedSpecified = true;

                                    soi.line = Convert.ToInt32(item.nt1_itemLine);
                                    soi.lineSpecified = true;

                                    soii[itemCount] = soi;
                                    itemCount++;
                                }

                                soil.item = soii;
                                tran.itemList = soil;
                                updateSOList[i] = tran;

                                var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                 "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', '" + transactionType + "', 'REQUESTNETSUITE_TASK.RNT_NSINTERNALID." + moInternalID + "', '" + gjob_id.ToString() + "'," +
                                                 "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + insertTask);
                                entities.Database.ExecuteSqlCommand(insertTask);

                                var updateTask = "update netsuite_dropshipso set ds_netsuiteClosedSOProgress = '" + gjob_id.ToString() + "' where ds_mono = '" + moNo + "' " +
                                                 "and ds_createdDate >'" + convertDateToString(rangeFrom) + "' and ds_createdDate <= '" + convertDateToString(rangeTo) + "' ";
                                this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);


                                status = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders Exception: " + ex.ToString());
                            status = false;
                            if (rowCount == 0)
                            {
                                rowCount++;
                            }
                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.updateList(updateSOList);

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncUpdateList(updateSOList);
                                    jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                        this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                    this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CloseDropshipSalesOrders: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("CloseDropshipSalesOrders Exception: rn_id = '" + rn_id + "', rn_sche_transactionType= " + transactionType + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Error("CloseDropshipSalesOrders: Login Netsuite failed.");
                }
            }
            return status;
        }
        //Dropshipment Invoice Creation
        /*public Boolean DropshipInvoiceCreation2(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation ***************");
            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        String subsidiarySEIS = "SEIS";
                        String subsidiaryTH = "GROLIER THAILAND";
                        var dropshipCountry = new string[] { "SG", "TH" };

                        var qListMono = (from q1 in entities.wms_jobordscan
                                         join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                                         where (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                         && q1.jos_businessChannel_code == "ET"
                                         && q1.jos_netsuiteProgress != null 
                                         && dropshipCountry.Contains(q2.nsjm_country)
                                         select new
                                         {
                                             q1.jos_moNo,
                                             q2.nsjm_moNo_internalID,
                                             //isFirstRun = q1.jos_dropshipInvoiceProgress == null ? "Y" : "N", 
                                             isFirstRun = "",
                                             q1.jos_job_ID 
                                         })
                                       .Distinct()
                                       .ToList();
                         
                        List<string> _IDjob = new List<string>();
                        foreach (var qJobID in qListMono)
                        {
                            if (qJobID.isFirstRun == "Y")
                            {
                                _IDjob.Add(qJobID.jos_job_ID);
                            }
                        }

                        var qFilterMono = (from d in qListMono
                                           where d.isFirstRun == "Y"
                                           select new { d.jos_moNo, d.nsjm_moNo_internalID }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];
                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                Boolean isValid = true;
                                //To take dropshipment SO number from GMY SO number
                                var dropshipmo = (from newso in entities.netsuite_newso
                                                  where newso.nt1_moNo_internalID == i.nsjm_moNo_internalID
                                                  select newso.nt1_SEIS_moNo).ToList().FirstOrDefault();
                                
                                if (dropshipmo != null)
                                {
                                    #region Checking for duplicate items with different price 
                                    var invItems = (from newso in entities.netsuite_newso
                                                    where newso.nt1_moNo == dropshipmo
                                                    select new
                                                    {
                                                        newso.nt1_moNo_internalID,
                                                        newso.nt1_customer_internalID,
                                                        newso.nt1_moNo,
                                                        newso.nt1_discount,
                                                        newso.nt1_discountItem_internalID,
                                                        newso.nt1_ponumber,
                                                        newso.nt1_subsidiary,
                                                        newso.nt1_itemID,
                                                        newso.nt1_item_internalID,
                                                        newso.nt1_rate,
                                                        newso.nt1_pricelevel_InternalID
                                                    }).ToList();
                                     
                                    var grpItems = from a in invItems
                                                   let b = new
                                                   {
                                                      itemID = a.nt1_itemID, 
                                                      rate = a.nt1_rate
                                                   }
                                                   group a by b into c
                                                   select new
                                                   {
                                                      itemID = c.Key.itemID,
                                                      rate = c.Key.rate,
                                                      tolItems = c.Count()
                                                   };

                                    var duplicateItems = (from countItem in grpItems
                                                          where countItem.tolItems > 1
                                                          select new { countItem.itemID }).ToList(); 

                                    foreach(var item in duplicateItems)
                                    {
                                        this.DataFromNetsuiteLog.Fatal("This Sales Order Number : " + dropshipmo + "/" + i.nsjm_moNo_internalID + ", item : " + item.itemID + " is duplicate and having different price.");
                                        isValid = false;
                                    }
                                    #endregion

                                    if (isValid == true)
                                    {

                                        //To select the information for dropship SO 
                                        var invoice = (from newso in invItems 
                                                       select new
                                                       {
                                                           newso.nt1_moNo_internalID,
                                                           newso.nt1_customer_internalID,
                                                           newso.nt1_moNo,
                                                           newso.nt1_discount,
                                                           newso.nt1_discountItem_internalID,
                                                           newso.nt1_ponumber,
                                                           newso.nt1_subsidiary
                                                       }).ToList().FirstOrDefault();


                                        if (invoice != null)
                                        {
                                            Invoice inv = new Invoice();
                                            CustomFieldRef[] cfrList = new CustomFieldRef[2];
                                            String dropshipLocation = string.Empty;
                                            String dropshipInvoiceForm = string.Empty;

                                            if (invoice.nt1_subsidiary == subsidiarySEIS)
                                            {
                                                dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                                dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPSG;
                                            }
                                            else
                                            if (invoice.nt1_subsidiary == subsidiaryTH)
                                            {
                                                dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                                dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPTH;
                                            }


                                            #region Main Information
                                            //Form 
                                            RecordRef refForm = new RecordRef();
                                            refForm.internalId = dropshipInvoiceForm;
                                            inv.customForm = refForm;

                                            //Customer 
                                            RecordRef refEntity = new RecordRef();
                                            refEntity.internalId = invoice.nt1_customer_internalID;
                                            inv.entity = refEntity;

                                            //PO #
                                            inv.otherRefNum = invoice.nt1_ponumber;

                                            //Memo - Dropship SO Number
                                            inv.memo = i.jos_moNo + "/" + invoice.nt1_moNo;

                                            //Line of Business
                                            RecordRef refClass = new RecordRef();
                                            refClass.internalId = @Resource.LOB_TRADE_INTERNALID;
                                            inv.@class = refClass;

                                            //Location
                                            RecordRef refLocationSO = new RecordRef();
                                            refLocationSO.internalId = dropshipLocation;
                                            inv.location = refLocationSO;

                                            //Discount Plan 
                                            RecordRef refDiscPlan = new RecordRef();
                                            refDiscPlan.internalId = invoice.nt1_discountItem_internalID;
                                            inv.discountItem = refDiscPlan;

                                            //Dropship Sales Order Number
                                            StringCustomFieldRef scfrDropshipSONo = new StringCustomFieldRef();
                                            scfrDropshipSONo.scriptId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_SCRIPTID;
                                            scfrDropshipSONo.internalId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_INTERNALID;
                                            scfrDropshipSONo.value = invoice.nt1_moNo;
                                            cfrList[0] = scfrDropshipSONo;

                                            //Dropship Sales Order Number Internal ID
                                            StringCustomFieldRef scfrDropshipSONoInternalID = new StringCustomFieldRef();
                                            scfrDropshipSONoInternalID.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_SCRIPTID;
                                            scfrDropshipSONoInternalID.internalId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_INTERNALID;
                                            scfrDropshipSONoInternalID.value = invoice.nt1_moNo_internalID;
                                            cfrList[1] = scfrDropshipSONoInternalID;
                                            #endregion

                                            #region Items Information
                                            //Items 
                                            this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Assign Sales Order Items Information");
                                            var query2 = (from josp in entities.wms_jobordscan_pack
                                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                                          where josp.josp_ordFulFill > 0 && josp.josp_moNo == i.jos_moNo
                                                          && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                                          && _IDjob.Contains(josp.josp_jobID)
                                                          select new
                                                          {
                                                              josp.josp_pack_ID,
                                                              jompd.nsjompd_item_internalID,
                                                              jomp.nsjomp_jobOrdMaster_ID,
                                                              qty = josp.josp_ordFulFill 
                                                          }).ToList();

                                            var groupQ2 = from p in query2
                                                          let k = new
                                                          {
                                                              itemInternalID = p.nsjompd_item_internalID 
                                                          }
                                                          group p by k into g
                                                          select new
                                                          {
                                                              itemInternalID = g.Key.itemInternalID, 
                                                              fulFillQty = g.Sum(p => p.qty)
                                                          };


                                            InvoiceItem[] invii = new InvoiceItem[groupQ2.Count()];
                                            InvoiceItemList invil = new InvoiceItemList();

                                            if (groupQ2.Count() > 0)
                                            {
                                                int itemCount = 0;
                                                foreach (var item in groupQ2)
                                                {
                                                    InvoiceItem invi = new InvoiceItem();
                                                    Double itemRate = 0;
                                                    String priceLevelInternalID = string.Empty;

                                                    RecordRef refItem = new RecordRef();
                                                    refItem.type = RecordType.inventoryItem;
                                                    refItem.typeSpecified = true;
                                                    refItem.internalId = item.itemInternalID;
                                                    invi.item = refItem;

                                                    invi.quantity = Convert.ToDouble(item.fulFillQty);
                                                    invi.quantitySpecified = true;
                                                     
                                                    var itemInfo = (from itemList in invItems
                                                                    where itemList.nt1_item_internalID == item.itemInternalID
                                                                    select new { itemList.nt1_pricelevel_InternalID, itemList.nt1_rate }).ToList().FirstOrDefault();

                                                    if (itemInfo != null)
                                                    {
                                                        priceLevelInternalID = itemInfo.nt1_pricelevel_InternalID;
                                                        itemRate = Convert.ToDouble(itemInfo.nt1_rate);

                                                        RecordRef refPriceLevel = new RecordRef();
                                                        refPriceLevel.internalId = priceLevelInternalID;
                                                        invi.price = refPriceLevel;

                                                        if (priceLevelInternalID == "-1" || priceLevelInternalID == "3") //Custom and Sepcial Price
                                                        {
                                                            invi.rate = Convert.ToString(itemRate);
                                                        }
                                                    }

                                                    invi.orderLine = itemCount + 1;
                                                    invi.orderLineSpecified = true;

                                                    invii[itemCount] = invi;
                                                    itemCount++;
                                                }
                                            }
                                            #endregion

                                            inv.customFieldList = cfrList;
                                            invil.item = invii;
                                            inv.itemList = invil;
                                            invList[invCount] = inv;
                                            rowCount = invCount + 1;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'UPD-STATUS.DROPSHIP INVOICE', 'DROPSHIPINVOICECREATION.DROPSHIPNO." + invoice.nt1_moNo + '.' + invoice.nt1_moNo_internalID + "." + i.jos_moNo + "." + i.nsjm_moNo_internalID + "'," +
                                                "'" + gjob_id.ToString() + "'," +

                                                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updJobOrdScan = "UPDATE wms_jobordscan SET jos_dropshipInvoiceProgress = '" + gjob_id.ToString() + "' WHERE jos_dropshipInvoiceProgress is null " +
                                                                "and jos_moNo = '" + i.jos_moNo + "' " +
                                                                "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                                "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updJobOrdScan);
                                            entities.Database.ExecuteSqlCommand(updJobOrdScan);

                                            invCount++;
                                            status = true;
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("DropshipInvoiceCreation Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.addList(invList);
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug(updateTask);
                                    this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = " + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = " + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = " + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            scope1.Complete();
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("DropshipInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("DropshipInvoiceCreation: Login Netsuite failed.");
                }
            }
            logout();
            return status;
        }*/
        //Dropshipment Invoice Creation
        public Boolean DropshipFulfillmentCreation(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("DropshipFulfillmentCreation ***************");
            Boolean status = false;
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            using (TransactionScope scope1 = new TransactionScope())
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("DropshipFulfillmentCreation: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("DropshipFulfillmentCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }



                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("DropshipFulfillmentCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        String subsidiarySEIS = "SEIS";
                        String subsidiaryTH = "GROLIER THAILAND";
                        String createdDate = convertDateToString(DateTime.Now);

                        var qFilterMono = (from q1 in entities.netsuite_dropshipfulfillment
                                           where (q1.dsf_lastfulfilleddate > rangeFrom && q1.dsf_lastfulfilleddate <= rangeTo)
                                           && (q1.dsf_iffProgress == null || q1.dsf_iffProgress == "")
                                           && q1.dsf_status != "CLOSED"
                                           select new
                                           {
                                               q1.dsf_dropshipmono_internalID,
                                               q1.dsf_dropshipmono,
                                               q1.dsf_GMYmono,
                                               q1.dsf_GMYmono_internalID,
                                               q1.dsf_customer_InternalID,
                                               //q1.dsf_discount,
                                               //q1.dsf_discountItem_internalID,
                                               q1.dsf_ponumber,
                                               q1.dsf_subsidiary,
                                               q1.dsf_is_fas
                                               //q1.dsf_basedprice //#1050
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("DropshipFulfillmentCreation: " + qFilterMono.Count() + " records to update.");
                        ItemFulfillment[] iffList = new ItemFulfillment[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {

                            if (i.dsf_is_fas == "Y") //FAS order 09/10/2018 - Mohan
                            {
                                try
                                {
                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = i.dsf_dropshipmono_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.itemFulfillment;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;

                                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                    ItemFulfillment iff2 = new ItemFulfillment();

                                    if (iff1 != null)
                                    {

                                        ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                        iff2.createdFrom = refCreatedFrom;

                                        iff2.tranDate = DateTime.Now;
                                        iff2.tranDateSpecified = true;

                                        //Added for Advanced Inventory - WY-23.JUNE.2015
                                        iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                        iff2.shipStatusSpecified = true;

                                        //Memo - Dropship SO Number
                                        iff2.memo = i.dsf_GMYmono;

                                        ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];


                                        //To select the information for dropship SO 
                                        var fulfillment = (from ds in entities.netsuite_dropshipfulfillment
                                                           where (ds.dsf_lastfulfilleddate > rangeFrom && ds.dsf_lastfulfilleddate <= rangeTo
                                                           && (ds.dsf_iffProgress == null || ds.dsf_iffProgress == "")
                                                           && ds.dsf_dropshipmono == i.dsf_dropshipmono)
                                                           && ds.dsf_status != "CLOSED"
                                                           select new
                                                           {
                                                               ds.dsf_itemID,
                                                               ds.dsf_item_internalID,
                                                               ds.dsf_rate,
                                                               ds.dsf_pricelevel_InternalID,
                                                               ds.dsf_pricelevel,
                                                               ds.dsf_ordQty,
                                                               ds.dsf_wmsfulfilledqty,
                                                               ds.dsf_createdinvoiceqty,
                                                               ds.dsf_basedprice,//#1050
                                                               ds.dsf_is_fas
                                                               //ds.dsf_class_name
                                                           }).ToList();

                                        this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: Checked netsuite_dropshipfulfillment for " + i.dsf_dropshipmono);

                                        if (ifitems.Count() > 0)
                                        {
                                            for (int j = 0; j < ifitemlist.item.Length; j++)
                                            {

                                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                                iffi.item = ifitemlist.item[j].item;
                                                iffi.quantity = 0;
                                                iffi.quantitySpecified = true;
                                                iffi.itemIsFulfilled = false;
                                                iffi.itemIsFulfilledSpecified = true;
                                                iffi.orderLine = ifitemlist.item[j].orderLine;
                                                iffi.orderLineSpecified = true;



                                                int index = fulfillment.FindIndex(s => s.dsf_item_internalID == ifitemlist.item[j].item.internalId && s.dsf_ordQty == ifitemlist.item[j].quantity);
                                                // s.dsf_ordQty == ifitemlist.item[j].quantity i.dsf_class_name + "--- " +
                                                Console.WriteLine(index + "--- " + ifitemlist.item[j].item.internalId + "--- " + fulfillment[index].dsf_wmsfulfilledqty + "--- " + "--- " + ifitemlist.item[j].quantity);
                                                if (index >= 0)
                                                {
                                                    String dropshipLocation = string.Empty;

                                                    if (i.dsf_subsidiary == subsidiarySEIS)
                                                    {
                                                        dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                                    }
                                                    else if (i.dsf_subsidiary == subsidiaryTH)
                                                    {
                                                        dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                                    }

                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = dropshipLocation;
                                                    iffi.location = refLocation;

                                                    if (fulfillment[index].dsf_wmsfulfilledqty > fulfillment[index].dsf_ordQty)
                                                    {
                                                        this.DataFromNetsuiteLog.Fatal("This Sales Order : " + i.dsf_dropshipmono + "/" + i.dsf_GMYmono + ",Item ISBN : " + fulfillment[index].dsf_itemID + " having OrdQty (" + fulfillment[index].dsf_ordQty + ") < FulfilledQty (" + fulfillment[index].dsf_wmsfulfilledqty + "), Rate : " + fulfillment[index].dsf_rate);
                                                    }
                                                    else
                                                    {
                                                        if (ifitemlist.item[j].quantityRemaining >= fulfillment[index].dsf_wmsfulfilledqty)
                                                        {
                                                            iffi.quantity = Convert.ToInt32(fulfillment[index].dsf_wmsfulfilledqty);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;
                                                        }
                                                        else
                                                        {
                                                            iffi.quantity = Convert.ToInt32(ifitemlist.item[j].quantityRemaining);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;
                                                        }



                                                    }

                                                }
                                                ifitems[j] = iffi;
                                            }

                                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                            ifil1.replaceAll = false;
                                            ifil1.item = ifitems;
                                            iff2.itemList = ifil1;

                                            iffList[ordCount] = iff2;

                                            rowCount = ordCount + 1;

                                            ordCount++;
                                            status = true;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-DROPSHIP FULFILLMENT', 'DROPSHIPFULFILLMENT.DROPSHIPNO." + i.dsf_dropshipmono + '.' + i.dsf_dropshipmono_internalID + "." + i.dsf_GMYmono + "." + i.dsf_GMYmono_internalID + "'," +
                                                    "'" + gjob_id.ToString() + "','START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_iffProgress = '" + gjob_id.ToString() + "'" +
                                                        ",dsf_iffSeqNo = '" + rowCount + "'" +
                                                        "WHERE dsf_dropshipmono = '" + i.dsf_dropshipmono + "' " +
                                                        "AND dsf_lastfulfilleddate > '" + convertDateToString(rangeFrom) + "' " +
                                                        "AND dsf_lastfulfilleddate <= '" + convertDateToString(rangeTo) + "' ";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updDSF);
                                            entities.Database.ExecuteSqlCommand(updDSF);

                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("FAS DropshipFulfillmentCreation Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }



                            }
                            else
                            {
                                try
                                {

                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = i.dsf_dropshipmono_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.itemFulfillment;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;

                                    ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                    ItemFulfillment iff2 = new ItemFulfillment();



                                    if (iff1 != null)
                                    {

                                        ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                        iff2.createdFrom = refCreatedFrom;

                                        iff2.tranDate = DateTime.Now;
                                        iff2.tranDateSpecified = true;

                                        //Added for Advanced Inventory - WY-23.JUNE.2015
                                        iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                        iff2.shipStatusSpecified = true;

                                        //Memo - Dropship SO Number
                                        iff2.memo = i.dsf_GMYmono;

                                        ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];


                                        //To select the information for dropship SO 
                                        var fulfillment = (from ds in entities.netsuite_dropshipfulfillment
                                                           where (ds.dsf_lastfulfilleddate > rangeFrom && ds.dsf_lastfulfilleddate <= rangeTo
                                                           && (ds.dsf_iffProgress == null || ds.dsf_iffProgress == "")
                                                           && ds.dsf_dropshipmono == i.dsf_dropshipmono)
                                                           && ds.dsf_status != "CLOSED"
                                                           select new
                                                           {
                                                               ds.dsf_itemID,
                                                               ds.dsf_item_internalID,
                                                               ds.dsf_rate,
                                                               ds.dsf_pricelevel_InternalID,
                                                               ds.dsf_pricelevel,
                                                               ds.dsf_ordQty,
                                                               ds.dsf_wmsfulfilledqty,
                                                               ds.dsf_createdinvoiceqty,
                                                               ds.dsf_basedprice //#1050
                                                           }).ToList();

                                        this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: Checked netsuite_dropshipfulfillment for " + i.dsf_dropshipmono);

                                        if (ifitems.Count() > 0)
                                        {
                                            for (int j = 0; j < ifitemlist.item.Length; j++)
                                            {

                                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                                iffi.item = ifitemlist.item[j].item;
                                                iffi.quantity = 0;
                                                iffi.quantitySpecified = true;
                                                iffi.itemIsFulfilled = false;
                                                iffi.itemIsFulfilledSpecified = true;
                                                iffi.orderLine = ifitemlist.item[j].orderLine;
                                                iffi.orderLineSpecified = true;


                                                int index = fulfillment.FindIndex(s => s.dsf_item_internalID == ifitemlist.item[j].item.internalId);
                                                if (index >= 0)
                                                {
                                                    String dropshipLocation = string.Empty;

                                                    if (i.dsf_subsidiary == subsidiarySEIS)
                                                    {
                                                        dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                                    }
                                                    else if (i.dsf_subsidiary == subsidiaryTH)
                                                    {
                                                        dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                                    }

                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = dropshipLocation;
                                                    iffi.location = refLocation;

                                                    if (fulfillment[index].dsf_wmsfulfilledqty > fulfillment[index].dsf_ordQty)
                                                    {
                                                        this.DataFromNetsuiteLog.Fatal("This Sales Order : " + i.dsf_dropshipmono + "/" + i.dsf_GMYmono + ",Item ISBN : " + fulfillment[index].dsf_itemID + " having OrdQty (" + fulfillment[index].dsf_ordQty + ") < FulfilledQty (" + fulfillment[index].dsf_wmsfulfilledqty + "), Rate : " + fulfillment[index].dsf_rate);
                                                    }
                                                    else
                                                    {
                                                        if (ifitemlist.item[j].quantityRemaining >= fulfillment[index].dsf_wmsfulfilledqty)
                                                        {
                                                            iffi.quantity = Convert.ToInt32(fulfillment[index].dsf_wmsfulfilledqty);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;
                                                        }
                                                        else
                                                        {
                                                            iffi.quantity = Convert.ToInt32(ifitemlist.item[j].quantityRemaining);
                                                            iffi.quantitySpecified = true;

                                                            iffi.itemIsFulfilled = true;
                                                            iffi.itemIsFulfilledSpecified = true;
                                                        }



                                                    }

                                                }
                                                ifitems[j] = iffi;
                                            }

                                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                            ifil1.replaceAll = false;
                                            ifil1.item = ifitems;
                                            iff2.itemList = ifil1;

                                            iffList[ordCount] = iff2;

                                            rowCount = ordCount + 1;

                                            ordCount++;
                                            status = true;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-DROPSHIP FULFILLMENT', 'DROPSHIPFULFILLMENT.DROPSHIPNO." + i.dsf_dropshipmono + '.' + i.dsf_dropshipmono_internalID + "." + i.dsf_GMYmono + "." + i.dsf_GMYmono_internalID + "'," +
                                                    "'" + gjob_id.ToString() + "','START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_iffProgress = '" + gjob_id.ToString() + "'" +
                                                        ",dsf_iffSeqNo = '" + rowCount + "'" +
                                                        "WHERE dsf_dropshipmono = '" + i.dsf_dropshipmono + "' " +
                                                        "AND dsf_lastfulfilleddate > '" + convertDateToString(rangeFrom) + "' " +
                                                        "AND dsf_lastfulfilleddate <= '" + convertDateToString(rangeTo) + "' ";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updDSF);
                                            entities.Database.ExecuteSqlCommand(updDSF);

                                        }
                                    }
                                }//end try
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("DropshipFulfillmentCreation Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }

                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(iffList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug(updateTask);
                                        this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_iffProgress = '" + jobID + "'" +
                                                                "WHERE dsf_iffProgress = '" + gjob_id.ToString() + "' " +
                                                                "AND dsf_lastfulfilleddate > '" + convertDateToString(rangeFrom) + "' " +
                                                                "AND dsf_lastfulfilleddate <= '" + convertDateToString(rangeTo) + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + updDSF);
                                        entities.Database.ExecuteSqlCommand(updDSF);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("DropshipFulfillmentCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("DropshipFulfillmentCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("DropshipFulfillmentCreation: Login Netsuite failed.");
                }
            }
            //logout();
            return status;
        }
        public Boolean DropshipInvoiceCreation(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation ***************");
            Boolean status = false;
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);


            using (TransactionScope scope1 = new TransactionScope())
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("DropshipInvoiceCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }


                // Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        String subsidiarySEIS = "SEIS";
                        String subsidiaryTH = "GROLIER THAILAND";
                        String createdDate = convertDateToString(DateTime.Now);

                        var qFilterMono = (from q1 in entities.netsuite_dropshipfulfillment
                                           where (q1.dsf_iffUpdatedDate > rangeFrom && q1.dsf_iffUpdatedDate <= rangeTo)
                                           && (q1.dsf_invProgress == null || q1.dsf_invProgress == "")
                                           && q1.dsf_iffInternalID != null
                                           && q1.dsf_status != "CLOSED"
                                           select new
                                           {
                                               q1.dsf_dropshipmono_internalID,
                                               q1.dsf_dropshipmono,
                                               q1.dsf_GMYmono,
                                               q1.dsf_GMYmono_internalID,
                                               q1.dsf_customer_InternalID,
                                               q1.dsf_discount,
                                               q1.dsf_discountItem_internalID,
                                               q1.dsf_ponumber,
                                               q1.dsf_subsidiary,
                                               q1.dsf_is_fas //FAS order 09/10/2018 - Mohan
                                               //q1.dsf_basedprice //#1050
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];
                        foreach (var i in qFilterMono)
                        {
                            if (i.dsf_is_fas == "Y")
                            {

                                try
                                {
                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = i.dsf_dropshipmono_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.invoice;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;


                                    Invoice inv2 = (Invoice)rSO;
                                    Invoice inv = new Invoice();

                                    if (inv2 != null)
                                    {
                                        InvoiceItemList invitemlist = inv2.itemList;

                                        CustomFieldRef[] cfrList = new CustomFieldRef[2];
                                        String dropshipLocation = string.Empty;
                                        String dropshipInvoiceForm = string.Empty;

                                        if (i.dsf_subsidiary == subsidiarySEIS)
                                        {
                                            dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                            dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPSG;
                                        }
                                        else if (i.dsf_subsidiary == subsidiaryTH)
                                        {
                                            dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                            dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPTH;
                                        }


                                        //Form 
                                        RecordRef refForm = new RecordRef();
                                        refForm.internalId = dropshipInvoiceForm;
                                        inv.customForm = refForm;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                        inv.createdFrom = refCreatedFrom;

                                        //Customer 
                                        RecordRef refEntity = new RecordRef();
                                        refEntity.internalId = i.dsf_customer_InternalID;
                                        inv.entity = refEntity;

                                        //PO #
                                        inv.otherRefNum = i.dsf_ponumber;

                                        //Memo - Dropship SO Number
                                        inv.memo = i.dsf_GMYmono;

                                        //Location
                                        RecordRef refLocationSO = new RecordRef();
                                        refLocationSO.internalId = dropshipLocation;
                                        inv.location = refLocationSO;

                                        //Discount Plan 
                                        RecordRef refDiscPlan = new RecordRef();
                                        refDiscPlan.internalId = i.dsf_discountItem_internalID;
                                        inv.discountItem = refDiscPlan;

                                        //Dropship Sales Order Number
                                        StringCustomFieldRef scfrDropshipSONo = new StringCustomFieldRef();
                                        scfrDropshipSONo.scriptId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_SCRIPTID;
                                        scfrDropshipSONo.internalId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_INTERNALID;
                                        scfrDropshipSONo.value = i.dsf_dropshipmono;
                                        cfrList[0] = scfrDropshipSONo;

                                        //Dropship Sales Order Number Internal ID
                                        StringCustomFieldRef scfrDropshipSONoInternalID = new StringCustomFieldRef();
                                        scfrDropshipSONoInternalID.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_SCRIPTID;
                                        scfrDropshipSONoInternalID.internalId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_INTERNALID;
                                        scfrDropshipSONoInternalID.value = i.dsf_dropshipmono_internalID;
                                        cfrList[1] = scfrDropshipSONoInternalID;

                                        this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Assign Sales Order Items Information");



                                        //To select the information for dropship SO 
                                        var invoice = (from ds in entities.netsuite_dropshipfulfillment
                                                       where (ds.dsf_iffUpdatedDate > rangeFrom && ds.dsf_iffUpdatedDate <= rangeTo)
                                                       && (ds.dsf_invProgress == null || ds.dsf_invProgress == "")
                                                       && ds.dsf_iffInternalID != null
                                                       && ds.dsf_dropshipmono == i.dsf_dropshipmono
                                                       && ds.dsf_status != "CLOSED"
                                                       select new
                                                       {
                                                           ds.dsf_itemID,
                                                           ds.dsf_item_internalID,
                                                           ds.dsf_rate,
                                                           ds.dsf_pricelevel_InternalID,
                                                           ds.dsf_pricelevel,
                                                           ds.dsf_ordQty,
                                                           ds.dsf_wmsfulfilledqty,
                                                           ds.dsf_createdinvoiceqty,
                                                           ds.dsf_basedprice,//#1050
                                                           ds.dsf_is_fas
                                                       }).ToList();

                                        this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: Checked netsuite_dropshipfulfillment for " + i.dsf_dropshipmono);

                                        if (inv2.itemList != null)
                                        {
                                            InvoiceItem[] invitems = new InvoiceItem[invitemlist.item.Length];
                                            for (int a = 0; a < invitemlist.item.Length; a++)
                                            {

                                                InvoiceItem invi = new InvoiceItem();
                                                invi.item = invitemlist.item[a].item;
                                                invi.quantity = 0;
                                                invi.quantitySpecified = true;
                                                invi.itemIsFulfilled = false;
                                                invi.itemIsFulfilledSpecified = true;
                                                invi.orderLine = invitemlist.item[a].orderLine;
                                                invi.orderLineSpecified = true;

                                                int index = invoice.FindIndex(s => s.dsf_item_internalID == invitemlist.item[a].item.internalId && s.dsf_wmsfulfilledqty == invitemlist.item[a].quantity);
                                                //&& s.dsf_wmsfulfilledqty == invitemlist.item[a].quantityFulfilled
                                                Console.WriteLine(invitemlist.item[a].item.internalId + "--- " + invitemlist.item[a].quantityFulfilled + "----" + invitemlist.item[a].quantity);
                                                if (index >= 0)
                                                {

                                                    Double itemRate = 0;
                                                    String priceLevelInternalID = string.Empty;
                                                    Double USDPrice = 0;
                                                    int invoiceQty = 0;

                                                    if (invoice[index].dsf_wmsfulfilledqty > invoice[index].dsf_ordQty)
                                                    {
                                                        this.DataFromNetsuiteLog.Fatal("This Sales Order : " + i.dsf_dropshipmono + "/" + i.dsf_GMYmono + ",Item ISBN : " + invoice[index].dsf_itemID + " having OrdQty (" + invoice[index].dsf_ordQty + ") < FulfilledQty (" + invoice[index].dsf_wmsfulfilledqty + "), Rate : " + invoice[index].dsf_rate);
                                                    }
                                                    else
                                                    {
                                                        invoiceQty = (int)invoice[index].dsf_wmsfulfilledqty;

                                                        invi.quantity = Convert.ToDouble(invoiceQty);
                                                        invi.quantitySpecified = true;

                                                        priceLevelInternalID = invoice[index].dsf_pricelevel_InternalID;
                                                        itemRate = Convert.ToDouble(invoice[index].dsf_rate);

                                                        //Commented by WY - 23.Nov.2015 - Netsuite Trx Form was fixed.
                                                        //if (i.dsf_subsidiary == subsidiaryTH)//Only TH need to set grossAmt because NS trx form calculation not working
                                                        //{
                                                        //    grossAmount = itemRate * Convert.ToDouble(invoiceQty);
                                                        //    invi.grossAmt = grossAmount;
                                                        //    invi.grossAmtSpecified = true;
                                                        //}

                                                        priceLevelInternalID = "-1"; //To set all the price level to Custom - WY - 09.DEC.2015

                                                        RecordRef refPriceLevel = new RecordRef();
                                                        refPriceLevel.internalId = priceLevelInternalID;
                                                        invi.price = refPriceLevel;

                                                        if (priceLevelInternalID == "-1")//Custom 
                                                        {
                                                            invi.rate = Convert.ToString(itemRate);
                                                        }

                                                        //#1050 - Set price for USD  
                                                        USDPrice = Convert.ToDouble(invoice[index].dsf_basedprice);
                                                        if (USDPrice != 0) //If db not been store, then will follow the setup in item
                                                        {
                                                            CustomFieldRef[] cfrItemList = new CustomFieldRef[1];
                                                            DoubleCustomFieldRef refUSDPrice = new DoubleCustomFieldRef();
                                                            refUSDPrice.scriptId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_SCRIPTID;
                                                            refUSDPrice.internalId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_INTERNALID;
                                                            refUSDPrice.value = USDPrice;
                                                            cfrItemList[0] = refUSDPrice;
                                                            invi.customFieldList = cfrItemList;
                                                        }

                                                        invitems[a] = invi;

                                                    }
                                                }
                                                else
                                                {
                                                    //Double itemRate = 0;
                                                    String priceLevelInternalID = string.Empty;
                                                    //Double USDPrice = 0;
                                                    int invoiceQty = 0;

                                                    invoiceQty = 0;

                                                    invi.quantity = Convert.ToDouble(invoiceQty);
                                                    invi.quantitySpecified = true;
                                                    invitems[a] = invi;
                                                }

                                            }//end item loop
                                            inv.customFieldList = cfrList;
                                            invitemlist.item = invitems;
                                            inv.itemList = invitemlist;
                                            invList[invCount] = inv;
                                            rowCount = invCount + 1;
                                            status = true;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'UPD-STATUS.DROPSHIP INVOICE', 'DROPSHIPINVOICECREATION.DROPSHIPNO." + i.dsf_dropshipmono + '.' + i.dsf_dropshipmono_internalID + "." + i.dsf_GMYmono + "." + i.dsf_GMYmono_internalID + "'," +
                                                "'" + gjob_id.ToString() + "','START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_invProgress = '" + gjob_id.ToString() + "'" + ",dsf_lastinvoicedate = '" + createdDate + "' " +
                                                        ", dsf_invSeqNo = '" + rowCount + "'" +
                                                        "WHERE dsf_dropshipmono = '" + i.dsf_dropshipmono + "' " +
                                                        "AND dsf_iffUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                        "AND dsf_iffUpdatedDate <= '" + convertDateToString(rangeTo) + "' ";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updDSF);
                                            entities.Database.ExecuteSqlCommand(updDSF);

                                            invCount++;

                                        }

                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("DropshipInvoiceCreation Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }

                            }
                            else
                            {
                                try
                                {
                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = i.dsf_dropshipmono_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.invoice;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;


                                    Invoice inv2 = (Invoice)rSO;
                                    Invoice inv = new Invoice();

                                    if (inv2 != null)
                                    {
                                        InvoiceItemList invitemlist = inv2.itemList;

                                        CustomFieldRef[] cfrList = new CustomFieldRef[2];
                                        String dropshipLocation = string.Empty;
                                        String dropshipInvoiceForm = string.Empty;

                                        if (i.dsf_subsidiary == subsidiarySEIS)
                                        {
                                            dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                            dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPSG;
                                        }
                                        else if (i.dsf_subsidiary == subsidiaryTH)
                                        {
                                            dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                            dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPTH;
                                        }


                                        //Form 
                                        RecordRef refForm = new RecordRef();
                                        refForm.internalId = dropshipInvoiceForm;
                                        inv.customForm = refForm;

                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                        inv.createdFrom = refCreatedFrom;

                                        //Customer 
                                        RecordRef refEntity = new RecordRef();
                                        refEntity.internalId = i.dsf_customer_InternalID;
                                        inv.entity = refEntity;

                                        //PO #
                                        inv.otherRefNum = i.dsf_ponumber;

                                        //Memo - Dropship SO Number
                                        inv.memo = i.dsf_GMYmono;

                                        //Location
                                        RecordRef refLocationSO = new RecordRef();
                                        refLocationSO.internalId = dropshipLocation;
                                        inv.location = refLocationSO;

                                        //Discount Plan 
                                        RecordRef refDiscPlan = new RecordRef();
                                        refDiscPlan.internalId = i.dsf_discountItem_internalID;
                                        inv.discountItem = refDiscPlan;

                                        //Dropship Sales Order Number
                                        StringCustomFieldRef scfrDropshipSONo = new StringCustomFieldRef();
                                        scfrDropshipSONo.scriptId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_SCRIPTID;
                                        scfrDropshipSONo.internalId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_INTERNALID;
                                        scfrDropshipSONo.value = i.dsf_dropshipmono;
                                        cfrList[0] = scfrDropshipSONo;

                                        //Dropship Sales Order Number Internal ID
                                        StringCustomFieldRef scfrDropshipSONoInternalID = new StringCustomFieldRef();
                                        scfrDropshipSONoInternalID.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_SCRIPTID;
                                        scfrDropshipSONoInternalID.internalId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_INTERNALID;
                                        scfrDropshipSONoInternalID.value = i.dsf_dropshipmono_internalID;
                                        cfrList[1] = scfrDropshipSONoInternalID;

                                        this.DataFromNetsuiteLog.Info("DropshipInvoiceCreation: Assign Sales Order Items Information");



                                        //To select the information for dropship SO 
                                        var invoice = (from ds in entities.netsuite_dropshipfulfillment
                                                       where (ds.dsf_iffUpdatedDate > rangeFrom && ds.dsf_iffUpdatedDate <= rangeTo)
                                                       && (ds.dsf_invProgress == null || ds.dsf_invProgress == "")
                                                       && ds.dsf_iffInternalID != null
                                                       && ds.dsf_dropshipmono == i.dsf_dropshipmono
                                                       && ds.dsf_status != "CLOSED"
                                                       select new
                                                       {
                                                           ds.dsf_itemID,
                                                           ds.dsf_item_internalID,
                                                           ds.dsf_rate,
                                                           ds.dsf_pricelevel_InternalID,
                                                           ds.dsf_pricelevel,
                                                           ds.dsf_ordQty,
                                                           ds.dsf_wmsfulfilledqty,
                                                           ds.dsf_createdinvoiceqty,
                                                           ds.dsf_basedprice //#1050
                                                       }).ToList();

                                        this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: Checked netsuite_dropshipfulfillment for " + i.dsf_dropshipmono);

                                        if (inv2.itemList != null)
                                        {
                                            InvoiceItem[] invitems = new InvoiceItem[invitemlist.item.Length];
                                            for (int a = 0; a < invitemlist.item.Length; a++)
                                            {

                                                InvoiceItem invi = new InvoiceItem();
                                                invi.item = invitemlist.item[a].item;
                                                invi.quantity = 0;
                                                invi.quantitySpecified = true;
                                                invi.itemIsFulfilled = false;
                                                invi.itemIsFulfilledSpecified = true;
                                                invi.orderLine = invitemlist.item[a].orderLine;
                                                invi.orderLineSpecified = true;

                                                int index = invoice.FindIndex(s => s.dsf_item_internalID == invitemlist.item[a].item.internalId);
                                                if (index >= 0)
                                                {

                                                    Double itemRate = 0;
                                                    String priceLevelInternalID = string.Empty;
                                                    Double USDPrice = 0;
                                                    int invoiceQty = 0;

                                                    if (invoice[index].dsf_wmsfulfilledqty > invoice[index].dsf_ordQty)
                                                    {
                                                        this.DataFromNetsuiteLog.Fatal("This Sales Order : " + i.dsf_dropshipmono + "/" + i.dsf_GMYmono + ",Item ISBN : " + invoice[index].dsf_itemID + " having OrdQty (" + invoice[index].dsf_ordQty + ") < FulfilledQty (" + invoice[index].dsf_wmsfulfilledqty + "), Rate : " + invoice[index].dsf_rate);
                                                    }
                                                    else
                                                    {
                                                        invoiceQty = (int)invoice[index].dsf_wmsfulfilledqty;

                                                        invi.quantity = Convert.ToDouble(invoiceQty);
                                                        invi.quantitySpecified = true;

                                                        priceLevelInternalID = invoice[index].dsf_pricelevel_InternalID;
                                                        itemRate = Convert.ToDouble(invoice[index].dsf_rate);

                                                        //Commented by WY - 23.Nov.2015 - Netsuite Trx Form was fixed.
                                                        //if (i.dsf_subsidiary == subsidiaryTH)//Only TH need to set grossAmt because NS trx form calculation not working
                                                        //{
                                                        //    grossAmount = itemRate * Convert.ToDouble(invoiceQty);
                                                        //    invi.grossAmt = grossAmount;
                                                        //    invi.grossAmtSpecified = true;
                                                        //}

                                                        priceLevelInternalID = "-1"; //To set all the price level to Custom - WY - 09.DEC.2015

                                                        RecordRef refPriceLevel = new RecordRef();
                                                        refPriceLevel.internalId = priceLevelInternalID;
                                                        invi.price = refPriceLevel;

                                                        if (priceLevelInternalID == "-1")//Custom 
                                                        {
                                                            invi.rate = Convert.ToString(itemRate);
                                                        }

                                                        //#1050 - Set price for USD  
                                                        USDPrice = Convert.ToDouble(invoice[index].dsf_basedprice);
                                                        if (USDPrice != 0) //If db not been store, then will follow the setup in item
                                                        {
                                                            CustomFieldRef[] cfrItemList = new CustomFieldRef[1];
                                                            DoubleCustomFieldRef refUSDPrice = new DoubleCustomFieldRef();
                                                            refUSDPrice.scriptId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_SCRIPTID;
                                                            refUSDPrice.internalId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_INTERNALID;
                                                            refUSDPrice.value = USDPrice;
                                                            cfrItemList[0] = refUSDPrice;
                                                            invi.customFieldList = cfrItemList;
                                                        }

                                                        invitems[a] = invi;

                                                    }
                                                }
                                                else
                                                {
                                                    //Double itemRate = 0;
                                                    String priceLevelInternalID = string.Empty;
                                                    //Double USDPrice = 0;
                                                    int invoiceQty = 0;

                                                    invoiceQty = 0;

                                                    invi.quantity = Convert.ToDouble(invoiceQty);
                                                    invi.quantitySpecified = true;
                                                    invitems[a] = invi;
                                                }

                                            }//end item loop
                                            inv.customFieldList = cfrList;
                                            invitemlist.item = invitems;
                                            inv.itemList = invitemlist;
                                            invList[invCount] = inv;
                                            rowCount = invCount + 1;
                                            status = true;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'UPD-STATUS.DROPSHIP INVOICE', 'DROPSHIPINVOICECREATION.DROPSHIPNO." + i.dsf_dropshipmono + '.' + i.dsf_dropshipmono_internalID + "." + i.dsf_GMYmono + "." + i.dsf_GMYmono_internalID + "'," +
                                                "'" + gjob_id.ToString() + "','START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_invProgress = '" + gjob_id.ToString() + "'" + ",dsf_lastinvoicedate = '" + createdDate + "' " +
                                                        ", dsf_invSeqNo = '" + rowCount + "'" +
                                                        "WHERE dsf_dropshipmono = '" + i.dsf_dropshipmono + "' " +
                                                        "AND dsf_iffUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                        "AND dsf_iffUpdatedDate <= '" + convertDateToString(rangeTo) + "' ";
                                            this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updDSF);
                                            entities.Database.ExecuteSqlCommand(updDSF);

                                            invCount++;

                                        }

                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.DataFromNetsuiteLog.Error("DropshipInvoiceCreation Exception: " + ex.ToString());
                                    status = false;
                                    if (rowCount == 0)
                                    {
                                        rowCount++;
                                    }
                                    break;
                                }
                            }

                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.addList(invList);
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug(updateTask);
                                        this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_invProgress = '" + jobID + "'" +
                                                    "WHERE dsf_invProgress = '" + gjob_id.ToString() + "' " +
                                                    "AND dsf_iffUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                    "AND dsf_iffUpdatedDate <= '" + convertDateToString(rangeTo) + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updDSF);
                                        entities.Database.ExecuteSqlCommand(updDSF);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("DropshipInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("DropshipInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("DropshipInvoiceCreation: Login Netsuite failed.");
                }
            }
            //logout();
            return status;
        }
        public Boolean DropshipBillOnlyInvoiceCreation(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("DropshipBillOnlyInvoiceCreation ***************");

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                //TBA
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("DropshipBillOnlyInvoiceCreation: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("DropshipBillOnlyInvoiceCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("DropshipBillOnlyInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        String subsidiarySEIS = "SEIS";
                        String subsidiaryTH = "GROLIER THAILAND";
                        String createdDate = convertDateToString(DateTime.Now);

                        var qFilterMono = (from q1 in entities.netsuite_dropshipfulfillment
                                           where (q1.dsf_lastfulfilleddate > rangeFrom && q1.dsf_lastfulfilleddate <= rangeTo)
                                           && (q1.dsf_invProgress == null || q1.dsf_invProgress == "")
                                           && q1.dsf_status == "CLOSED"
                                           select new
                                           {
                                               q1.dsf_dropshipmono_internalID,
                                               q1.dsf_dropshipmono,
                                               q1.dsf_GMYmono,
                                               q1.dsf_GMYmono_internalID,
                                               q1.dsf_customer_InternalID,
                                               q1.dsf_discount,
                                               q1.dsf_discountItem_internalID,
                                               q1.dsf_ponumber,
                                               q1.dsf_subsidiary,
                                               //q1.dsf_basedprice //#1050
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("DropshipBillOnlyInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];
                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                DateTime AutoBill_DeployedDate = Convert.ToDateTime("2015-04-01");
                                var dropshipmo = (from newso in entities.netsuite_newso
                                                  where newso.nt1_moNo_internalID == i.dsf_dropshipmono_internalID
                                                  && newso.nt1_createdDate >= AutoBill_DeployedDate
                                                  select new { newso.nt1_SEIS_moNo, newso.nt1_businessChannel_internalID, newso.nt1_subsidiary }).ToList().FirstOrDefault();

                                this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: Checked created date in newso for " + i.dsf_dropshipmono);

                                //To select the information for dropship SO 
                                var invoice = (from ds in entities.netsuite_dropshipfulfillment
                                               where (ds.dsf_lastfulfilleddate > rangeFrom && ds.dsf_lastfulfilleddate <= rangeTo)
                                               && (ds.dsf_invProgress == null || ds.dsf_invProgress == "")
                                               && ds.dsf_dropshipmono == i.dsf_dropshipmono
                                               && ds.dsf_status == "CLOSED"
                                               select new
                                               {
                                                   ds.dsf_itemID,
                                                   ds.dsf_item_internalID,
                                                   ds.dsf_rate,
                                                   ds.dsf_pricelevel_InternalID,
                                                   ds.dsf_pricelevel,
                                                   ds.dsf_ordQty,
                                                   ds.dsf_wmsfulfilledqty,
                                                   ds.dsf_createdinvoiceqty,
                                                   ds.dsf_basedprice //#1050
                                               }).ToList();

                                this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: Checked netsuite_dropshipfulfillment for " + i.dsf_dropshipmono);

                                if ((invoice.Count() > 0) && (dropshipmo != null))
                                {
                                    Invoice inv = new Invoice();
                                    CustomFieldRef[] cfrList = new CustomFieldRef[3];
                                    String dropshipLocation = string.Empty;
                                    String dropshipInvoiceForm = string.Empty;

                                    if (i.dsf_subsidiary == subsidiarySEIS)
                                    {
                                        dropshipLocation = @Resource.DROPSHIPSG_LOCATION_INTERNALID;
                                        dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPSG;
                                    }
                                    else
                                        if (i.dsf_subsidiary == subsidiaryTH)
                                        {
                                            dropshipLocation = @Resource.DROPSHIPTH_LOCATION_INTERNALID;
                                            dropshipInvoiceForm = @Resource.TRADE_INVOICE_CUSTOMFORM_DROPSHIPTH;
                                        }


                                    #region Main Information
                                    //Form 
                                    RecordRef refForm = new RecordRef();
                                    refForm.internalId = dropshipInvoiceForm;
                                    inv.customForm = refForm;

                                    //Customer 
                                    RecordRef refEntity = new RecordRef();
                                    refEntity.internalId = i.dsf_customer_InternalID;
                                    inv.entity = refEntity;

                                    //PO #
                                    inv.otherRefNum = i.dsf_ponumber;

                                    //Memo - Dropship SO Number
                                    inv.memo = i.dsf_GMYmono + "/" + i.dsf_dropshipmono;

                                    //Line of Business
                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = @Resource.LOB_TRADE_INTERNALID;
                                    if (dropshipmo.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                    {
                                        refClass.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;
                                    }
                                    // Added by Brash Developer on 21-Apr-2021 Start
                                    else if (dropshipmo.nt1_businessChannel_internalID == @Resource.LOB_ONLINESTORE_INTERNALID)
                                    {
                                        refClass.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;
                                    }
                                    //End
                                    inv.@class = refClass;

                                    //Location
                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = dropshipLocation;
                                    inv.location = refLocationSO;

                                    //Discount Plan 
                                    RecordRef refDiscPlan = new RecordRef();
                                    refDiscPlan.internalId = i.dsf_discountItem_internalID;
                                    inv.discountItem = refDiscPlan;

                                    //Dropship Sales Order Number
                                    StringCustomFieldRef scfrDropshipSONo = new StringCustomFieldRef();
                                    scfrDropshipSONo.scriptId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_SCRIPTID;
                                    scfrDropshipSONo.internalId = @Resource.CUSTOMFIELD_SEISSALESORDERNUMBER_INTERNALID;
                                    scfrDropshipSONo.value = i.dsf_dropshipmono;
                                    cfrList[0] = scfrDropshipSONo;

                                    //Dropship Sales Order Number Internal ID
                                    StringCustomFieldRef scfrDropshipSONoInternalID = new StringCustomFieldRef();
                                    scfrDropshipSONoInternalID.scriptId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_SCRIPTID;
                                    scfrDropshipSONoInternalID.internalId = @Resource.CUSTOMFIELD_DROPSHIPSOINTERNALID_INTERNALID;
                                    scfrDropshipSONoInternalID.value = i.dsf_dropshipmono_internalID;
                                    cfrList[1] = scfrDropshipSONoInternalID;

                                    //SOR Tag
                                    //if (dropshipmo.nt1_subsidiary == subsidiarySEIS && dropshipmo.nt1_businessChannel_internalID == @Resource.LOB_EDUCATIONGENERAL_INTERNALID)
                                    //{
                                    //    BooleanCustomFieldRef sorTag = new BooleanCustomFieldRef();
                                    //    sorTag.scriptId = @Resource.CUSTBODY_SORTAG_SCRIPTID;
                                    //    sorTag.value = true;
                                    //    cfrList[2] = sorTag;
                                    //} 
                                    #endregion
                                    #region Items Information
                                    this.DataFromNetsuiteLog.Info("DropshipBillOnlyInvoiceCreation: Assign Sales Order Items Information");

                                    InvoiceItem[] invii = new InvoiceItem[invoice.Count()];
                                    InvoiceItemList invil = new InvoiceItemList();

                                    int itemCount = 0;
                                    foreach (var item in invoice)
                                    {
                                        InvoiceItem invi = new InvoiceItem();
                                        Double itemRate = 0;
                                        //Double grossAmount = 0;
                                        String priceLevelInternalID = string.Empty;
                                        Int32 invoiceQty = 0;
                                        Boolean isValid = true;
                                        Double USDPrice = 0;

                                        if (item.dsf_wmsfulfilledqty > item.dsf_ordQty)
                                        {
                                            isValid = false;
                                            this.DataFromNetsuiteLog.Fatal("This Sales Order : " + i.dsf_dropshipmono + "/" + i.dsf_GMYmono + ",Item ISBN : " + item.dsf_itemID + " having OrdQty (" + item.dsf_ordQty + ") > FulfilledQty (" + item.dsf_wmsfulfilledqty + "), Rate : " + item.dsf_rate);
                                        }
                                        else
                                        {
                                            invoiceQty = Convert.ToInt32(item.dsf_wmsfulfilledqty);
                                        }

                                        if (isValid == true && invoiceQty > 0)
                                        {
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.dsf_item_internalID;
                                            invi.item = refItem;

                                            invi.quantity = Convert.ToDouble(invoiceQty);
                                            invi.quantitySpecified = true;

                                            priceLevelInternalID = item.dsf_pricelevel_InternalID;
                                            itemRate = Convert.ToDouble(item.dsf_rate);

                                            //Commented by WY - 23.Nov.2015 - Netsuite Trx Form was fixed.
                                            //if (i.dsf_subsidiary == subsidiaryTH)//Only TH need to set grossAmt because NS trx form calculation not working
                                            //{
                                            //    grossAmount = itemRate * Convert.ToDouble(invoiceQty);
                                            //    invi.grossAmt = grossAmount;
                                            //    invi.grossAmtSpecified = true;
                                            //}

                                            priceLevelInternalID = "-1"; //To set all the price level to Custom - WY - 09.DEC.2015

                                            RecordRef refPriceLevel = new RecordRef();
                                            refPriceLevel.internalId = priceLevelInternalID;
                                            invi.price = refPriceLevel;

                                            if (priceLevelInternalID == "-1")//Custom 
                                            {
                                                invi.rate = Convert.ToString(itemRate);
                                            }

                                            //#1050 - Set price for USD  
                                            USDPrice = Convert.ToDouble(item.dsf_basedprice);
                                            if (USDPrice != 0 && USDPrice != null) //If db not been store, then will follow the setup in item
                                            {
                                                CustomFieldRef[] cfrItemList = new CustomFieldRef[1];
                                                DoubleCustomFieldRef refUSDPrice = new DoubleCustomFieldRef();
                                                refUSDPrice.scriptId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_SCRIPTID;
                                                refUSDPrice.internalId = @Resource.CUSTOMFIELD_ITEMUSDPRICE_INTERNALID;
                                                refUSDPrice.value = USDPrice;
                                                cfrItemList[0] = refUSDPrice;
                                                invi.customFieldList = cfrItemList;
                                            }

                                            invi.orderLine = itemCount + 1;
                                            invi.orderLineSpecified = true;

                                            invii[itemCount] = invi;
                                            itemCount++;

                                        }
                                    }
                                    #endregion

                                    if (itemCount > 0)
                                    {
                                        inv.customFieldList = cfrList;
                                        invil.item = invii;
                                        inv.itemList = invil;
                                        invList[invCount] = inv;
                                        rowCount = invCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'UPD-STATUS.DROPSHIP BOINVOICE', 'DROPSHIPINVOICECREATION.DROPSHIPNO." + i.dsf_dropshipmono + '.' + i.dsf_dropshipmono_internalID + "." + i.dsf_GMYmono + "." + i.dsf_GMYmono_internalID + "'," +
                                            "'" + gjob_id.ToString() + "','START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_invProgress = '" + gjob_id.ToString() + "'" + ",dsf_lastinvoicedate = '" + createdDate + "' " +
                                                    ", dsf_invSeqNo = '" + rowCount + "'" +
                                                    "WHERE dsf_dropshipmono = '" + i.dsf_dropshipmono + "' " +
                                                    "AND dsf_lastfulfilleddate > '" + convertDateToString(rangeFrom) + "' " +
                                                    "AND dsf_lastfulfilleddate <= '" + convertDateToString(rangeTo) + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updDSF);
                                        entities.Database.ExecuteSqlCommand(updDSF);


                                        invCount++;
                                    }
                                    status = true;
                                }
                                else
                                {
                                    if (dropshipmo == null)
                                    {
                                        var insertItem = "insert into unfulfillso (uf_transactiontype, uf_mono, uf_itemInternalID, uf_fulfillQty, uf_rangeFrom, uf_rangeTo, uf_createdDate, uf_remarks) " +
                                                         " values ('" + transactionType + "','" + i.dsf_dropshipmono + "', 'whole inv', '', " +
                                                         " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "', 'The original so may created before apr 2015') ";
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + insertItem);
                                        entities.Database.ExecuteSqlCommand(insertItem);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("DropshipBillOnlyInvoiceCreation Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.addList(invList);
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug(updateTask);
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updDSF = "UPDATE netsuite_dropshipfulfillment SET dsf_invProgress = '" + jobID + "'" +
                                                    "WHERE dsf_invProgress = '" + gjob_id.ToString() + "' " +
                                                    "AND dsf_lastfulfilleddate > '" + convertDateToString(rangeFrom) + "' " +
                                                    "AND dsf_lastfulfilleddate <= '" + convertDateToString(rangeTo) + "' ";
                                        this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updDSF);
                                        entities.Database.ExecuteSqlCommand(updDSF);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("DropshipBillOnlyInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("DropshipBillOnlyInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("DropshipBillOnlyInvoiceCreation: Login Netsuite failed.");
                }
            }
            //logout();
            return status;
        }
        //GMY Invoice Creation
        public Boolean GMYInvoiceCreation(Int32 rn_id, String transactionType, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("GMYInvoiceCreation ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                string loginEmail = "";
                loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };
                Boolean loginStatus = false;
                netsuiteService.Timeout = 820000000;
                netsuiteService.CookieContainer = new CookieContainer();
                ApplicationInfo appinfo = new ApplicationInfo();
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Info("GMYInvoiceCreation: Login Netsuite success.");
                        loginStatus = true;
                    }
                    else
                    {
                        loginStatus = false;
                    }
                }
                catch (Exception ex)
                {
                    loginStatus = false;
                    this.DataFromNetsuiteLog.Fatal("GMYInvoiceCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }
                if (loginStatus == true)
                {

                    this.DataFromNetsuiteLog.Info("GMYInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qFilterMono = (from q1 in entities.wms_jobordscan
                                           join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                                           where (q1.jos_businessChannel_code == "ET" || q1.jos_businessChannel_code == "BC")
                                         && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                         && q1.jos_netsuiteProgress != null
                                         && q1.jos_GMYInvoiceProgress == null
                                           select new
                                           {
                                               q1.jos_moNo,
                                               q2.nsjm_moNo_internalID,
                                               //isFirstRun = q1.jos_GMYInvoiceProgress == null ? "Y" : "N",
                                               //q1.jos_job_ID //Item List - Loop for each item to handling advanced billing
                                           })
                                      .Distinct()
                                      .ToList();

                        //#region Item List - Loop for each item to handling advanced billing
                        //List<string> _IDjob = new List<string>();
                        //foreach (var qJobID in qListMono)
                        //{
                        //    if (qJobID.isFirstRun == "Y")
                        //    {
                        //        _IDjob.Add(qJobID.jos_job_ID);
                        //    }
                        //}
                        //#endregion

                        //var qFilterMono = (from d in qListMono
                        //                   where d.isFirstRun == "Y"
                        //                   select new { d.jos_moNo, d.nsjm_moNo_internalID }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("GMYInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                #region To check if wmsfulfilledqty is same as netsuite fulfilled qty then only can proceed for billing
                                Boolean isEqual = true;
                                var qNewSO = (from newso in entities.netsuite_newso
                                              where newso.nt1_moNo == i.jos_moNo
                                              select newso.nt1_fulfilledQty).Sum();

                                var qSyncSO = (from syncso in entities.netsuite_syncso
                                               where syncso.nt2_moNo == i.jos_moNo
                                               select syncso.nt2_wmsfulfilledqty).Sum();

                                if (Convert.ToInt32(qNewSO) < Convert.ToInt32(qSyncSO))
                                {
                                    isEqual = false;

                                    var insertItem = "insert into unfulfillso (uf_transactiontype,uf_mono,uf_itemInternalID,uf_fulfillQty,uf_rangeFrom,uf_rangeTo,uf_createdDate,uf_remarks) " +
                                                     " values ('" + transactionType + "','" + i.jos_moNo + "', '" + Convert.ToString(qNewSO) + "','" + Convert.ToString(qSyncSO) + "', " +
                                                     " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "','Netsuite Fulfilled Qty Less Than Warehouse Fulfilled Qty') ";
                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertItem);
                                    entities.Database.ExecuteSqlCommand(insertItem);
                                }
                                #endregion

                                if (isEqual == true)
                                {
                                    InitializeRef refSO = new InitializeRef();
                                    refSO.type = InitializeRefType.salesOrder;
                                    refSO.internalId = i.nsjm_moNo_internalID;
                                    refSO.typeSpecified = true;

                                    InitializeRecord recSO = new InitializeRecord();
                                    recSO.type = InitializeType.invoice;
                                    recSO.reference = refSO;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    ReadResponse rrSO = netsuiteService.initialize(recSO);
                                    Record rSO = rrSO.record;
                                    //CustomFieldRef[] cfrList = new CustomFieldRef[1];

                                    Invoice inv2 = (Invoice)rSO;
                                    Invoice inv = new Invoice();

                                    if (inv2 != null)
                                    {
                                        #region Main Information
                                        //Form 
                                        RecordRef refForm = new RecordRef();
                                        refForm.internalId = @Resource.TRADE_INVOICE_CUSTOMFORM_GMY;
                                        inv.customForm = refForm;

                                        //createdfrom 
                                        RecordRef refCreatedFrom = new RecordRef();
                                        refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                        inv.createdFrom = refCreatedFrom;

                                        //Customer 
                                        RecordRef refEntity = new RecordRef();
                                        refEntity.internalId = inv2.entity.internalId;
                                        inv.entity = refEntity;

                                        //PO #
                                        inv.otherRefNum = inv2.otherRefNum;

                                        //Memo - Doesn't need to copy the memo,just set it blank, it will affect and show in SOA.
                                        //inv.memo = inv2.memo;
                                        inv.memo = "";

                                        ////Line of Business
                                        //RecordRef refClass = new RecordRef();
                                        //refClass.internalId = @Resource.LOB_TRADE_INTERNALID;
                                        //inv.@class = refClass;

                                        //Location
                                        RecordRef refLocationSO = new RecordRef();
                                        refLocationSO.internalId = @Resource.TRADE_EXCESSFULFILLMENTLOCATION_INTERNALID;
                                        inv.location = refLocationSO;

                                        ////SOR Start Date
                                        //inv.tranDate = Convert.ToDateTime("2015-04-01").AddDays(1);
                                        //inv.tranDateSpecified = true;

                                        //BooleanCustomFieldRef sorTag = new BooleanCustomFieldRef();
                                        //sorTag.scriptId = @Resource.CUSTBODY_SORTAG_SCRIPTID; 
                                        //sorTag.value = true; 
                                        //cfrList[0] = sorTag;

                                        //DateTime SORStart = Convert.ToDateTime("2015-04-01").AddDays(1);
                                        //DateCustomFieldRef custSORStartDate = new DateCustomFieldRef();
                                        //custSORStartDate.scriptId = @Resource.CUSTBODY_SORSTART_SCRIPTID;
                                        //custSORStartDate.value = SORStart;
                                        //cfrList[1] = custSORStartDate;

                                        ////SOR End Date
                                        //DateTime SOREnd = Convert.ToDateTime("2015-04-01").AddDays(10);
                                        //DateCustomFieldRef custSOREndDate = new DateCustomFieldRef();
                                        //custSOREndDate.scriptId = @Resource.CUSTBODY_SOREND_SCRIPTID;
                                        //custSOREndDate.value = SOREnd;
                                        //cfrList[2] = custSOREndDate;
                                        #endregion

                                        InvoiceItemList invitemlist = inv2.itemList;

                                        var query2 = (from josp in entities.wms_jobordscan_pack
                                                      join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                                      join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                                      join jos in entities.wms_jobordscan on new { a = josp.josp_jobID, b = josp.josp_moNo } equals new { a = jos.jos_job_ID, b = jos.jos_moNo }
                                                      where josp.josp_moNo == i.jos_moNo
                                                      && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                                      && jos.jos_GMYInvoiceProgress == null
                                                      && jos.jos_netsuiteProgress != null
                                                      select new
                                                      {
                                                          josp.josp_pack_ID,
                                                          jompd.nsjompd_item_internalID,
                                                          jomp.nsjomp_jobOrdMaster_ID,
                                                          qty = josp.josp_ordFulFill
                                                      }).ToList();

                                        var groupQ2 = from p in query2
                                                      let k = new
                                                      {
                                                          itemInternalID = p.nsjompd_item_internalID,
                                                      }
                                                      group p by k into g
                                                      select new
                                                      {
                                                          itemInternalID = g.Key.itemInternalID,
                                                          fulFillQty = g.Sum(p => p.qty)
                                                      };

                                        List<String> deCommitItem = new List<String>();
                                        List<Int32> deCommitQty = new List<Int32>();
                                        Hashtable htDBItems = new Hashtable();
                                        foreach (var item in groupQ2)
                                        {
                                            deCommitItem.Add(item.itemInternalID);
                                            deCommitQty.Add(Convert.ToInt32(item.fulFillQty));
                                            htDBItems.Add(item.itemInternalID, Convert.ToInt32(item.fulFillQty));
                                        }

                                        if (inv2.itemList != null)
                                        {
                                            #region Item List - Loop for each item to handling advanced billing
                                            InvoiceItem[] invitems = new InvoiceItem[invitemlist.item.Length];

                                            for (int a = 0; a < invitemlist.item.Length; a++)
                                            {
                                                InvoiceItem invi = new InvoiceItem();
                                                Boolean isExist = false;
                                                double oriInvQty = invitemlist.item[a].quantity;

                                                invi.item = invitemlist.item[a].item;
                                                invi.quantity = 0;
                                                invi.quantitySpecified = true;

                                                invi.orderLine = invitemlist.item[a].orderLine;
                                                invi.orderLineSpecified = true;

                                                isExist = htDBItems.Contains(invitemlist.item[a].item.internalId);
                                                if (isExist)
                                                {
                                                    int fulfilQty = (int)htDBItems[invitemlist.item[a].item.internalId];
                                                    fulfilQty = fulfilQty - Convert.ToInt32(invitemlist.item[a].quantityRemaining);

                                                    htDBItems.Remove(invitemlist.item[a].item.internalId);
                                                    htDBItems.Add(invitemlist.item[a].item.internalId, fulfilQty);
                                                }

                                                for (int j = 0; j < deCommitItem.Count(); j++)
                                                {
                                                    if (invitemlist.item[a].item.internalId.Equals(deCommitItem[j]))
                                                    {
                                                        if (oriInvQty <= Convert.ToInt32(deCommitQty[j]))
                                                        {
                                                            invi.quantity = oriInvQty;
                                                        }
                                                        else
                                                        {
                                                            invi.quantity = Convert.ToInt32(deCommitQty[j]);
                                                        }
                                                        int leftOutQty = Convert.ToInt32(deCommitQty[j]) - Convert.ToInt32(oriInvQty);
                                                        if (leftOutQty <= 0)
                                                        {
                                                            leftOutQty = 0;
                                                        }
                                                        deCommitQty[j] = leftOutQty;
                                                        invi.quantitySpecified = true;
                                                        break;
                                                    }
                                                }
                                                invitems[a] = invi;
                                            }

                                            for (int a = 0; a < deCommitQty.Count(); a++)
                                            {
                                                if (Convert.ToInt32(deCommitQty[a]) > 0)
                                                {
                                                    var invleftOutQty = "insert into unfulfillso (uf_transactiontype,uf_mono,uf_itemInternalID,uf_fulfillQty,uf_rangeFrom,uf_rangeTo,uf_createdDate,uf_remarks) " +
                                                     " values ('" + transactionType + "','" + i.jos_moNo + "', '" + deCommitItem[a] + "','" + deCommitQty[a] + "', " +
                                                     " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "','Invoice Left Out Qty') ";
                                                    this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + invleftOutQty);
                                                    entities.Database.ExecuteSqlCommand(invleftOutQty);
                                                }
                                            }
                                            #endregion

                                            InvoiceItemList invil1 = new InvoiceItemList();
                                            invil1.replaceAll = false;
                                            invil1.item = invitems;
                                            inv.itemList = invil1;

                                            //inv.customFieldList = cfrList;// WY-19.JUNE.2015
                                            invList[invCount] = inv;
                                            rowCount = invCount + 1;

                                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'UPD-STATUS.GMY INVOICE', 'GMYINVOICECREATION.GMYNO." + i.jos_moNo + '.' + i.nsjm_moNo_internalID + "', '" + gjob_id.ToString() + "'," +
                                                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                            this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + insertTask);
                                            entities.Database.ExecuteSqlCommand(insertTask);

                                            var updJobOrdScan = "UPDATE wms_jobordscan SET jos_GMYInvoiceProgress = '" + gjob_id.ToString() + "' WHERE jos_GMYInvoiceProgress is null " +
                                                                "and jos_moNo = '" + i.jos_moNo + "' " +
                                                                "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                                "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";
                                            this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updJobOrdScan);
                                            entities.Database.ExecuteSqlCommand(updJobOrdScan);

                                            invCount++;
                                            status = true;
                                        }
                                    }
                                    else
                                    {
                                        var updJobOrdScan = "UPDATE wms_jobordscan SET jos_GMYInvoiceProgress = 'NO RECORD FOUND' WHERE jos_GMYInvoiceProgress is null " +
                                                            "and jos_moNo = '" + i.jos_moNo + "' " +
                                                            "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updJobOrdScan);
                                        entities.Database.ExecuteSqlCommand(updJobOrdScan);
                                    }
                                }

                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("GMYInvoiceCreation Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }

                        try
                        {
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res;
                                    //res = service.addList(invList);
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("GMYInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("GMYInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("GMYInvoiceCreation: Login Netsuite failed.");
                }
            }
            //logout();
            return status;
        }
        #endregion
        //#region Deduct Quantity for backorder case - WY-15.JULY.2015 
        //public Boolean DeductQtyForCancelItem(DateTime rangeFrom, DateTime rangeTo)
        //{
        //    this.DataFromNetsuiteLog.Info("DeductQtyForCancelItem ***************");
        //    Boolean status = false;
        //    var option = new TransactionOptions
        //    {
        //        IsolationLevel = IsolationLevel.RepeatableRead,
        //        Timeout = TimeSpan.FromSeconds(2400)
        //    };

        //    using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
        //    {
        //        try
        //        {
        //            Boolean loginStatus = login();
        //            if (loginStatus == true)
        //            {
        //                this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: Login Netsuite success.");
        //                using (sdeEntities entities = new sdeEntities())
        //                {
        //                    AsyncStatusResult job = new AsyncStatusResult();
        //                    Int32 rowCount = 0;
        //                    Guid gjob_id = Guid.NewGuid();
        //                    List<string> _DataExtracted = new List<string>();

        //                    #region Get List of SO to update from backorder_cancellation
        //                    var queryA = (from q1 in entities.backorder_cancellation
        //                                  join q2 in entities.netsuite_newso on q1.boc_moNo equals q2.nt1_moNo
        //                                  where (q1.boc_createdDate > rangeFrom && q1.boc_createdDate <= rangeTo) 
        //                                 select new
        //                                  {
        //                                      q1.boc_moNo,
        //                                      q2.nt1_moNo_internalID,
        //                                      isFirstRun = q1.boc_netsuiteJob == null ? "Y" : "N" 
        //                                  }).Distinct().ToList();

        //                    var qCancelMoNo = (from d in queryA
        //                                      where d.isFirstRun == "Y"
        //                                     select new { d.boc_moNo, d.nt1_moNo_internalID }).Distinct().ToList();
        //                    #endregion
        //                    SalesOrder[] soList = new SalesOrder[qCancelMoNo.Count];
        //                    foreach (var q1 in qCancelMoNo)
        //                    {
        //                        try
        //                        {
        //                            #region Variables Declaration 
        //                            SalesOrder decommitSO = new SalesOrder();
        //                            List<String> deCommitItem = new List<String>();
        //                            List<Int32> deCommitQty = new List<Int32>(); 
        //                            #endregion

        //                            #region Search Sales Order
        //                            RecordRef refMoNo = new RecordRef();
        //                            refMoNo.internalId = q1.nt1_moNo_internalID;

        //                            SearchPreferences sp = new SearchPreferences();
        //                            sp.bodyFieldsOnly = false;
        //                            service.searchPreferences = sp; 

        //                            TransactionSearchAdvanced sotsa = new TransactionSearchAdvanced();
        //                            TransactionSearch sots = new TransactionSearch();
        //                            TransactionSearchBasic sotsb = new TransactionSearchBasic();

        //                            SearchMultiSelectField cancelSO = new SearchMultiSelectField();
        //                            cancelSO.@operator = SearchMultiSelectFieldOperator.anyOf;
        //                            cancelSO.operatorSpecified = true;
        //                            cancelSO.searchValue = new RecordRef[] { refMoNo };
        //                            sotsb.internalId = cancelSO;

        //                            sots.basic = sotsb;
        //                            sotsa.criteria = sots;
        //                            SearchResult sr = service.search(sotsa);
        //                            Record[] srRecord = sr.recordList;
        //                            #endregion

        //                            #region Assign cancel item and qty to list 
        //                            for (int i = 0; i < srRecord.Count(); i++)
        //                            {
        //                                SalesOrder so = (SalesOrder)srRecord[i];
        //                                decommitSO.itemList = so.itemList;

        //                                var queryB = (from q2 in entities.backorder_cancellation
        //                                              where (q2.boc_createdDate > rangeFrom && q2.boc_createdDate <= rangeTo)
        //                                              && q2.boc_moNo == q1.boc_moNo 
        //                                              select new
        //                                              {
        //                                                  q2.boc_isbn,
        //                                                  q2.boc_quantity  
        //                                              }).ToList();

        //                                var groupQ2 = from p in queryB
        //                                              let k = new
        //                                              {
        //                                                  itemISBN = p.boc_isbn
        //                                              }
        //                                              group p by k into g
        //                                              select new
        //                                              {
        //                                                  item = g.Key.itemISBN,
        //                                                  cancelQty = g.Sum(p => p.boc_quantity)
        //                                              };

        //                                foreach (var q2 in groupQ2)
        //                                {
        //                                    for (int j = 0; j < so.itemList.item.Count(); j++)
        //                                    {
        //                                        if (!String.IsNullOrEmpty(q2.item) && q2.item.Equals(so.itemList.item[j].item.internalId))
        //                                        {
        //                                            Int32 updDummyQty = 0;
        //                                            updDummyQty = Convert.ToInt32(so.itemList.item[j].quantity) - Convert.ToInt32(q2.cancelQty);
        //                                            if (updDummyQty < 0)
        //                                            {
        //                                                updDummyQty = 0;
        //                                            }
        //                                            deCommitItem.Add(so.itemList.item[j].item.internalId);
        //                                            deCommitQty.Add(updDummyQty);
        //                                        }
        //                                        else
        //                                        { 
        //                                            deCommitItem.Add(so.itemList.item[j].item.internalId);
        //                                            deCommitQty.Add(Convert.ToInt32(so.itemList.item[j].quantity));
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            #endregion

        //                            #region Decommit item

        //                            if (deCommitItem.Count() > 0)
        //                            {
        //                                decommitSO.internalId = q1.nt1_moNo_internalID;

        //                                SalesOrderItem[] soii = new SalesOrderItem[decommitSO.itemList.item.Count()];
        //                                SalesOrderItemList soil = new SalesOrderItemList();

        //                                for (int j = 0; j < decommitSO.itemList.item.Count(); j++)
        //                                {
        //                                    SalesOrderItem soi = new SalesOrderItem();
        //                                    RecordRef refItem = new RecordRef();
        //                                    refItem.internalId = decommitSO.itemList.item[j].item.internalId;
        //                                    soi.item = refItem;

        //                                    soi.quantity = decommitSO.itemList.item[j].quantity;
        //                                    soi.quantitySpecified = true;

        //                                    soi.amount = 0;
        //                                    soi.amountSpecified = true;

        //                                    soi.createPoSpecified = false;

        //                                    for (int i = 0; i < deCommitItem.Count(); i++)
        //                                    {
        //                                        if (deCommitItem[i].Equals(decommitSO.itemList.item[j].item.internalId))
        //                                        {
        //                                            soi.quantity = deCommitQty[i];
        //                                            soi.quantitySpecified = true;
        //                                            break;
        //                                        }
        //                                    } 
        //                                    soii[j] = soi; 
        //                                }

        //                                //soil.replaceAll = true;    //for remove all items
        //                                soil.item = soii;
        //                                decommitSO.itemList = soil;

        //                                Int32 soCount = 0;
        //                                soList[soCount] = decommitSO; 
        //                                rowCount = soCount + 1;

        //                                String refNo = "JOBORDSCAN.JOS_JOB_ID.ALL" + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + ".ALL";
        //                                var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
        //                                    "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'UPD-STATUS.UPD BACKORDER QTY','" + refNo + "','" + gjob_id.ToString() + "'," +
        //                                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + q1.nt1_moNo_internalID + "', '')"; 
        //                                this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + insertTask);
        //                                entities.Database.ExecuteSqlCommand(insertTask);

        //                                var updateTask = "update backorder_cancellation set boc_netsuiteJob = '" + gjob_id.ToString() + "' where boc_netsuiteJob is null " +
        //                                                 "and boc_createdDate > '" + convertDateToString(rangeFrom) + "' " +
        //                                                 "and boc_createdDate <= '" + convertDateToString(rangeTo) + "'  ";

        //                                this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + updateTask);
        //                                entities.Database.ExecuteSqlCommand(updateTask);

        //                                soCount++;
        //                                status = true;
        //                            }
        //                            #endregion

        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            this.DataFromNetsuiteLog.Error("DeductQtyForCancelItem Exception: " + ex.ToString());
        //                            status = false; 
        //                        }
        //                    }

        //                    try
        //                    {
        //                        if (status == true)
        //                        {
        //                            if (rowCount > 0)
        //                            {
        //                                job = service.asyncUpdateList(soList);
        //                                String jobID = job.jobId;

        //                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
        //                                this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + updateTask);
        //                                entities.Database.ExecuteSqlCommand(updateTask);

        //                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
        //                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.UPD BACKORDER QTY' " +
        //                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'"; 
        //                                this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + updateRequestNetsuite);
        //                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

        //                                //var updBOCancel = "update backorder_cancellation set boc_netsuiteJob = '" + gjob_id.ToString() + "' " +
        //                                //                   "where boc_netsuiteJob = '" + gjob_id.ToString() + "' " +
        //                                ////this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + updBOCancel);
        //                                //entities.Database.ExecuteSqlCommand(updBOCancel);

        //                            }
        //                        }
        //                        else if (rowCount == 0)
        //                        {
        //                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
        //                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.UPD BACKORDER QTY' " +
        //                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
        //                            this.DataFromNetsuiteLog.Debug("DeductQtyForCancelItem: " + updateRequestNetsuite);
        //                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
        //                        }
        //                        scope1.Complete();
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        this.DataFromNetsuiteLog.Error("DeductQtyForCancelItem Exception: " + ex.ToString());
        //                        status = false;
        //                    }
        //                }
        //                logout();
        //            }
        //            else
        //            {
        //                this.DataFromNetsuiteLog.Fatal("DeductQtyForCancelItem: Login Netsuite failed.");
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            this.DataFromNetsuiteLog.Error("DeductQtyForCancelItem Exception: " + ex.ToString());
        //            status = false;
        //        }
        //    }//end of scope1
        //    return status;
        //}
        //#endregion
        /*
        public Boolean SODoNotCommitUpdate(DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("SODoNotCommitUpdate ***************");
            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("Login Netsuite");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        //Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var requestnetsuitetask = (from r in entities.requestnetsuite_task
                                                   where r.rnt_status == "TRUE" && r.rnt_description == "SSA-FULFILLMENT" && r.rnt_task == "ADD"
                                                   select r).ToList();

                        this.DataFromNetsuiteLog.Info(requestnetsuitetask.Count() + " records to update.");

                        SalesOrder[] soList = new SalesOrder[requestnetsuitetask.Count()];

                        foreach (var t in requestnetsuitetask)
                        {
                            try
                            {
                                SalesOrder so = new SalesOrder();
                                so.internalId = t.rnt_createdFromInternalId;
                                //RecordRef refClass = new RecordRef();
                                //refClass.internalId = "4";
                                //so.@class = refClass;

                                var orderItem = (from jom in entities.netsuite_jobordmaster
                                                 join jomp in entities.netsuite_jobordmaster_pack on jom.nsjom_jobOrdMaster_ID equals jomp.nsjomp_jobOrdMaster_ID
                                                 where jom.nsjom_moNo_internalID == "7218"//t.rnt_createdFromInternalID
                                                 select jomp).ToList();

                                if (orderItem.Count() > 0)
                                {
                                    this.DataFromNetsuiteLog.Info(orderItem.Count() + " records to update.");

                                    SalesOrderItem[] soii = new SalesOrderItem[orderItem.Count()];
                                    SalesOrderItemList soil = new SalesOrderItemList();

                                    foreach (var item in orderItem)
                                    {
                                        Int32 itemCount = 0;
                                        SalesOrderItem soi = new SalesOrderItem();
                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = t.rnt_createdFromInternalId;
                                        soi.item = refItem;
                                        //soi.amount = 0;
                                        soi.amountSpecified = false;
                                        soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                        soi.commitInventorySpecified = true;
                                        soi.createPoSpecified = false;
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
                                this.DataFromNetsuiteLog.Error(ex.ToString());
                                status = false;
                            }
                        }

                        if (status == true)
                        {
                            //WriteResponse[] res = service.updateList(soList);
                            //status = true;

                            job = service.asyncUpdateList(soList);
                            String jobID = job.jobId;

                            var updateTask = "update requestnetsuite_task set " +
                                "rnt_jobID = '" + jobID + "' where " +
                                "rnt_jobID = '" + gjob_id.ToString() + "'";
                            this.DataFromNetsuiteLog.Debug(updateTask);
                            entities.Database.ExecuteSqlCommand(updateTask);
                            scope1.Complete();
                        }
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("Login Netsuite failed.");
                }
            }
            logout();
            return status;
        }
        */
        public Boolean SOStatusUpdate(List<String> updateSOList)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            this.DataFromNetsuiteLog.Info("SOStatusUpdate ***************");
            Boolean status = false;
            //TBA
            string loginEmail = "";
            loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
            tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
            tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

            ItemSearchBasic basic = new ItemSearchBasic()
            {
                internalId = new SearchMultiSelectField()
                {
                    @operator = SearchMultiSelectFieldOperator.anyOf,
                    operatorSpecified = true,
                    searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                }
            };
            Boolean loginStatus = false;
            netsuiteService.Timeout = 820000000;
            netsuiteService.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            //  appinfo.applicationId = appID;
            netsuiteService.applicationInfo = appinfo;
            try
            {
                Console.WriteLine("Success");
                netsuiteService.tokenPassport = createTokenPassport();
                SearchResult status1 = netsuiteService.search(basic);
                if (status1.status.isSuccess == true)
                {
                    this.DataFromNetsuiteLog.Info("SOStatusUpdate: Login Netsuite success");
                    loginStatus = true;
                }
                else
                {
                    loginStatus = false;
                }
            }
            catch (Exception ex)
            {
                loginStatus = false;
                this.DataFromNetsuiteLog.Fatal("SOStatusUpdate: Login Netsuite failed. Exception : " + ex.ToString());

            }


            if (loginStatus == true)
            {
                this.DataFromNetsuiteLog.Info("SOStatusUpdate: Login Netsuite success");
                using (sdeEntities entities = new sdeEntities())
                {
                    try
                    {
                        AsyncStatusResult job = new AsyncStatusResult();

                        this.DataFromNetsuiteLog.Info("SOStatusUpdate: " + updateSOList.Count() + " records to update.");

                        SalesOrder[] soList = new SalesOrder[updateSOList.Count()];

                        for (int i = 0; i < updateSOList.Count(); i++)
                        {
                            SalesOrder so = new SalesOrder();
                            so.internalId = updateSOList[i];

                            CustomFieldRef[] cfrList = new CustomFieldRef[1];
                            StringCustomFieldRef scfr = new StringCustomFieldRef();
                            scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                            scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                            scfr.value = "2";
                            cfrList[0] = scfr;

                            so.customFieldList = cfrList;

                            soList[i] = so;
                        }
                        //TBA
                        netsuiteService.tokenPassport = createTokenPassport();
                        WriteResponseList resList = netsuiteService.addList(soList);
                        WriteResponse[] res = resList.writeResponse;
                        /*job = service.asyncUpdateList(poList);
                        String jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set " +
                            "rnt_jobID = '" + jobID + "' where " +
                            "rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug(updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                        scope1.Complete();*/
                    }
                    catch (Exception ex)
                    {
                        this.DataFromNetsuiteLog.Error("SOStatusUpdate Exception: " + ex.ToString());
                    }
                }
            }
            return status;
        }
        public Boolean POStatusUpdate(List<String> updatePOList)
        {
            this.DataFromNetsuiteLog.Info("POStatusUpdate ***************");
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            //updatePOList.Add("31386");
            //updatePOList.Add("31387");
            //TBA
            string loginEmail = "";
            loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
            tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
            tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;

            ItemSearchBasic basic = new ItemSearchBasic()
            {
                internalId = new SearchMultiSelectField()
                {
                    @operator = SearchMultiSelectFieldOperator.anyOf,
                    operatorSpecified = true,
                    searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                }
            };
            Boolean loginStatus = false;
            netsuiteService.Timeout = 820000000;
            netsuiteService.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            //  appinfo.applicationId = appID;
            netsuiteService.applicationInfo = appinfo;

            try
            {
                Console.WriteLine("Success");
                netsuiteService.tokenPassport = createTokenPassport();
                SearchResult status1 = netsuiteService.search(basic);
                if (status1.status.isSuccess == true)
                {
                    this.DataFromNetsuiteLog.Info("POStatusUpdate: Login Netsuite success.");
                    loginStatus = true;
                }
                else
                {
                    loginStatus = false;
                }
            }
            catch (Exception ex)
            {
                loginStatus = false;
                this.DataFromNetsuiteLog.Fatal("POStatusUpdate: Login Netsuite failed. Exception : " + ex.ToString());

            }
            //

            if (loginStatus == true)
            {
                this.DataFromNetsuiteLog.Info("POStatusUpdate: Login Netsuite success.");
                using (sdeEntities entities = new sdeEntities())
                {
                    try
                    {
                        AsyncStatusResult job = new AsyncStatusResult();

                        this.DataFromNetsuiteLog.Info("POStatusUpdate: " + updatePOList.Count() + " records to update.");

                        PurchaseOrder[] poList = new PurchaseOrder[updatePOList.Count()];

                        for (int i = 0; i < updatePOList.Count(); i++)
                        {
                            PurchaseOrder po = new PurchaseOrder();
                            po.internalId = updatePOList[i];

                            CustomFieldRef[] cfrList = new CustomFieldRef[1];
                            StringCustomFieldRef scfr = new StringCustomFieldRef();
                            scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                            scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                            scfr.value = "2";
                            cfrList[0] = scfr;

                            po.customFieldList = cfrList;

                            poList[i] = po;
                        }
                        //TBA
                        netsuiteService.tokenPassport = createTokenPassport();
                        WriteResponse[] res = netsuiteService.updateList(poList).writeResponse;
                        /*job = service.asyncUpdateList(poList);
                        String jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set " +
                            "rnt_jobID = '" + jobID + "' where " +
                            "rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug(updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);
                        scope1.Complete();*/
                    }
                    catch (Exception ex)
                    {
                        this.DataFromNetsuiteLog.Error("POStatusUpdate Exception: " + ex.ToString());
                    }
                }
            }
            return status;
        }

        #endregion

        #region General
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
                this.DataFromNetsuiteLog.Error("getNetsuitePassword Exception: " + ex.ToString());
            }
            return returnPass;
        }

        //public Boolean login()
        //{
        //    service.Timeout = 820000000;
        //    service.CookieContainer = new CookieContainer();
        //    ApplicationInfo appinfo = new ApplicationInfo();
        //    appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
        //    service.applicationInfo = appinfo;

        //    Passport passport = new Passport();
        //    passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
        //    passport.email = @Resource.NETSUITE_LOGIN_EMAIL;

        //    RecordRef role = new RecordRef();
        //    role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

        //    passport.role = role;
        //    //kang get netsuite password from DB
        //    //passport.password = @Resource.NETSUITE_LOGIN_PASSWORD;
        //    passport.password = getNetsuitePassword();

        //    Status status = service.login(passport).status;
        //    return status.isSuccess;
        //}
        //public void logout()
        //{
        //    try
        //    {
        //        Status logoutStatus = (service.logout()).status;
        //        if (logoutStatus.isSuccess == true)
        //        {
        //        }
        //        else
        //        {
        //            this.DataFromNetsuiteLog.Error("Login Netsuite failed.");
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        if (ex.Message.Contains("a session at a time"))
        //        {
        //            this.DataFromNetsuiteLog.Debug(ex.ToString());
        //        }
        //        else if (ex.Message.Contains("Your connection has timed out"))
        //        {
        //            this.DataFromNetsuiteLog.Debug(ex.ToString());
        //        }
        //        else
        //        {
        //            this.DataFromNetsuiteLog.Error(ex.ToString());
        //        } 
        //    }

        //}
        public DateTime convertDate(DateTime date)
        {
            DateTime convertedDate = DateTime.Now;
            try
            {
                convertedDate = Convert.ToDateTime(date.ToString("yyyy-MM-dd HH:mm:ss"));
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error(ex.ToString());
            }
            return convertedDate;
        }
        public String convertDateToString(DateTime date)
        {
            String convertedDate = null;
            try
            {
                convertedDate = date.ToString("yyyy-MM-dd HH:mm:ss");
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error(ex.ToString());
            }
            return convertedDate;
        }
        public String checkRecordRefIsNull(RecordRef recordRef)
        {
            String value = null;
            if (recordRef == null)
            {
                value = "";
            }
            else
            {
                value = recordRef.name;
            }
            return value;
        }
        public String checkRecordRefIsNull_internalID(RecordRef recordRef)
        {
            String value = null;
            if (recordRef == null)
            {
                value = "";
            }
            else
            {
                value = recordRef.internalId;
            }
            return value;
        }
        public String checkIsNull(String str)
        {
            if (String.IsNullOrEmpty(str))
            {
                str = "";
            }
            return str;
        }
        public String SpiltItemByName(String str)
        {
            String[] tempItem = str.Split(' ');

            if (tempItem.Count() > 2)
            {
                for (int j = 2; j < tempItem.Count(); j++)
                {
                    tempItem[1] += " " + tempItem[j];
                }
            }
            return tempItem[1];
        }
        public String SpiltItemByISBN(String str)
        {
            String[] tempItem = str.Split(' ');
            return tempItem[0];
        }
        private TokenPassport createTokenPassport()
        {
            string nonce = computeNonce();
            long timestamp = computeTimestamp();
            TokenPassportSignature signature = computeSignature(account, consumerKey, consumerSecret, tokenId, tokenSecret, nonce, timestamp);

            TokenPassport tokenPassport = new TokenPassport();
            tokenPassport.account = account;
            tokenPassport.consumerKey = consumerKey;
            tokenPassport.token = tokenId;
            tokenPassport.nonce = nonce;
            tokenPassport.timestamp = timestamp;
            tokenPassport.signature = signature;
            return tokenPassport;
        }

        private static string computeNonce()
        {
            RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();
            byte[] data = new byte[20];
            rng.GetBytes(data);
            int value = Math.Abs(BitConverter.ToInt32(data, 0));
            return value.ToString();
        }

        private static long computeTimestamp()
        {
            return ((long)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds);
        }

        private static TokenPassportSignature computeSignature(string compId, string consumerKey, string consumerSecret, string tokenId, string tokenSecret, string nonce, long timestamp)
        {
            string baseString = compId + "&" + consumerKey + "&" + tokenId + "&" + nonce + "&" + timestamp;
            string key = consumerSecret + "&" + tokenSecret;
            string signature = "";
            var encoding = new System.Text.ASCIIEncoding();
            byte[] keyBytes = encoding.GetBytes(key);
            byte[] baseStringBytes = encoding.GetBytes(baseString);
            //using (var hmacSha1 = new HMACSHA1(keyBytes))
            //{
            //    byte[] hashBaseString = hmacSha1.ComputeHash(baseStringBytes);
            //    signature = Convert.ToBase64String(hashBaseString);
            //}
            //TokenPassportSignature sign = new TokenPassportSignature();
            //sign.algorithm = "HMAC-SHA1";

            //ANET-44 SDE/ISAAC - TBA changing the signature method.
            //Commented above code by Brash Developer on 24-June-2021
            //Issue :- There is an ongoing 24 hours test window for HMAC-SHA1 Deprecation for Integrations Using Token-based Authentication (TBA), 
            //if possible you can use HMAC-SHA256 signature method.
            using (var hmacSha256 = new HMACSHA256(keyBytes))
            {
                byte[] hashBaseString = hmacSha256.ComputeHash(baseStringBytes);
                signature = Convert.ToBase64String(hashBaseString);
            }
            TokenPassportSignature sign = new TokenPassportSignature();
            sign.algorithm = "HMAC-SHA256";

            sign.Value = signature;
            return sign;
        }
        #endregion
    }
}
