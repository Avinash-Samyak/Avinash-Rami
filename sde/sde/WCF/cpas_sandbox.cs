using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MySql.Data.MySqlClient;
using log4net;
using System.Transactions;
using sde.Models;
using sde.comNetsuiteSandboxServices;
using System.Net;

namespace sde.WCF
{
    public class cpas_sandbox
    {
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");
        NetSuiteService service = new NetSuiteService();   //this is refer to sandbox

        #region Netsuite
        public Boolean sandboxCPASSalesOrder(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("sandboxCPASSalesOrder ***************");
            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrder: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        
                        var cpasSalesGroup = (from c in entities.cpas_stockposting
                                              join m in entities.map_location
                                              on c.spl_ml_location_internalID equals m.ml_location_internalID
                                              where (c.spl_transactionType == "SALES" || c.spl_transactionType == "UNSHIP")
                                              && (c.spl_createdDate > rangeFrom && c.spl_createdDate <= rangeTo)
                                              && c.spl_subsidiary == "IN"
                                              select new
                                              {
                                                  id = c.spl_sp_id,
                                                  tranType = c.spl_transactionType,
                                                  subsidiary = c.spl_subsidiary_internalID,
                                                  businessChannel = c.spl_mb_businessChannel_internalID,
                                                  postingDate = c.spl_postingDate,
                                                  location_id = c.spl_ml_location_internalID,
                                                  memo = c.spl_sDesc,
                                                  location_name ="TH RAMA MAIN"
                                              }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("sandboxCPASSalesOrder: " + cpasSalesGroup.Count() + " records to update.");

                        //status = true;
                        SalesOrder[] soList = new SalesOrder[cpasSalesGroup.Count()];

                        foreach (var con in cpasSalesGroup)
                        {
                            try
                            {
                                String refNo = null;
                                SalesOrder so = new SalesOrder();

                                RecordRef refForm = new RecordRef();
                                RecordRef refEntity = new RecordRef();
                                switch (con.subsidiary)
                                {
                                    case "3"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY;
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                        so.entity = refEntity;
                                        break;
                                    case "5"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG;
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                        so.entity = refEntity;
                                        break;
                                    case "7"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_ID;
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                        so.entity = refEntity;
                                        break;
                                    case "6"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_TH;
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                        so.entity = refEntity;
                                        break;
                                    case "9"://hard code - India
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_IN;
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                        so.entity = refEntity;
                                        break;
                                }

                                RecordRef refTerm = new RecordRef();
                                refTerm.internalId = @Resource.ACCOUNTINGLIST_TERMS_INTERNALID;//default 60 days
                                so.terms = refTerm;

                                //so.tranDate = DateTime.Now;
                                so.tranDate = Convert.ToDateTime(con.postingDate);
                                so.tranDateSpecified = true;
                                
                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = con.subsidiary;
                                so.subsidiary = refSubsidiary;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = con.businessChannel;
                                so.@class = refClass;

                                so.memo = con.memo + "; " + con.location_name; 

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = con.location_id;
                                so.location = refLocationSO;
                                
                                refNo = con.id;

                                var conItem = (from i in entities.cpas_stockposting
                                               where i.spl_sp_id == refNo
                                               && i.spl_ml_location_internalID == con.location_id
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

                                        /*
                                        soi.quantityFulfilled = Convert.ToDouble(item.spl_dQty);
                                        soi.quantityFulfilledSpecified = true;
                                        */

                                        soi.quantity = Convert.ToDouble(item.qty);
                                        soi.quantitySpecified = true;

                                        //ANET-28 Sales Order able to apply commit tag at item line.
                                        //soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                        soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                        soi.commitInventorySpecified = true;

                                        soi.amount = 0;
                                        soi.amountSpecified = true;

                                        //RecordRef refLocation = new RecordRef();
                                        //refLocation.internalId = item.location;
                                        //soi.location = refLocation;

                                        soii[itemCount] = soi;
                                        itemCount++;
                                    }

                                    soil.item = soii;
                                    so.itemList = soil;
                                    soList[soCount] = so;
                                    rowCount = soCount + 1;

                                    var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-SALES ORDER DEMO', 'CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesOrder: " + insertTask2);
                                    entities.Database.ExecuteSqlCommand(insertTask2);

                                    soCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("sandboxCPASSalesOrder Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of cpascontract

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //WriteResponse[] res = service.addList(soList);
                                job = service.asyncAddList(soList);
                                String jobID = job.jobId;
                                //WriteResponse[] res;
                                //res = service.addList(soList);
                                //String jobID = "IN Demo Testing"; 

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrder: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES ORDER DEMO' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrder: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES ORDER DEMO' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrder: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("sandboxCPASSalesOrder: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean sandboxCPASSalesOrderFulfillment(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("sandboxCPASSalesOrderFulfillment ***************");
            Boolean status = false;

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {

                    this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrderFulfillment: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var salesOrder = (from t in entities.requestnetsuite_task
                                          where t.rnt_updatedDate > rangeFrom
                                          && t.rnt_updatedDate <= rangeTo
                                          && t.rnt_description == "CPAS-SALES ORDER DEMO"
                                          && t.rnt_status == "TRUE"
                                          select t).ToList();

                        this.DataFromNetsuiteLog.Info("sandboxCPASSalesOrderFulfillment: " + salesOrder.Count() + " records to update.");
                        //status = true;
                        ItemFulfillment[] iffList = new ItemFulfillment[salesOrder.Count()];
                        Int32 fulFillCount = 0;

                        foreach (var so in salesOrder)
                        {
                            try
                            {
                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.salesOrder;
                                refSO.internalId = so.rnt_nsInternalId;
                                refSO.typeSpecified = true;

                                InitializeRecord recSO = new InitializeRecord();
                                recSO.type = InitializeType.itemFulfillment;
                                recSO.reference = refSO;

                                ReadResponse rrSO = service.initialize(recSO);
                                Record rSO = rrSO.record;

                                ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                ItemFulfillment iff2 = new ItemFulfillment();

                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_SALES_FULFILL_CUSTOMFORM_MY;
                                iff2.customForm = refForm;

                                //To get the Posting Date - WY-28.OCT.2014
                                String[] refNoArr;
                                DateTime postingDate = DateTime.Now;
                                String splSpID = string.Empty;
                                String locationID = string.Empty;
                                refNoArr = so.rnt_refNO.Split('.');
                                if (refNoArr.Count() >= 4)
                                {
                                    splSpID = refNoArr[2].ToString();
                                    locationID = refNoArr[3].ToString();

                                    var qPostingDate = (from c in entities.cpas_stockposting
                                                        where (c.spl_sp_id == splSpID && c.spl_ml_location_internalID == locationID)
                                                       select c.spl_postingDate).Distinct().ToList().FirstOrDefault();

                                    postingDate = Convert.ToDateTime(qPostingDate);
                                }

                                //iff2.tranDate = iff1.tranDate;
                                iff2.tranDate = postingDate;
                                iff2.tranDateSpecified = true;

                                ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                RecordRef refCreatedFrom = new RecordRef();
                                refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                iff2.createdFrom = refCreatedFrom;

                                ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];
                                int count1 = 0;

                                if (ifitemlist.item.Count() > 0)
                                {
                                    for (int i = 0; i < ifitemlist.item.Length; i++)
                                    {
                                        ItemFulfillmentItem iffi = new ItemFulfillmentItem();

                                        RecordRef refItem = new RecordRef();
                                        iffi.item = ifitemlist.item[i].item;

                                        iffi.orderLine = ifitemlist.item[i].orderLine;
                                        iffi.orderLineSpecified = true;

                                        RecordRef refLocation = new RecordRef();
                                        iffi.location = ifitemlist.item[i].location;

                                        iffi.quantity = ifitemlist.item[i].quantity;
                                        iffi.quantitySpecified = true;

                                        iffi.itemIsFulfilled = true;
                                        iffi.itemIsFulfilledSpecified = true;

                                        ifitems[count1] = iffi;
                                        count1++;
                                    }

                                    ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                    ifil1.item = ifitems;
                                    iff2.itemList = ifil1;

                                    iffList[fulFillCount] = iff2;
                                    rowCount = fulFillCount + 1;

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                    "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'UPD-STATUS.CPAS-SALES ORDER DEMO', 'REQUESTNETSUITETASK.RNT_ID." + so.rnt_id.ToString() + "', '" + gjob_id.ToString() + "'," +
                                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + so.rnt_nsInternalId + "','" + so.rnt_createdFromInternalId + "')";
                                    this.DataFromNetsuiteLog.Debug(insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    fulFillCount++;
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrderFulfillment: Sales order internalID_moNo: " + so.rnt_createdFromInternalId);
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("sandboxCPASSalesOrderFulfillment Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of ordMaster

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                job = service.asyncAddList(iffList);
                                String jobID = job.jobId; 
                                //WriteResponse[] res;
                                //res = service.addList(iffList);
                                //String jobID = "IN Demo Testing"; 

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrderFulfillment: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.CPAS-SALES ORDER DEMO' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrderFulfillment: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.CPAS-SALES ORDER DEMO' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("sandboxCPASSalesOrderFulfillment: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                    logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("sandboxCPASSalesOrderFulfillment: Login Netsuite failed.");
                }
                //}
            }//end of scope1

            return status;
        }
        public Boolean sandboxCPASOrderAdjustment(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("sandboxCPASOrderAdjustment ***************");
            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("sandboxCPASOrderAdjustment: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 daCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        try
                        {
                            var groupQ1 = (from q1 in entities.cpas_stockposting
                                           where q1.spl_transactionType.Contains("ADJUSTMENT") 
                                           && (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                           && q1.spl_subsidiary == "IN"
                                           select new
                                           {
                                               id = q1.spl_sp_id,
                                               tranType = q1.spl_transactionType,
                                               subsidiary = q1.spl_subsidiary_internalID,
                                               businessChannel = q1.spl_mb_businessChannel_internalID,
                                               memo = q1.spl_sDesc,
                                               postingDate = q1.spl_postingDate,
                                           }).Distinct().ToList();

                            //status = true;
                            InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];                            

                            foreach (var q1 in groupQ1)
                            {
                                InventoryAdjustment invAdj = new InventoryAdjustment();
                                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

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

                                //invAdj.tranDate = DateTime.Now;
                                invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                                invAdj.tranDateSpecified = true;
                                invAdj.memo = q1.memo;

                                //Set cost center to Finance & Accounting : Credit & Collections  - WY-30.OCT.2014
                                RecordRef refCostCenter = new RecordRef();
                                refCostCenter.internalId = @Resource.COSTCENTER_ACCOUNTING_INTERNALID;
                                invAdj.department = refCostCenter; 

                                var ordAdj = (from o in entities.cpas_stockposting
                                              where o.spl_transactionType == q1.tranType 
                                              && o.spl_sp_id == q1.id
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

                                this.DataFromNetsuiteLog.Info("sandboxCPASOrderAdjustment: " + ordAdjItem.Count() + " records to update.");

                                if (ordAdjItem.Count() > 0)
                                {
                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];
                                    
                                    Int32 itemCount = 0; 
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

                                    rowCount = daCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-ORDER ADJUSTMENT DEMO', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASOrderAdjustment: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    daCount++;
                                    status = true;
                                }
                            }
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res = service.addList(invAdjList);
                                    job = service.asyncAddList(invAdjList);
                                    String jobID = job.jobId;

                                    //WriteResponse[] res;
                                    //res = service.addList(invAdjList);
                                    //String jobID = "IN Demo Testing order adjustment"; 
                                     
                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASOrderAdjustment: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-ORDER ADJUSTMENT DEMO' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASOrderAdjustment: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-ORDER ADJUSTMENT DEMO' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASOrderAdjustment: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            scope1.Complete();
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("sandboxCPASOrderAdjustment Exception: " + ex.ToString());
                            status = false;
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("sandboxCPASOrderAdjustment: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean sandboxCPASCancellationOrder(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("sandboxCPASCancellationOrder ***************");
            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("sandboxCPASCancellationOrder: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 daCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        try
                        {
                            var groupQ1 = (from q1 in entities.cpas_stockposting
                                           where (q1.spl_transactionType == "RNCO" || q1.spl_transactionType == "RETN") 
                                           && (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                           && q1.spl_subsidiary == "IN"
                                           select new
                                           {
                                               id = q1.spl_sp_id,
                                               tranType = q1.spl_transactionType,
                                               subsidiary = q1.spl_subsidiary_internalID,
                                               businessChannel = q1.spl_mb_businessChannel_internalID,
                                               memo = q1.spl_sDesc,
                                               postingDate = q1.spl_postingDate
                                           }).Distinct().ToList();

                            //status = true;
                            InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];                            

                            foreach (var q1 in groupQ1)
                            {
                                InventoryAdjustment invAdj = new InventoryAdjustment();
                                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

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

                                //invAdj.tranDate = DateTime.Now;
                                invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                                invAdj.tranDateSpecified = true;
                                invAdj.memo = q1.memo;

                                var ordAdj = (from o in entities.cpas_stockposting
                                              where o.spl_transactionType == q1.tranType 
                                              && o.spl_sp_id == q1.id
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

                                this.DataFromNetsuiteLog.Info("sandboxCPASCancellationOrder: " + ordAdjItem.Count() + " records to update.");

                                if (ordAdjItem.Count() > 0)
                                {
                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];
                                    Int32 itemCount = 0;

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

                                    rowCount = daCount + 1;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-CANCELLATION ORDER DEMO', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASCancellationOrder: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    daCount++;
                                    status = true;
                                }
                            }
                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res = service.addList(invAdjList);
                                    job = service.asyncAddList(invAdjList);
                                    String jobID = job.jobId; 

                                    //WriteResponse[] res;
                                    //res = service.addList(invAdjList);
                                    //String jobID = "IN Demo Testing cancellation"; 

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASCancellationOrder: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-CANCELLATION ORDER DEMO' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("sandboxCPASCancellationOrder: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-CANCELLATION ORDER DEMO' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASCancellationOrder: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }

                            scope1.Complete();
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("sandboxCPASCancellationOrder Exception: " + ex.ToString());
                            status = false;
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("sandboxCPASCancellationOrder: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean sandboxCPASJournal(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("sandboxCPASJournal ***************");
            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("sandboxCPASJournal: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 jnCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var journal = (from jn in entities.cpas_journal
                                       join q2 in entities.map_country on jn.jn_subsidiary equals q2.mc_countryCode
                                       where jn.jn_rangeTo > rangeFrom && jn.jn_rangeTo <= rangeTo 
                                       && jn.jn_tranType != "PRESALES"
                                       && jn.jn_subsidiary == "IN"
                                       select new { jn.jn_tranType, jn.jn_desc, jn.jn_subsidiary_internalID, jn.jn_postingDate, q2.mc_country_internalID }).ToList();

                        var journalItem = from j in journal
                                          let k = new
                                          {
                                              tranType = j.jn_tranType,
                                              desc = j.jn_desc,
                                              subsidiary = j.jn_subsidiary_internalID,
                                              postingDate = j.jn_postingDate,
                                              country = j.mc_country_internalID
                                          }
                                          group j by k into g
                                          select new
                                          {
                                              tranType = g.Key.tranType,
                                              desc = g.Key.desc,
                                              subsidiary = g.Key.subsidiary,
                                              postingDate = g.Key.postingDate,
                                              country = g.Key.country
                                          };

                        this.DataFromNetsuiteLog.Info("sandboxCPASJournal: " + journal.Count() + " records to update.");

                        JournalEntry[] jeList = new JournalEntry[journalItem.Count()];
                        foreach (var j in journalItem)
                        {
                            try
                            {
                                Int32 lineCount = 0;
                                JournalEntry je = new JournalEntry();
                                JournalEntryLineList jell = new JournalEntryLineList();

                                //je.tranDate = DateTime.Now;
                                je.tranDate = Convert.ToDateTime(j.postingDate);
                                je.tranDateSpecified = true;

                                RecordRef refSub = new RecordRef();
                                refSub.internalId = j.subsidiary;
                                je.subsidiary = refSub;

                                CustomFieldRef[] cfrList1 = new CustomFieldRef[1];
                                StringCustomFieldRef scfr1 = new StringCustomFieldRef();
                                scfr1.scriptId = @Resource.CUSTOMFIELD_REMARKS_SCRIPTID;
                                scfr1.internalId = @Resource.CUSTOMFIELD_REMARKS_INTERNALID;
                                scfr1.value = j.desc;
                                cfrList1[0] = scfr1;

                                var journalLine = new List<cpas_journal>();
                                if (j.tranType.Equals("TOTAL UNSHIP"))
                                {
                                    journalLine = (from jn in entities.cpas_journal
                                                   where (jn.jn_tranType == j.tranType || jn.jn_tranType == "PRESALES") 
                                                   && jn.jn_subsidiary_internalID == j.subsidiary 
                                                   && jn.jn_postingDate == j.postingDate
                                                   select jn).ToList();
                                }
                                else
                                {
                                    journalLine = (from jn in entities.cpas_journal
                                                   where jn.jn_tranType == j.tranType 
                                                   && jn.jn_subsidiary_internalID == j.subsidiary 
                                                   && jn.jn_postingDate == j.postingDate
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

                                        if (j.tranType.Equals("RNCO"))
                                        {
                                            RecordRef refDepartment = new RecordRef();
                                            refDepartment.internalId = @Resource.COMPANY_COSTCENTER_INTERNALID;
                                            line.department = refDepartment;
                                        }

                                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                        StringCustomFieldRef scfr = new StringCustomFieldRef();
                                        scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                        scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                        scfr.value = j.country;
                                        cfrList[0] = scfr;

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

                                        rowCount = jnCount + 1;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-JOURNAL DEMO', 'CPASJOURNAL.JN_JOURNALID." + i.jn_journalID + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("sandboxCPASJournal: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);
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
                                this.DataFromNetsuiteLog.Error("sandboxCPASJournal Exception: " + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of journal

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                job = service.asyncAddList(jeList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASJournal: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-JOURNAL DEMO' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("sandboxCPASJournal: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-JOURNAL DEMO' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("sandboxCPASJournal: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("sandboxCPASJournal: Login Netsuite failed.");
                }
            }
            logout();
            return status;
        }
        #endregion

        #region General
        public Boolean login()
        {
            service.Timeout = 820000000;
            service.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
            service.applicationInfo = appinfo;

            Passport passport = new Passport();
            passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
            passport.email = @Resource.NETSUITESANDBOX_LOGIN_EMAIL;

            RecordRef role = new RecordRef();
            role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

            passport.role = role;
            passport.password = @Resource.NETSUITESANDBOX_LOGIN_PASSWORD;

            Status status = service.login(passport).status;
            return status.isSuccess;
        }
        public void logout()
        {
            try
            {
                Status logoutStatus = (service.logout()).status;
                if (logoutStatus.isSuccess == true)
                {
                }
                else
                {
                    this.DataFromNetsuiteLog.Error("Login Netsuite failed.");
                }
            }
            catch (Exception ex)
            {
                this.DataFromNetsuiteLog.Error(ex.ToString());
            }

        }
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
        #endregion
    }
}
