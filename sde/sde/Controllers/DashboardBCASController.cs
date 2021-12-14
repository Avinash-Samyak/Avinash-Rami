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
using System.Configuration;         //ckkoh 20150115
using MySql.Data.MySqlClient;
using System.Web.UI.WebControls;
using System.IO;
using System.Web.UI;       //ckkoh 20150115

namespace sde.Controllers
{
    public class DashboardBcasController : Controller
    {
        //
        // GET: /DashboardRIT/
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");

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
                    WriteResponse[] res = service.addList(invAdjList).writeResponse;
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

        [Authorize(Roles = "BCAS")]
        public ActionResult SOFulfillmentMY(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Fulfillment > By Date
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

                fromDate = fromDate.AddDays(-1).AddHours(11);
                toDate = toDate.AddHours(11);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_BcOrderFulfillment> tempIFList = new List<cls_BcOrderFulfillment>();

                var incSOFulfillment = "select substring(nsjomp_ordpack,1,5) as ordPack, substring(of_moNo,1,1) as ordType,  " +
                                        "round(sum((totFulfill/totISBN)*nsjomp_ordPrice),2) as ordPrice, round(sum((totFulfill/totISBN)*nsjomp_gstamount),2) as ordGst,  " +
                                        "round(sum(totDelivery),2) as ordShipping, sum(round(nsjomp_ordQty,2)) as ordQty, sum(round((totFulfill/totISBN),2)) as ordFulfill, sum(round(totFulfillnSku,2)) as ordFulfillnSku from  " +
                                        "(  " +
                                        "SELECT aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount,  " +
                                        "count(*) totISBN,  " +
                                        "SUM(aa.of_ordFulfillQty) totFulfill,  " +
                                        "SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty) totFulfillnSku,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_item_price),2) totPrice,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_gstamount),2) totGst,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryCharge),2) totDelivery,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryChargeGst),2) totDeliveryGst  " +
                                        "FROM sde.bcas_ordersfulfill aa  " +
                                        "left join sde.netsuite_jobordmaster_packdetail bb  " +
                                        "on aa.of_jobID=bb.nsjompd_job_ID and aa.of_pack_ID=bb.nsjompd_jobOrdMaster_pack_ID  " +
                                        "and aa.of_item_ID=bb.nsjompd_item_ID  " +
                                        "left join sde.netsuite_jobordmaster_pack cc  " +
                                        "on aa.of_jobID=cc.nsjomp_job_ID and aa.of_pack_ID=cc.nsjomp_jobOrdMaster_pack_ID  " +
                                        "where aa.of_rangeTo>'" + date1 + "' and aa.of_rangeTo<='" + date2 + "'  " +
                                        "and aa.of_country_tag='MY'  " +
                                        //"and aa.of_moNo not like 'MY%'  " +
                                        "group by aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount  " +
                                        ") as tt  " +
                                        "group by substring(nsjomp_ordpack,1,5), substring(of_moNo,1,1) order by substring(of_moNo,1,1), substring(nsjomp_ordpack,1,5)";


                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_BcOrderFulfillment BcOrderFulfillment = new cls_BcOrderFulfillment();
                    BcOrderFulfillment.ordPack = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                    BcOrderFulfillment.ordType = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    BcOrderFulfillment.ordPrice = (dtr.GetValue(2) == DBNull.Value) ? 0 : dtr.GetDecimal(2);
                    BcOrderFulfillment.ordGst = (dtr.GetValue(3) == DBNull.Value) ? 0 : dtr.GetDecimal(3);
                    BcOrderFulfillment.ordShipping = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetDecimal(4);
                    BcOrderFulfillment.ordQty = (dtr.GetValue(5) == DBNull.Value) ? 0 : dtr.GetInt32(5);
                    BcOrderFulfillment.ordFulfill = (dtr.GetValue(6) == DBNull.Value) ? 0 : dtr.GetInt32(6);
                    BcOrderFulfillment.ordFulfillnSku = (dtr.GetValue(7) == DBNull.Value) ? 0 : dtr.GetInt32(7);
                    tempIFList.Add(BcOrderFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                BcOrderFulfillmentList BcOrderFulfillmentList = new BcOrderFulfillmentList();
                BcOrderFulfillmentList.BcOrderFulfillment = tempIFList;
                return View(new Tuple<sde.Models.BcOrderFulfillmentList>(BcOrderFulfillmentList));
            }
        }

        [Authorize(Roles = "BCAS")]
        public ActionResult SOFulfillmentSG(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Fulfillment > By Date
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

                fromDate = fromDate.AddDays(-1).AddHours(11);
                toDate = toDate.AddHours(11);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_BcOrderFulfillment> tempIFList = new List<cls_BcOrderFulfillment>();

                var incSOFulfillment = "select substring(nsjomp_ordpack,1,5) as ordPack, substring(of_moNo,1,1) as ordType,  " +
                                        "round(sum((totFulfill/totISBN)*nsjomp_ordPrice),2) as ordPrice, round(sum((totFulfill/totISBN)*nsjomp_gstamount),2) as ordGst,  " +
                                        "round(sum(totDelivery),2) as ordShipping, sum(round(nsjomp_ordQty,2)) as ordQty, sum(round((totFulfill/totISBN),2)) as ordFulfill, sum(round(totFulfillnSku,2)) as ordFulfillnSku from  " +
                                        "(  " +
                                        "SELECT aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount,  " +
                                        "count(*) totISBN,  " +
                                        "SUM(aa.of_ordFulfillQty) totFulfill,  " +
                                        "SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty) totFulfillnSku,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_item_price),2) totPrice,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_gstamount),2) totGst,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryCharge),2) totDelivery,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryChargeGst),2) totDeliveryGst  " +
                                        "FROM sde.bcas_ordersfulfill aa  " +
                                        "left join sde.netsuite_jobordmaster_packdetail bb  " +
                                        "on aa.of_jobID=bb.nsjompd_job_ID and aa.of_pack_ID=bb.nsjompd_jobOrdMaster_pack_ID  " +
                                        "and aa.of_item_ID=bb.nsjompd_item_ID  " +
                                        "left join sde.netsuite_jobordmaster_pack cc  " +
                                        "on aa.of_jobID=cc.nsjomp_job_ID and aa.of_pack_ID=cc.nsjomp_jobOrdMaster_pack_ID  " +
                                        "where aa.of_rangeTo>'" + date1 + "' and aa.of_rangeTo<='" + date2 + "'  " +
                                        "and aa.of_country_tag='SG'  " +
                                        //"and aa.of_moNo not like 'MY%'  " +
                                        "group by aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount  " +
                                        ") as tt  " +
                                        "group by substring(nsjomp_ordpack,1,5), substring(of_moNo,1,1) order by substring(of_moNo,1,1), substring(nsjomp_ordpack,1,5)";


                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_BcOrderFulfillment BcOrderFulfillment = new cls_BcOrderFulfillment();
                    BcOrderFulfillment.ordPack = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                    BcOrderFulfillment.ordType = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    BcOrderFulfillment.ordPrice = (dtr.GetValue(2) == DBNull.Value) ? 0 : dtr.GetDecimal(2);
                    BcOrderFulfillment.ordGst = (dtr.GetValue(3) == DBNull.Value) ? 0 : dtr.GetDecimal(3);
                    BcOrderFulfillment.ordShipping = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetDecimal(4);
                    BcOrderFulfillment.ordQty = (dtr.GetValue(5) == DBNull.Value) ? 0 : dtr.GetInt32(5);
                    BcOrderFulfillment.ordFulfill = (dtr.GetValue(6) == DBNull.Value) ? 0 : dtr.GetInt32(6);
                    BcOrderFulfillment.ordFulfillnSku = (dtr.GetValue(7) == DBNull.Value) ? 0 : dtr.GetInt32(7);
                    tempIFList.Add(BcOrderFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                BcOrderFulfillmentList BcOrderFulfillmentList = new BcOrderFulfillmentList();
                BcOrderFulfillmentList.BcOrderFulfillment = tempIFList;
                return View(new Tuple<sde.Models.BcOrderFulfillmentList>(BcOrderFulfillmentList));
            }
        }

        [Authorize(Roles = "BCAS")]
        public ActionResult SOFulfillmentID(DateTime? fromDate1 = null, DateTime? toDate1 = null) // SO Fulfillment > By Date
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

                fromDate = fromDate.AddDays(-1).AddHours(11);
                toDate = toDate.AddHours(11);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_BcOrderFulfillment> tempIFList = new List<cls_BcOrderFulfillment>();

                var incSOFulfillment = "select substring(nsjomp_ordpack,1,5) as ordPack, substring(of_moNo,1,1) as ordType,  " +
                                        "round(sum((totFulfill/totISBN)*nsjomp_ordPrice),2) as ordPrice, round(sum((totFulfill/totISBN)*nsjomp_gstamount),2) as ordGst,  " +
                                        "round(sum(totDelivery),2) as ordShipping, sum(round(nsjomp_ordQty,2)) as ordQty, sum(round((totFulfill/totISBN),2)) as ordFulfill, sum(round(totFulfillnSku,2)) as ordFulfillnSku from  " +
                                        "(  " +
                                        "SELECT aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount,  " +
                                        "count(*) totISBN,  " +
                                        "SUM(aa.of_ordFulfillQty) totFulfill,  " +
                                        "SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty) totFulfillnSku,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_item_price),2) totPrice,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_gstamount),2) totGst,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryCharge),2) totDelivery,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryChargeGst),2) totDeliveryGst  " +
                                        "FROM sde.bcas_ordersfulfill aa  " +
                                        "left join sde.netsuite_jobordmaster_packdetail bb  " +
                                        "on aa.of_jobID=bb.nsjompd_job_ID and aa.of_pack_ID=bb.nsjompd_jobOrdMaster_pack_ID  " +
                                        "and aa.of_item_ID=bb.nsjompd_item_ID  " +
                                        "left join sde.netsuite_jobordmaster_pack cc  " +
                                        "on aa.of_jobID=cc.nsjomp_job_ID and aa.of_pack_ID=cc.nsjomp_jobOrdMaster_pack_ID  " +
                                        "where aa.of_rangeTo>'" + date1 + "' and aa.of_rangeTo<='" + date2 + "'  " +
                                        "and aa.of_country_tag='ID'  " +
                                        //"and aa.of_moNo not like 'MY%'  " +
                                        "group by aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_pack_ID, cc.nsjomp_ordpack,  " +
                                        "cc.nsjomp_ordQty, nsjomp_ordPrice,nsjomp_gstamount  " +
                                        ") as tt  " +
                                        "group by substring(nsjomp_ordpack,1,5), substring(of_moNo,1,1) order by substring(of_moNo,1,1), substring(nsjomp_ordpack,1,5)";


                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_BcOrderFulfillment BcOrderFulfillment = new cls_BcOrderFulfillment();
                    BcOrderFulfillment.ordPack = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                    BcOrderFulfillment.ordType = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    BcOrderFulfillment.ordPrice = (dtr.GetValue(2) == DBNull.Value) ? 0 : dtr.GetDecimal(2);
                    BcOrderFulfillment.ordGst = (dtr.GetValue(3) == DBNull.Value) ? 0 : dtr.GetDecimal(3);
                    BcOrderFulfillment.ordShipping = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetDecimal(4);
                    BcOrderFulfillment.ordQty = (dtr.GetValue(5) == DBNull.Value) ? 0 : dtr.GetInt32(5);
                    BcOrderFulfillment.ordFulfill = (dtr.GetValue(6) == DBNull.Value) ? 0 : dtr.GetInt32(6);
                    BcOrderFulfillment.ordFulfillnSku = (dtr.GetValue(7) == DBNull.Value) ? 0 : dtr.GetInt32(7);
                    tempIFList.Add(BcOrderFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                BcOrderFulfillmentList BcOrderFulfillmentList = new BcOrderFulfillmentList();
                BcOrderFulfillmentList.BcOrderFulfillment = tempIFList;
                return View(new Tuple<sde.Models.BcOrderFulfillmentList>(BcOrderFulfillmentList));
            }
        }

        [Authorize(Roles = "BCAS")]
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

                fromDate = fromDate.AddDays(-1).AddHours(11);
                toDate = toDate.AddHours(11);

                var date1 = fromDate.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_BcIncompleteOrderFulfillment> tempIFList = new List<cls_BcIncompleteOrderFulfillment>();

                var incSOFulfillment = "select substring(nsjomp_ordpack,1,5) as ordPack, of_item_ISBN as ordISBN, sum(totOrdQty) as ordQty, sum(totFulfill) as ordFulfill, sum(totFulfillnSku) as ordFulfillnSku " +
                                        "from  " +
                                        "(  " +
                                        "SELECT aa.of_rangeTo, aa.of_moNo, aa.of_country_tag,  " +
                                        "aa.of_item_ISBN, bb.nsjompd_tax_code, bb.nsjompd_item_price, cc.nsjomp_ordpack,  " +
                                        "count(*) totISBN,  " +
                                        "SUM(aa.of_ordQty) totOrdQty,  " +
                                        "SUM(aa.of_ordFulfillQty) totFulfill,  " +
                                        "SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty) totFulfillnSku,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_item_price),2) totPrice,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_gstamount),2) totGst,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryCharge),2) totDelivery,  " +
                                        "round(SUM(aa.of_ordFulfillQty*aa.of_ordSkuQty*bb.nsjompd_deliveryChargeGst),2) totDeliveryGst  " +
                                        "FROM sde.bcas_ordersfulfill aa  " +
                                        "left join sde.netsuite_jobordmaster_packdetail bb   " +
                                        "on aa.of_jobID=bb.nsjompd_job_ID   " +
                                        "and aa.of_pack_ID=bb.nsjompd_jobOrdMaster_pack_ID   " +
                                        "and aa.of_item_ID=bb.nsjompd_item_ID  " +
                                        "left join sde.netsuite_jobordmaster_pack cc  " +
                                        "on aa.of_jobID=cc.nsjomp_job_ID and aa.of_pack_ID=cc.nsjomp_jobOrdMaster_pack_ID   " +
                                        "where aa.of_rangeTo>'" + date1 + "' and aa.of_rangeTo<='" + date2 + "'  " +
                    //"and aa.of_country_tag='ID'  " +
                    //"and aa.of_moNo like 'M%'  " +
                    //"and aa.of_moNo not like 'MY%'  " +
                                        "group by aa.of_rangeTo, aa.of_moNo, aa.of_country_tag, aa.of_item_ISBN, bb.nsjompd_tax_code, bb.nsjompd_item_price, cc.nsjomp_ordpack  " +
                                        ") as tt  " +
                                        "group by substring(nsjomp_ordpack,1,5), of_item_ISBN having sum(totOrdQty)<>sum(totFulfill)";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_BcIncompleteOrderFulfillment BcIncompleteOrderFulfillment = new cls_BcIncompleteOrderFulfillment();
                    BcIncompleteOrderFulfillment.ordPack = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                    BcIncompleteOrderFulfillment.ordISBN = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    BcIncompleteOrderFulfillment.ordQty = (dtr.GetValue(2) == DBNull.Value) ? 0 : dtr.GetInt32(2);
                    BcIncompleteOrderFulfillment.ordFulfill = (dtr.GetValue(3) == DBNull.Value) ? 0 : dtr.GetInt32(3);
                    BcIncompleteOrderFulfillment.ordFulfillnSku = (dtr.GetValue(4) == DBNull.Value) ? 0 : dtr.GetInt32(4);
                    tempIFList.Add(BcIncompleteOrderFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                BcIncompleteOrderFulfillmentList BcIncompleteOrderFulfillmentList = new BcIncompleteOrderFulfillmentList();
                BcIncompleteOrderFulfillmentList.BcIncompleteOrderFulfillment = tempIFList;
                return View(new Tuple<sde.Models.BcIncompleteOrderFulfillmentList>(BcIncompleteOrderFulfillmentList));
            }
        }
    }
}
