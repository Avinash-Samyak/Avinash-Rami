/*
 * Date         Developer    Issue
 * ----------------------------------------------------------------------------------------------
 * 2015-05-26   David        #675 
 * 2016-01-22   David        #1075
 * 2016-01-22   David        #1076
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MySql.Data.MySqlClient;
using log4net;
using System.Transactions;
using sde.Models;
using sde.comNetsuiteServices;
using System.Net;
using System.Collections;
using System.Security.Cryptography;

namespace sde.WCF
{
    public class bcas
    {
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");
        NetSuiteService service = new NetSuiteService();

        //TBA
        string account = @Resource.NETSUITE_LOGIN_ACCOUNT;
        string appID = @Resource.NETSUITE_LOGIN_ACCOUNT;
        string consumerKey = @Resource.NETSUITE_Consumer_Key;
        string consumerSecret = @Resource.NETSUITE_Consumer_Secret;
        string tokenId, tokenSecret;

        #region Netsuite
        public Boolean BCASSOrderAdjustment(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("BCASSOrderAdjustment ***************");
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
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASSOrderAdjustment: Login Netsuite failed. Exception : " + ex.ToString());

                }


                //    Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 daCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        
                        try
                        {
                            var query1 = (from q1 in entities.wms_jobordscan
                                          where q1.jos_businessChannel_code == "BC"
                                          && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                          //&& q1.jos_country_tag != "ID"
                                          select new
                                          {
                                              q1.jos_job_ID,
                                              q1.jos_businessChannel_code,
                                              q1.jos_country_tag,
                                              q1.jos_rangeTo,
                                              mono = q1.jos_moNo.Substring(0, 1)
                                          })
                                      .Distinct()
                                      .OrderBy(x => x.jos_businessChannel_code)
                                      .ThenBy(y => y.jos_country_tag)
                                      .ToList();

                            this.DataFromNetsuiteLog.Info("BCASSOrderAdjustment: " + query1.Count() + " records to update.");
                            InventoryAdjustment[] invAdjList = new InventoryAdjustment[query1.Count()];

                            foreach (var q1 in query1)
                            {
                                InventoryAdjustment invAdj = new InventoryAdjustment();
                                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                                RecordRef refAccount = new RecordRef();
                                refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_WRITEOFFDMG;
                                invAdj.account = refAccount;

                                RecordRef refCustomer = new RecordRef();
                                if (q1.jos_country_tag.Equals("ID"))
                                {
                                    refCustomer.internalId = @Resource.BCAS_CUSTOMER_ID;
                                    invAdj.customer = refCustomer;
                                }
                                else if (q1.jos_country_tag.Equals("MY"))
                                {
                                    refCustomer.internalId = @Resource.BCAS_CUSTOMER_MY;
                                    invAdj.customer = refCustomer;
                                }
                                else if (q1.jos_country_tag.Equals("SG"))
                                {
                                    refCustomer.internalId = @Resource.BCAS_CUSTOMER_SG;
                                    invAdj.customer = refCustomer;
                                }

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = @Resource.BCAS_DUMMYSALES_MY;
                                invAdj.subsidiary = refSubsidiary;

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                invAdj.@class = refBusinessChannel;

                                //RecordRef refCostCenter = new RecordRef();
                                //refCostCenter.internalId = @Resource.COSTCENTER_SALESANDMARKETING;
                                //invAdj.department = refCostCenter;

                                invAdj.tranDate = DateTime.Now;
                                //invAdj.tranDate = DateTime.Now.AddDays(-2);  //for year end closing
                                invAdj.tranDateSpecified = true;

                                if (q1.mono.Equals("E"))
                                {
                                    invAdj.memo = "BCAS REPLACEMENT SALES";
                                }
                                else
                                {
                                    invAdj.memo = "BCAS SALES";
                                }

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
                                scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_PHYSICALCOUNT;
                                cfrList[0] = scfr;
                                invAdj.customFieldList = cfrList;

                                #region Unscan
                                //var query3 = (from josp in entities.wms_jobordscan_pack
                                //              join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                //              join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                //              where josp.josp_jobID == q1.jos_job_ID && josp.josp_ordFulFill > 0
                                //              && josp.josp_moNo.StartsWith(q1.mono) 
                                //              && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                              //select new
                                              //{
                                              //    jompd.nsjompd_item_internalID,
                                              //    jomp.nsjomp_ordQty,
                                              //    jomp.nsjomp_ordFulfill,
                                              //    totalQty = (jomp.nsjomp_ordQty * jompd.nsjompd_sku_qty),
                                              //    fullQty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty)
                                              //}).ToList();

                                var query3 = (from ji in entities.bcas_ordersfulfill
                                              join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                              where ji.of_jobID == q1.jos_job_ID
                                              && ji.of_moNo.StartsWith(q1.mono) 
                                              && ji.of_rangeTo > rangeFrom 
                                              && ji.of_rangeTo <= rangeTo
                                              && (ji.of_netsuiteAdjustment == null || ji.of_netsuiteAdjustment == "")
                                              && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                              && nstask.rnt_status == "TRUE"
                                              select new
                                                {
                                                    ji.of_item_internalID,
                                                    ji.of_ordQty,
                                                    ji.of_ordFulfillQty,
                                                    totalQty = (ji.of_ordQty * ji.of_ordSkuQty),
                                                    fullQty = (ji.of_ordFulfillQty * ji.of_ordSkuQty)
                                                }).ToList();

                                var groupQ3 = from p in query3
                                              let k = new
                                              {
                                                  //itemInternalID = p.nsjompd_item_internalID,
                                                  itemInternalID = p.of_item_internalID,
                                                  totalQty = p.totalQty,
                                                  fulFillQty = p.fullQty
                                              }
                                              group p by k into g
                                              where (g.Sum(p => p.fullQty) - g.Sum(p => p.totalQty)) < 0
                                              select new
                                              {
                                                  item = g.Key.itemInternalID,
                                                  adjQty = g.Sum(p => p.fullQty) - g.Sum(p => p.totalQty)
                                              };

                                if (groupQ3.Count() > 0)
                                {
                                    InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[groupQ3.Count()];
                                    
                                    Int32 itemCount = 0;

                                    foreach (var i in groupQ3)
                                    {
                                        InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                                        RecordRef refItem = new RecordRef();
                                        refItem.internalId = i.item;
                                        item.item = refItem;

                                        RecordRef refLocation = new RecordRef();
                                        refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                        item.location = refLocation;

                                        item.adjustQtyBy = Convert.ToInt32(i.adjQty);
                                        item.adjustQtyBySpecified = true;

                                        //cpng start
                                        if (i.item != @Resource.BCAS_MY_DC_INTERNALID
                                                && i.item != @Resource.BCAS_MY_ROUDING_INTERNALID)
                                        {
                                            InventoryAssignment[] IAA = new InventoryAssignment[1];
                                            InventoryAssignment IA = new InventoryAssignment();
                                            InventoryAssignmentList IAL = new InventoryAssignmentList();
                                            InventoryDetail ID = new InventoryDetail();

                                            IA.quantity = Convert.ToInt32(i.adjQty);
                                            IA.quantitySpecified = true;
                                            IA.binNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN };
                                            IAA[0] = IA;
                                            IAL.inventoryAssignment = IAA;
                                            ID.inventoryAssignmentList = IAL;

                                            item.inventoryDetail = ID;
                                        }
                                        //cpng end

                                        items[itemCount] = item;
                                        itemCount++;

                                        var insertLog = "insert into bcas_inventoryadjustment (ia_jobID, ia_businessChannel_code,ia_countryTag,ia_moNo_prefix,ia_item_internalID,ia_adjQty,ia_createdDate,ia_rangeFrom,ia_rangeTo) " +
                                            "values ('" + q1.jos_job_ID + "','" + q1.jos_businessChannel_code + "','" + q1.jos_country_tag + "','" + q1.mono + "','" + i.item + "','" + i.adjQty + "'," +
                                            "'" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "')";
                                        this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + insertLog);
                                        entities.Database.ExecuteSqlCommand(insertLog);
                                    }
                                    iail.inventory = items;
                                    invAdj.inventoryList = iail;
                                    invAdjList[daCount] = invAdj;

                                    rowCount = daCount + 1;

                                    String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-ORDER ADJUSTMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteAdjustment = '" + gjob_id.ToString() + "' where (of_netsuiteAdjustment is null or of_netsuiteAdjustment = '') " +
                                                        "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                        "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                        "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                        "and of_moNo like '" + q1.mono + "%' ";

                                    this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + updateBcasFulfill);
                                    entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                    status = true;
                                    daCount++;
                                }
                                #endregion
                            }

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //WriteResponse[] res = service.addList(invAdjList);
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(invAdjList);
                                    String jobID = job.jobId;

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-ORDER ADJUSTMENT' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    // stop update the NS job id 2014-DEC-07
                                    //var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteAdjustment = '" + jobID + "' where of_netsuiteAdjustment ='" + gjob_id.ToString() + "' " +
                                    //                    "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                    //                    "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' ";
                                    //this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + updateBcasFulfill);
                                    //entities.Database.ExecuteSqlCommand(updateBcasFulfill);
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-ORDER ADJUSTMENT' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASSOrderAdjustment: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                            scope1.Complete();
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("BCASSOrderAdjustment Exception: " + ex.ToString());
                            status = false;
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSOrderAdjustment: Login Netsuite failed.");
                }
            }//end of scope1
            //logout();
            return status;
        }
        public Boolean BCASJournal(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("BCASJournal ***************");
            Boolean status = false;

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
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASJournal: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASJournal: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //     Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASJournal: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 jnCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var IDquery = (from q1 in entities.wms_jobordscan
                                      where q1.jos_businessChannel_code == "BC"
                                      && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                      && !q1.jos_moNo.Contains("E")
                                      && q1.jos_country_tag == "ID"
                                      && q1.jos_netsuiteProgress == null
                                      select new { q1.jos_job_ID, q1.jos_moNo })
                                      .Distinct()
                                      .ToList();
                        List<string> _IDjob = new List<string>();
                        foreach (var q1 in IDquery)
                        {
                            _IDjob.Add(q1.jos_job_ID + "-" + q1.jos_moNo);
                        }

                        var journalGroup = (from q1 in entities.wms_jobordscan
                                            join q2 in entities.map_country on q1.jos_country_tag equals q2.mc_countryCode
                                            join q3 in entities.map_currency on q1.jos_country_tag equals q3.mc_country
                                            where q1.jos_businessChannel_code == "BC" 
                                            && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                            && !q1.jos_moNo.Contains("E") 
                                            && (q1.jos_country_tag.Equals("MY") || q1.jos_country_tag.Equals("ID")) //Add country in MY and ID - WY-04.SEPT.2014
                                            && q1.jos_netsuiteProgress == null
                                            select new
                                            {
                                                q1.jos_job_ID,
                                                q1.jos_businessChannel_code,
                                                q1.jos_country_tag,
                                                q2.mc_country_internalID,
                                                q3.mc_currency_internalID,
                                                q1.jos_rangeTo,
                                                mono = q1.jos_moNo.Substring(0, 1)
                                            })
                              .Distinct()
                              .OrderBy(x => x.jos_businessChannel_code)
                              .ThenBy(y => y.jos_country_tag)
                              .ToList();

                        //status = true;
                        this.DataFromNetsuiteLog.Info("BCASJournal: " + journalGroup.Count() + " records to update.");

                        JournalEntry[] jeList = new JournalEntry[journalGroup.Count()];
                        foreach (var j in journalGroup)
                        {
                            try
                            {
                                JournalEntry je = new JournalEntry();
                                JournalEntryLineList jell = new JournalEntryLineList(); 

                                je.tranDate = DateTime.Now;
                                je.tranDateSpecified = true;

                                RecordRef refSub = new RecordRef();
                                refSub.internalId = @Resource.BCAS_DUMMYSALES_MY;//j.subsidiary;
                                je.subsidiary = refSub;

                                RecordRef refCurrency = new RecordRef();
                                refCurrency.internalId = j.mc_currency_internalID;
                                je.currency = refCurrency;

                                CustomFieldRef[] cfrList1 = new CustomFieldRef[1];
                                StringCustomFieldRef scfr1 = new StringCustomFieldRef();
                                scfr1.scriptId = @Resource.CUSTOMFIELD_REMARKS_SCRIPTID;
                                scfr1.internalId = @Resource.CUSTOMFIELD_REMARKS_INTERNALID;
                                if (j.mono.Equals("E"))
                                {
                                    scfr1.value = "BCAS REPLACEMENT SALES";
                                }
                                else
                                {
                                    scfr1.value = "BCAS SALES";
                                }
                                cfrList1[0] = scfr1;

                                #region ID Sales
                                if (j.jos_country_tag.Equals("ID"))
                                {
                                    var query2 = (from jomp in entities.netsuite_jobordmaster_pack
                                                  join jom in entities.netsuite_jobordmaster 
                                                  on jomp.nsjomp_jobOrdMaster_ID equals jom.nsjom_jobOrdMaster_ID
                                                  where jomp.nsjomp_job_ID == j.jos_job_ID
                                                  && jom.nsjom_moNo.StartsWith(j.mono) 
                                                  && _IDjob.Contains(jom.nsjom_nsj_job_ID + "-" + jom.nsjom_moNo)
                                                  select new { jomp.nsjomp_ofrCode, amt = (jomp.nsjomp_ordFulfill * jomp.nsjomp_ordPrice) }).ToList();

                                    var journalLine = from p in query2
                                                      let k = new
                                                      {
                                                          jobID = p.nsjomp_ofrCode
                                                      }
                                                      group p by k into g
                                                      select new
                                                      {
                                                          amt = g.Sum(p => p.amt)
                                                      };

                                    var journalAcc = (from q1 in entities.map_chartofaccount
                                                      where q1.coa_tranType == "BCAS JOURNAL"
                                                      select q1).ToList();

                                    if (journalLine.Count() > 0)
                                    {
                                        double? amount = 0;
                                        foreach (var jl in journalLine)
                                        {
                                            amount = amount + jl.amt;
                                        }

                                        JournalEntryLine[] lines = new JournalEntryLine[2];
                                        for (int i = 0; i < lines.Count(); i++)
                                        {
                                            JournalEntryLine line = new JournalEntryLine();

                                            RecordRef refBusinessChannel = new RecordRef();
                                            refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                            line.@class = refBusinessChannel;

                                            CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                            StringCustomFieldRef scfr = new StringCustomFieldRef();
                                            scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                            scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                            scfr.value = j.mc_country_internalID;
                                            cfrList[0] = scfr;

                                            line.customFieldList = cfrList;

                                            if (j.mono.Equals("E"))
                                            {
                                                line.memo = "BCAS REPLACEMENT SALES " + j.jos_country_tag;
                                            }
                                            else
                                            {
                                                line.memo = "BCAS SALES " + j.jos_country_tag;
                                            }

                                            //RecordRef refEntity = new RecordRef();
                                            //if (j.jos_country_tag.Equals("ID"))
                                            //{
                                            //    refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                            //    line.entity = refEntity;
                                            //}
                                            //else if (j.jos_country_tag.Equals("MY"))
                                            //{
                                            //    refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                            //    line.entity = refEntity;
                                            //}
                                            //else if (j.jos_country_tag.Equals("SG"))
                                            //{
                                            //    refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                            //    line.entity = refEntity;
                                            //}

                                            if (i == 0)
                                            {
                                                if (journalAcc[i].coa_glType.Equals("DEBIT"))
                                                {
                                                    RecordRef refDebit = new RecordRef();
                                                    refDebit.internalId = journalAcc[i].coa_account_internalID;
                                                    line.account = refDebit;

                                                    line.debit = Convert.ToDouble(amount);
                                                    line.debitSpecified = true;
                                                }
                                            }
                                            else if (i == 1)
                                            {
                                                if (journalAcc[i].coa_glType.Equals("CREDIT"))
                                                {
                                                    RecordRef refCredit = new RecordRef();
                                                    refCredit.internalId = journalAcc[i].coa_account_internalID;
                                                    line.account = refCredit;

                                                    line.credit = Convert.ToDouble(amount);
                                                    line.creditSpecified = true;
                                                }
                                            }
                                            lines[i] = line;
                                        }
                                        jell.line = lines;
                                        je.lineList = jell;
                                        jeList[jnCount] = je;

                                        rowCount = jnCount + 1;

                                        //Insert query2 list into bcas_journalcalclist - WY-03.SEPT.2014
                                        var insertJournal = "insert into bcas_journalcalclist (jcl_amt,jcl_job_ID,jcl_businessChannel_code,jcl_country_tag, " +
                                                            "jcl_country_internalID,jcl_currency_internalID,jcl_jos_rangeTo,jcl_moNo,jcl_createdDate,jcl_rangeFrom,jcl_rangeTo) " +
                                                            "values ( '" + Convert.ToDouble(amount) + "', '" + j.jos_job_ID + "', '" + j.jos_businessChannel_code + "', '" + j.jos_country_tag + "', " +
                                                            "'" + j.mc_country_internalID + "', '" + j.mc_currency_internalID + "', '" + convertDateToString(Convert.ToDateTime(j.jos_rangeTo)) + "', " +
                                                            "'" + j.mono + "', '" + convertDateToString(DateTime.Now) + "', '" + convertDateToString(rangeFrom) + "', '" + convertDateToString(rangeTo) + "') ";

                                        this.DataFromNetsuiteLog.Debug("BCASJournal: " + insertJournal);
                                        entities.Database.ExecuteSqlCommand(insertJournal);


                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + j.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + j.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-JOURNAL', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";

                                        this.DataFromNetsuiteLog.Debug("BCASJournal: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);


                                        var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                                            "and jos_job_ID = '" + j.jos_job_ID + "' " +
                                                            "and jos_moNo like '" + j.mono + "%' " +
                                                            "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                        this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        jnCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region Other Sales
                                else
                                {
                                var query2 = (from josp in entities.wms_jobordscan_pack
                                              join jomp in entities.netsuite_jobordmaster_pack 
                                              on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                              where josp.josp_jobID == j.jos_job_ID 
                                              //&& josp.josp_ordFulFill > 0
                                              && josp.josp_moNo.StartsWith(j.mono) 
                                              && josp.josp_rangeTo > rangeFrom 
                                              && josp.josp_rangeTo <= rangeTo
                                              select new { jomp.nsjomp_ofrCode, amt = (josp.josp_ordFulFill * jomp.nsjomp_ordPrice) }).ToList();
                                  
                                var groupQ2 = from p in query2
                                              let k = new
                                              {
                                                  jobID = p.nsjomp_ofrCode
                                              }
                                              group p by k into g
                                              select new
                                              {
                                                  amt = g.Sum(p => p.amt)
                                              };

                                var journalAcc = (from q1 in entities.map_chartofaccount
                                                  where q1.coa_tranType == "BCAS JOURNAL"
                                                  select q1).ToList();

                                if (groupQ2.Count() > 0)
                                {
                                    double? calc_amount = 0;
                                    foreach (var jl in groupQ2)
                                    {
                                        calc_amount = calc_amount + jl.amt;
                                    }

                                    decimal amount = 0;
                                    amount = Convert.ToDecimal(calc_amount);
                                    amount = Decimal.Round(amount, 2);

                                    JournalEntryLine[] lines = new JournalEntryLine[2];
                                    for (int i = 0; i < lines.Count(); i++)
                                    {
                                        JournalEntryLine line = new JournalEntryLine();

                                        RecordRef refBusinessChannel = new RecordRef();
                                        refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                        line.@class = refBusinessChannel;

                                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                        StringCustomFieldRef scfr = new StringCustomFieldRef();
                                        scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                        scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                        scfr.value = j.mc_country_internalID;
                                        cfrList[0] = scfr;

                                        line.customFieldList = cfrList;

                                        if (j.mono.Equals("E"))
                                        {
                                            line.memo = "BCAS REPLACEMENT SALES " + j.jos_country_tag;
                                        }
                                        else
                                        {
                                            line.memo = "BCAS SALES " + j.jos_country_tag;
                                        }

                                        RecordRef refEntity = new RecordRef();
                                        if (j.jos_country_tag.Equals("ID"))
                                        {
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                            line.entity = refEntity;
                                        }
                                        else if (j.jos_country_tag.Equals("MY"))
                                        {
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                            line.entity = refEntity;
                                        }
                                        else if (j.jos_country_tag.Equals("SG"))
                                        {
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                            line.entity = refEntity;
                                        }

                                        if (i == 0)
                                        {
                                            if (journalAcc[i].coa_glType.Equals("DEBIT"))
                                            {
                                                RecordRef refDebit = new RecordRef();
                                                refDebit.internalId = journalAcc[i].coa_account_internalID;
                                                line.account = refDebit;

                                                line.debit = Convert.ToDouble(amount);
                                                line.debitSpecified = true;
                                            }
                                        }
                                        else if (i == 1)
                                        {
                                            if (journalAcc[i].coa_glType.Equals("CREDIT"))
                                            {
                                                RecordRef refCredit = new RecordRef();
                                                refCredit.internalId = journalAcc[i].coa_account_internalID;
                                                line.account = refCredit;

                                                line.credit = Convert.ToDouble(amount);
                                                line.creditSpecified = true;
                                            }
                                        }
                                        lines[i] = line;
                                    }
                                    jell.line = lines;
                                    je.lineList = jell;
                                    jeList[jnCount] = je;

                                    rowCount = jnCount + 1;

                                    //Insert query2 list into bcas_journalcalclist - WY-03.SEPT.2014
                                    var insertJournal = "insert into bcas_journalcalclist (jcl_amt,jcl_job_ID,jcl_businessChannel_code,jcl_country_tag, " +
                                                        "jcl_country_internalID,jcl_currency_internalID,jcl_jos_rangeTo,jcl_moNo,jcl_createdDate,jcl_rangeFrom,jcl_rangeTo) " +
                                                        "values ( '" + Convert.ToDouble(amount) + "', '" + j.jos_job_ID + "', '" + j.jos_businessChannel_code  + "', '" + j.jos_country_tag + "', " +
                                                        "'" + j.mc_country_internalID + "', '" + j.mc_currency_internalID + "', '" + convertDateToString(Convert.ToDateTime(j.jos_rangeTo)) + "', " +
                                                        "'" + j.mono + "', '" + convertDateToString(DateTime.Now) + "', '" + convertDateToString(rangeFrom) + "', '" + convertDateToString(rangeTo) + "') ";
                                    this.DataFromNetsuiteLog.Debug(insertJournal);
                                    entities.Database.ExecuteSqlCommand(insertJournal);

                                       
                                    String refNo = "JOBORDSCAN.JOS_JOB_ID." + j.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + j.mono;
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-JOURNAL', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug(insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);


                                    var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                                        "and jos_job_ID = '" + j.jos_job_ID + "' " +
                                                        "and jos_moNo like '" + j.mono + "%' " +
                                                        "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                        "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                    this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    jnCount++;

                                    status = true; //Change from first loop into second loop - WY-04.SEPT.2014
                                }
                                }
                                #endregion

                                //status = true;
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASJournal Exception: " + ex.ToString());
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
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(jeList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateTask2 = "update wms_jobordscan set jos_netsuiteProgress = '" + jobID + "' where jos_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                    "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                    "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateTask2);
                                entities.Database.ExecuteSqlCommand(updateTask2);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-JOURNAL' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-JOURNAL' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("BCASJournal: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASJournal: Login Netsuite failed.");
                }
            }

            //logout();
            return status;
        }
        public Boolean BCASSalesOrderOLD(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;

            // schedule an hour later than BCASDeductDummyUpdate
            //Boolean BCASDeCommitUpdateStatus = BCASDeCommitUpdate(rangeFrom, rangeTo);
            //if (BCASDeCommitUpdateStatus == true)
            //{

            this.DataFromNetsuiteLog.Info("BCASSalesOrder ***************");
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
                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrder: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASSalesOrder: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        List<String> fulfillItem = new List<String>();
                        List<Int32> fulfillQty = new List<Int32>();

                        var IDquery = (from q1 in entities.wms_jobordscan
                                       where q1.jos_businessChannel_code == "BC"
                                       && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                       && q1.jos_country_tag == "ID"
                                       select new { q1.jos_job_ID, q1.jos_moNo })
                                      .Distinct()
                                      .ToList();

                        List<string> _IDjob = new List<string>();
                        foreach (var q1 in IDquery)
                        {
                            _IDjob.Add(q1.jos_job_ID + "-" + q1.jos_moNo);
                        }
                        
                        var query1 = (from q1 in entities.wms_jobordscan
                                        where q1.jos_businessChannel_code == "BC" 
                                        && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                        select new
                                        {
                                            q1.jos_job_ID,
                                            q1.jos_businessChannel_code,
                                            q1.jos_country_tag,
                                            q1.jos_rangeTo,
                                            mono = q1.jos_moNo.Substring(0, 1)
                                        })
                                        .Distinct()
                                        .OrderBy(x => x.jos_businessChannel_code)
                                        .ThenBy(y => y.jos_country_tag)
                                        .ToList();

                        //status = true;
                        this.DataFromNetsuiteLog.Info("BCASSalesOrder: " + query1.Count() + " records to update.");
                        SalesOrder[] soList = new SalesOrder[query1.Count()];

                        foreach (var q1 in query1)
                        {
                            try
                            {
                                SalesOrder so = new SalesOrder();

                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.BCAS_SALES_CUSTOMFORM_MY;
                                so.customForm = refForm;

                                so.tranDate = DateTime.Now;
                                //so.tranDate = DateTime.Now.AddDays(-2); //for year end closing
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                so.@class = refClass;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                so.location = refLocation;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                if (q1.mono.Equals("E"))
                                {
                                    so.memo = "BCAS REPLACEMENT SALES";
                                }
                                else
                                {
                                    so.memo = "BCAS SALES";
                                }

                                RecordRef refEntity = new RecordRef();
                                if (q1.jos_country_tag.Equals("ID"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                    so.entity = refEntity;
                                }
                                else if (q1.jos_country_tag.Equals("MY"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                    so.entity = refEntity;
                                }
                                else if (q1.jos_country_tag.Equals("SG"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                    so.entity = refEntity;
                                }

                                //RecordRef refShipAddr = new RecordRef();
                                //refShipAddr.internalId = "1";
                                //so.shipAddressList = refShipAddr;

                                #region ID Sales
                                if (q1.jos_country_tag.Equals("ID"))
                                {
                                    //var query3 = (from ji in entities.netsuite_jobitem
                                    //              where ji.nsji_nsj_jobID == q1.jos_job_ID
                                    //              && _IDjob.Contains(ji.nsji_nsj_jobID + "-" + ji.nsji_moNo)
                                    //              select ji).ToList();

                                    var query3 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                  && ji.of_moNo.StartsWith(q1.mono) 
                                                  && _IDjob.Contains(ji.of_jobID + "-" + ji.of_moNo)
                                                  && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                  && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                  && nstask.rnt_status == "TRUE"
                                                  select ji).ToList();

                                    var groupQ3 = from p in query3
                                                    let k = new
                                                    {
                                                        //itemInternalID = p.nsji_item_internalID,
                                                        //fulFillQty = p.nsji_item_qty
                                                        itemInternalID = p.of_item_internalID,
                                                        //fulFillQty = p.of_ordQty
                                                    }
                                                    group p by k into g
                                                    select new
                                                    {
                                                        item = g.Key.itemInternalID,
                                                        //fulFillQty = g.Sum(p => p.nsji_item_qty)
                                                        fulFillQty = g.Sum(p => p.of_ordQty)
                                                    };

                                    if (groupQ3.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ3.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ3)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            soi.amount = 0;
                                            soi.amountSpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region MY Sales
                                else if (q1.jos_country_tag.Equals("MY"))
                                {
                                    var query2 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                    && ji.of_moNo.StartsWith(q1.mono) 
                                                    && ji.of_rangeTo > rangeFrom 
                                                    && ji.of_rangeTo <= rangeTo
                                                    && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                    && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                    && nstask.rnt_status == "TRUE"
                                                    select new { ji.of_item_internalID, qty = (ji.of_ordFulfillQty * ji.of_ordSkuQty) }).ToList();

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.of_item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      fulFillQty = g.Sum(p => p.qty)
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ2)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            soi.amount = 0;
                                            soi.amountSpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region SG Sales
                                else
                                {
                                    var query2 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                    && ji.of_moNo.StartsWith(q1.mono) 
                                                    && ji.of_rangeTo > rangeFrom 
                                                    && ji.of_rangeTo <= rangeTo
                                                    && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                    && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                    && nstask.rnt_status == "TRUE"
                                                    select new { ji.of_item_internalID, qty = (ji.of_ordFulfillQty * ji.of_ordSkuQty) }).ToList();

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.of_item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      fulFillQty = g.Sum(p => p.qty)
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ2)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            //soi.amount = 0;
                                            //soi.amountSpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASSalesOrder Exception: (" + q1.jos_job_ID + "," + q1.jos_rangeTo + ")" + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of bcas SO

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(soList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-SALES ORDER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                // stop update the NS job id 2014-DEC-07
                                //var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + jobID + "' where of_netsuiteSalesorder ='" + gjob_id.ToString() + "' " +
                                //                    "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                //                    "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " ;
                                //this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                //entities.Database.ExecuteSqlCommand(updateBcasFulfill);
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-SALES ORDER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrder: Login Netsuite failed.");
                }
            }//end of scope1
            //}
            return status;
        }
        public Boolean BCASSalesOrderAdhoc()
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            Boolean status = false;
            this.DataFromNetsuiteLog.Info("BCASSalesOrderAdHoc ***************");

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
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASSalesOrderAdHoc: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrderAdHoc: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASSalesOrderAdHoc: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        List<String> fulfillItem = new List<String>();
                        List<Int32> fulfillQty = new List<Int32>();
                         
                        var query1 = (from q1 in entities.bcas_sep_sales
                                      select new
                                      {
                                          q1.country,
                                          salesType = q1.sales_type
                                      }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("BCASSalesOrderAdHoc: " + query1.Count() + " records to update.");
                        SalesOrder[] soList = new SalesOrder[query1.Count()];

                        foreach (var q1 in query1)
                        {
                            try
                            {
                                SalesOrder so = new SalesOrder();

                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.BCAS_SALES_CUSTOMFORM_MY;
                                so.customForm = refForm;

                                so.tranDate = DateTime.Now;
                                //so.tranDate = DateTime.Now.AddDays(-2); //for year end closing
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                so.@class = refClass;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                so.location = refLocation;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                if (q1.salesType.Equals("REPLACEMENT"))
                                {
                                    so.memo = "BCAS REPLACEMENT SALES";
                                }
                                else
                                {
                                    so.memo = "BCAS SALES";
                                } 

                                RecordRef refEntity = new RecordRef();
                                if (q1.country.Equals("ID"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                    so.entity = refEntity;
                                }
                                else if (q1.country.Equals("MY"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                    so.entity = refEntity;
                                }
                                else if (q1.country.Equals("SG"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                    so.entity = refEntity;
                                }
                                  
                                #region Other Sales 
                                var query2 = (from bss in entities.bcas_sep_sales
                                              join mi in entities.map_item on bss.isbn equals mi.mi_item_isbn
                                              where bss.country == q1.country && bss.sales_type == q1.salesType 
                                             select new { qty = bss.quantity, mi.mi_item_internalID}).ToList();

                                var groupQ2 = from p in query2
                                                let k = new
                                                {
                                                    itemInternalID = p.mi_item_internalID,
                                                    fulFillQty = p.qty
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    item = g.Key.itemInternalID,
                                                    fulFillQty = g.Sum(p => p.qty)
                                                };

                                if (groupQ2.Count() > 0)
                                {
                                    SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                    SalesOrderItemList soil = new SalesOrderItemList();
                                    Int32 itemCount = 0;
                                    Int32 itemQty = 0;

                                    foreach (var item in groupQ2)
                                    {
                                        var query3 = (from bsl in entities.bcas_sep_loaded
                                                      join mi in entities.map_item on bsl.item_isbn equals mi.mi_item_isbn
                                                      where bsl.type == q1.salesType && mi.mi_item_internalID == item.item
                                                      select new { bsl.my_qty,bsl.sg_qty,bsl.id_qty }).ToList();
                                        if (query3.Count() > 0)
                                        {
                                            switch (q1.country)
                                            {
                                                case "SG":
                                                    itemQty = Convert.ToInt32(item.fulFillQty) - Convert.ToInt32(query3[0].sg_qty);
                                                    break;
                                                case "MY":
                                                    itemQty = Convert.ToInt32(item.fulFillQty) - Convert.ToInt32(query3[0].my_qty);
                                                    break;
                                                case "ID":
                                                    itemQty = Convert.ToInt32(item.fulFillQty) - Convert.ToInt32(query3[0].id_qty);
                                                    break;
                                            }
                                        }
                                        else
                                        {
                                            itemQty = Convert.ToInt32(item.fulFillQty);
                                        }

                                        SalesOrderItem soi = new SalesOrderItem();

                                        RecordRef refItem = new RecordRef();
                                        refItem.type = RecordType.inventoryItem;
                                        refItem.typeSpecified = true;
                                        refItem.internalId = item.item;
                                        soi.item = refItem;

                                        Int32 testQty = Convert.ToInt32(item.fulFillQty);
                                        soi.quantity = Convert.ToDouble(itemQty);
                                        soi.quantitySpecified = true;

                                        soi.amount = 0;
                                        soi.amountSpecified = true;

                                        soii[itemCount] = soi;
                                        itemCount++;
                                    }
                                    soil.item = soii;
                                    so.itemList = soil;
                                    soList[soCount] = so;

                                    rowCount = soCount + 1; 

                                    soCount++;
                                    status = true;
                                } 
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASSalesOrderAdHoc Exception: (" + q1.country + "," + q1.salesType + ")" + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of bcas SO

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(soList);
                                String jobID = job.jobId;
                                 
                            }
                        }
                        else if (rowCount == 0)
                        { 
                        }
                        scope1.Complete();
                    }//end of sdeEntities
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrderAdHoc: Login Netsuite failed.");
                }
            } 
            return status;
        }
        public Boolean BCASSalesOrderFulfillment(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            this.DataFromNetsuiteLog.Info("BCASSalesOrderFulfillment ***************");
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
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrderFulfillment: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //
                //Boolean loginStatus = login();
                if (loginStatus == true)
                {

                    this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var salesOrder = (from t in entities.requestnetsuite_task
                                            where t.rnt_updatedDate > rangeFrom
                                            && t.rnt_updatedDate <= rangeTo
                                            && t.rnt_description == "BCAS-SALES ORDER"
                                            && t.rnt_status == "TRUE"
                                            select t).ToList();

                        this.DataFromNetsuiteLog.Info("BCASSalesOrderFulfillment: " + salesOrder.Count() + " records to update.");
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

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                ReadResponse rrSO = netsuiteService.initialize(recSO);
                                Record rSO = rrSO.record;

                                ItemFulfillment iff1 = (ItemFulfillment)rSO;
                                ItemFulfillment iff2 = new ItemFulfillment();

                                if (iff1 != null)
                                {
                                    string soInternalId = iff1.createdFrom.internalId;
                                    DateTime soTranDate = iff1.tranDate;

                                    ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                    iff2.createdFrom = refCreatedFrom;

                                     //iff2.tranDate = DateTime.Now.AddDays(-2); //for year end closing
                                    //iff2.tranDate = DateTime.Now;
                                    iff2.tranDate = soTranDate;
                                    iff2.tranDateSpecified = true;

                                    ////Added for Advanced Inventory
                                    iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                    iff2.shipStatusSpecified = true;

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

                                            //cpng start
                                            if (ifitemlist.item[i].item.internalId != @Resource.BCAS_MY_DC_INTERNALID
                                                && ifitemlist.item[i].item.internalId != @Resource.BCAS_MY_ROUDING_INTERNALID)
                                            {
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = ifitemlist.item[i].quantity;
                                                IA.quantitySpecified = true;
                                                IA.binNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN_COMMIT };
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iffi.inventoryDetail = ID;
                                            }
                                            //cpng end

                                            ifitems[count1] = iffi;
                                            count1++;
                                        }

                                        ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                        ifil1.item = ifitems;
                                        iff2.itemList = ifil1;

                                        iffList[fulFillCount] = iff2;
                                        rowCount = fulFillCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'UPD-STATUS.BCAS-SALES ORDER', 'REQUESTNETSUITETASK.RNT_ID." + so.rnt_id.ToString() + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + so.rnt_nsInternalId + "','" + so.rnt_createdFromInternalId + "')";
                                        this.DataFromNetsuiteLog.Debug(insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var insSalesTrx = "insert into bcas_salestransaction (bst_createdDate, bst_ifSeqNo, bst_soInternalID, " +
                                            "bst_postingDate,bst_ifJobID) " +
                                            "values ('" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', '" + soInternalId + "', " +
                                            "'" + convertDateToString(Convert.ToDateTime(soTranDate)) + "'," +
                                            "'" + gjob_id.ToString() + "')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + insSalesTrx);
                                        entities.Database.ExecuteSqlCommand(insSalesTrx);

                                        fulFillCount++;
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: Sales order internalID_moNo: " + so.rnt_createdFromInternalId);
                                        status = true;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASSalesOrderFulfillment Exception: " + ex.ToString());
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
                                //var updSalesTrxTest = "update bcas_salestransaction set bst_ifJobID = '" + gjob_id.ToString() + "' where bst_ifJobID = '" + gjob_id.ToString() + "'";
                                //this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + updSalesTrxTest);
                                //entities.Database.ExecuteSqlCommand(updSalesTrxTest);

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(iffList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updSalesTrx = "update bcas_salestransaction set bst_ifJobID = '" + jobID + "' where bst_ifJobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.BCAS-SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.BCAS-SALES ORDER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("BCASSalesOrderFulfillment: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrderFulfillment: Login Netsuite failed.");
                }
                //}
            }//end of scope1

            return status;
        }
        public Boolean BCASSalesOrderApprove(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            /* David added on 26-May-2015 #675*/
            this.DataFromNetsuiteLog.Info("BCASDummyOrdersApprove ***************");
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
                //  appinfo.applicationId = appID;
                netsuiteService.applicationInfo = appinfo;
                try
                {
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASDummyOrdersApprove: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //
                // Boolean loginStatus = login();
                if (loginStatus == true)
                {

                    this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Guid gjob_id = Guid.NewGuid();

                        var salesOrder = (from t in entities.requestnetsuite_task
                                          where t.rnt_updatedDate > rangeFrom
                                          && t.rnt_updatedDate <= rangeTo
                                          && t.rnt_description == "UPD-STATUS.BCAS-SALES ORDER"
                                          && t.rnt_status == "TRUE"
                                          select t).ToList();
                        try
                        {
                            this.DataFromNetsuiteLog.Info("BCASDummyOrdersApprove: Total Bcas Sales Orders = " + salesOrder.Count() + ".");
                            if (salesOrder.Count() > 0)
                            {
                                SalesOrder[] updateList = new SalesOrder[4];
                                string[] array_netsuiteID = new string[] { @Resource.BCAS_DUMMYSALES_INTERNALID, @Resource.BCAS_DUMMYSALES_INTERNALID_ID, @Resource.BCAS_DUMMYSALES_INTERNALID_SG, @Resource.BCAS_DUMMYSALES_INTERNALID_TH };

                                Int32 rowCount = 0;
                                for (int i = 0; i < array_netsuiteID.Count(); i++)
                                {
                                    rowCount = i + 1;

                                    SalesOrder tran = new SalesOrder();
                                    tran.internalId = array_netsuiteID[i];
                                    tran.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                    tran.orderStatusSpecified = true;
                                    updateList[i] = tran;

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                    "rnt_seqNO,rnt_createdFromInternalID) values ('UPDATE', 'UPD-STATUS.BCAS-DUMMYORDERS-APPROVE', 'REQUESTNETSUITE_TASK.BCAS-DUMMYORDERS." + array_netsuiteID[i] + "', '" + gjob_id.ToString() + "'," +
                                    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);
                                }

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncUpdateList(updateList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'UPD-STATUS.BCAS-DUMMYORDERS-APPROVE' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                            else
                            {
                                this.DataFromNetsuiteLog.Debug("BCASDummyOrdersApprove: BCAS fulfillments have not complete yet !");
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("BCASDummyOrdersApprove Exception: " + ex.ToString());
                        }
                    }
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASDummyOrdersApprove: Login Netsuite failed.");
                }
            }
            return status;
        }

        public Boolean BCASDeductDummyUpdate(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            // schedule an hour earlier than BCASSalesOrder
            this.DataFromNetsuiteLog.Info("BCASDeCommitUpdate ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
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
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: Login Netsuite success.");
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
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate: Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    //
                    //Boolean loginStatus = login();
                    if (loginStatus == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: Login Netsuite success.");
                        using (sdeEntities entities = new sdeEntities())
                        {
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();
                            List<string> _DataExtracted = new List<string>();

                            #region Get ordered qty
                            //List<String> MYjob = new List<String>();

                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: queryMY");
                            //var queryMY = (from q1 in entities.wms_jobordscan
                            //               where q1.jos_rangeTo > rangeFrom
                            //               && q1.jos_rangeTo <= rangeTo
                            //               && q1.jos_businessChannel_code == "BC"
                            //               && q1.jos_country_tag == "MY"
                            //               && q1.jos_netsuiteProgressDummy == null
                            //               select new
                            //               {
                            //                   q1.jos_moNo,
                            //                   //isFirstRun = q1.jos_netsuiteProgressDummy == null ? "Y" : "N",
                            //                   //q1.jos_job_ID,
                            //                   //q1.jos_ordRecNo
                            //               })
                            //               .Distinct()
                            //               .ToList();

                            //foreach (var qJobID in queryMY)
                            //{
                            //    if (qJobID.isFirstRun == "Y")
                            //    {
                            //        //MYjob.Add(qJobID.jos_job_ID + qJobID.jos_moNo + qJobID.jos_ordRecNo);
                            //        MYjob.Add(qJobID.jos_job_ID + qJobID.jos_moNo);
                            //    }
                            //}
                            //this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + MYjob.Count());
                            /////

                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: query2 start");
                            //foreach (var q1 in query1)
                            //{ 
                            var query2 = (from josp in entities.wms_jobordscan_pack
                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                          join jos in entities.wms_jobordscan on new { jobID=josp.josp_jobID, moNo=josp.josp_moNo, ordRecNo=josp.josp_ordRecNo } equals new { jobID=jos.jos_job_ID, moNo=jos.jos_moNo, ordRecNo=jos.jos_ordRecNo }
                                          //where MYjob.Contains(josp.josp_jobID + josp.josp_moNo + josp.josp_ordRecNo)
                                          where jos.jos_rangeTo > rangeFrom
                                          && jos.jos_rangeTo <= rangeTo
                                          && jos.jos_businessChannel_code == "BC"
                                          && jos.jos_country_tag == "MY"
                                          && jos.jos_netsuiteProgressDummy == null
                                          select new
                                          {
                                              //jos.jos_jobordscan_ID,
                                              jos_job_ID = josp.josp_jobID,
                                              jos_moNo = josp.josp_moNo,
                                              jos_rangeTo = josp.josp_rangeTo,
                                              jos_country_tag = "MY",
                                              josp.josp_pack_ID,
                                              jomp.nsjomp_jobOrdMaster_pack_ID,
                                              jompd.nsjompd_item_ID,
                                              jompd.nsjompd_isbn,
                                              item_internalID = (jompd.nsjompd_item_internalID),
                                              jomp.nsjomp_ordQty,
                                              josp.josp_ordFulFill,
                                              jompd.nsjompd_sku_qty,
                                              calcqty = (jomp.nsjomp_ordFulfill * jompd.nsjompd_sku_qty),//cpng
                                              calcfulfillQty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty),
                                              qtyUnFulFill = (josp.josp_ordUnFulFill * jompd.nsjompd_sku_qty)// cpng
                                          }).ToList();
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: query2 end");

                            //this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: query2 start");
                            //var query2 = (from p in query1
                            //              where p.jos_rangeTo > rangeFrom
                            //              && p.jos_rangeTo <= rangeTo
                            //              select new
                            //              {
                            //                  jos_job_ID = p.jos_job_ID,
                            //                  jos_moNo = p.jos_moNo,
                            //                  jos_rangeTo = p.jos_rangeTo,
                            //                  jos_country_tag = p.jos_country_tag,
                            //                  p.josp_pack_ID,
                            //                  p.nsjomp_jobOrdMaster_pack_ID,
                            //                  p.nsjompd_item_ID,
                            //                  p.nsjompd_isbn,
                            //                  p.item_internalID,
                            //                  p.nsjomp_ordQty,
                            //                  p.josp_ordFulFill,
                            //                  p.nsjompd_sku_qty,
                            //                  p.calcqty,
                            //                  p.calcfulfillQty,
                            //                  p.qtyUnFulFill
                            //              }).ToList();
                            //this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: query2 end");



                            #region data4bcasFulfill
                            var groupQ3 = from p in query2
                                          let k = new
                                          {
                                              p.jos_job_ID,
                                              p.jos_moNo,
                                              p.jos_rangeTo,
                                              p.jos_country_tag,
                                              p.josp_pack_ID,
                                              p.nsjomp_jobOrdMaster_pack_ID,
                                              p.nsjompd_item_ID,
                                              p.nsjompd_isbn,
                                              p.item_internalID,
                                              p.nsjompd_sku_qty,
                                              //p.calcqty,
                                              //p.calcfulfillQty
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              g.Key.jos_job_ID,
                                              g.Key.jos_moNo,
                                              g.Key.jos_rangeTo,
                                              g.Key.jos_country_tag,
                                              g.Key.josp_pack_ID,
                                              g.Key.nsjomp_jobOrdMaster_pack_ID,
                                              g.Key.nsjompd_item_ID,
                                              g.Key.nsjompd_isbn,
                                              g.Key.item_internalID,
                                              ordQty = g.Sum(p => p.nsjomp_ordQty),
                                              ordFulFill = g.Sum(p => p.josp_ordFulFill),
                                              g.Key.nsjompd_sku_qty,
                                              qty = g.Sum(p => p.calcqty),
                                              fulfillQty = g.Sum(p => p.calcfulfillQty)
                                          };

                            foreach (var data in groupQ3)
                            {
                                _DataExtracted.Add(data.jos_job_ID + "," + data.jos_moNo + "," + convertDateToString(Convert.ToDateTime(data.jos_rangeTo)) + "," + data.jos_country_tag + "," + data.josp_pack_ID + "," + data.nsjomp_jobOrdMaster_pack_ID + "," + data.nsjompd_item_ID + "," + data.item_internalID + "," + data.ordQty.ToString() + "," + data.ordFulFill.ToString() + "," + data.nsjompd_sku_qty.ToString() + "," + data.nsjompd_isbn);
                            }
                            #endregion

                            //cpng start
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: groupNotFulfill start");
                            Hashtable htWMSItemsQTY = new Hashtable(); // cpng added: item qty sync down to wms
                            var groupNotFulfill = from p in query2
                                          let k = new
                                          {
                                              itemInternalID = p.item_internalID,
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              itemInternalID = g.Key.itemInternalID,
                                              SyncQty = g.Sum(p => p.calcqty),//cpng
                                              fulFillQty = g.Sum(p => p.calcfulfillQty),
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
                            // //where MYjob.Contains(josp.josp_jobID + josp.josp_moNo + josp.josp_ordRecNo)
                            // where MYjob.Contains(josp.josp_jobID + josp.josp_moNo)
                            // && josp.josp_rangeTo > rangeFrom
                            // && josp.josp_rangeTo <= rangeTo
                            // select new { josp, jomp }).ToList().ForEach(x => x.josp.josp_ordUnFulFill = x.jomp.nsjomp_ordQty - x.josp.josp_ordFulFill);
                            //entities.SaveChanges();

                            //string jobIDUnfulfill = String.Join("', '", MYjob.ToArray());

                            //if (!string.IsNullOrEmpty(jobIDUnfulfill))
                            //{
                                var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                                " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                                " join netsuite_jobordmaster_packdetail jompd on jomp.nsjomp_jobOrdMaster_pack_ID = jompd.nsjompd_jobOrdMaster_pack_ID " +
                                " join wms_jobordscan jos on  jos.jos_job_ID = josp.josp_jobID and  jos.jos_moNo = josp.josp_moNo and  jos.jos_ordRecNo = josp.josp_ordRecNo " +
                                " set josp.josp_ordUnFulFill = jomp.nsjomp_ordFulfill - josp.josp_ordFulFill " +
                                " where jos.jos_businessChannel_code = 'BC' " +
                                " and josp.josp_rangeTo > '" + convertDateToString(rangeFrom) + "' and josp.josp_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                " and jos.jos_country_tag = 'MY' " +
                                " and jos.jos_netsuiteProgressDummy is null ";

                                this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                                entities.Database.ExecuteSqlCommand(updateUnfulfill2);
                            //}

                            //cpng end
                            #endregion

                            if (_DataExtracted.Count() > 0)
                            {
                                for (int _i = 0; _i < _DataExtracted.Count(); _i++)
                                {
                                    string[] array_extracted = Convert.ToString(_DataExtracted[_i]).Split(',');
                                    Int32 _ordQty = 0;
                                    Int32 _fulfillQty = 0;
                                    Int32 _skuQty = 0;

                                    if (!String.IsNullOrEmpty(array_extracted[8]) && (!String.IsNullOrWhiteSpace(array_extracted[8])))
                                    {
                                        _ordQty = Convert.ToInt32(array_extracted[8]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[9]) && (!String.IsNullOrWhiteSpace(array_extracted[9])))
                                    {
                                        _fulfillQty = Convert.ToInt32(array_extracted[9]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[10]) && (!String.IsNullOrWhiteSpace(array_extracted[10])))
                                    {
                                        _skuQty = Convert.ToInt32(array_extracted[10]);
                                    }

                                    if (_fulfillQty > 0)  //kang - only fulfillqty > 0 will generate sales order
                                    {
                                        var insertBcasExtracted = "insert into bcas_ordersfulfill (of_jobID, of_moNo, of_rangeTo, of_country_tag, of_pack_ID, of_jobOrdMaster_ID, of_item_ID" +
                                       ", of_item_internalID, of_ordQty, of_ordFulfillQty, of_ordSkuQty, of_createdDate, of_netsuiteDummySo, of_netsuiteSalesorder, of_netsuiteAdjustment, of_item_ISBN) values ('" + array_extracted[0] + "', '" + array_extracted[1] + "','" + array_extracted[2] + "','" + array_extracted[3] + "'," +
                                       "'" + array_extracted[4] + "','" + array_extracted[5] + "', '" + array_extracted[6] + "', '" + array_extracted[7] + "', " + _ordQty + "," +
                                       _fulfillQty + ", " + _skuQty + ", '" + convertDateToString(DateTime.Now) + "', '" + gjob_id.ToString() + "', '', '', '" + array_extracted[11] + "') ";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + insertBcasExtracted);
                                        entities.Database.ExecuteSqlCommand(insertBcasExtracted);
                                    }
                                }
                                Int32 soCount = 0;
                                rowCount = soCount + 1;
                                status = true;
                            }//end of _DataExtracted

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    var updateBcasFulfillPack = "update sde.bcas_ordersfulfill aa inner join sde.netsuite_jobordmaster_pack cc " +
                                                        "on aa.of_jobID=cc.nsjomp_job_ID and aa.of_pack_ID=cc.nsjomp_jobOrdMaster_pack_ID " +
                                                        "set aa.of_ordPack=cc.nsjomp_ordPack, " +
                                                        "aa.of_ordPackQty=cc.nsjomp_ordQty, " +
                                                        "aa.of_ordPackPrice=cc.nsjomp_ordPrice, " +
                                                        "aa.of_ordPackGst=cc.nsjomp_gstamount " +
                                                        "where aa.of_netsuiteDummySo = '" + gjob_id.ToString() + "' " +
                                                        "and aa.of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                        "and aa.of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                        "and aa.of_country_tag = 'MY' ";
                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + updateBcasFulfillPack);
                                    entities.Database.ExecuteSqlCommand(updateBcasFulfillPack);

                                    String jobID = gjob_id.ToString();

                                    if ((jobID != null))
                                    {

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID.ALL" + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + ".ALL";
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'BCAS-DEDUCT DUMMY SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'TRUE', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + @Resource.BCAS_DUMMYSALES_INTERNALID + "', '')";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_status = 'UPLOADED', rn_completedAt = '" + convertDateToString(DateTime.Now) + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT DUMMY SALES ORDER' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updateTask2 = "update wms_jobordscan set jos_netsuiteProgressDummy = '" + jobID + "' where jos_netsuiteProgressDummy is null " +
                                                            "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and jos_country_tag = 'MY' " +
                                                            "and jos_businessChannel_code = 'BC'";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + updateTask2);
                                        entities.Database.ExecuteSqlCommand(updateTask2);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteDummySo = '" + jobID + "' where of_netsuiteDummySo = '" + gjob_id.ToString() + "' " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_country_tag = 'MY' ";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);


                                        var insertRequestNetsuiteSG = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                                            "('BCAS-DEDUCT SG DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(Convert.ToDateTime(rangeFrom)) + "','" + convertDateToString(Convert.ToDateTime(rangeTo)) + "')";

                                        this.DataFromNetsuiteLog.Debug("PushNetsuite: " + insertRequestNetsuiteSG);
                                        entities.Database.ExecuteSqlCommand(insertRequestNetsuiteSG);

                                        var insertRequestNetsuiteTH = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                            "('BCAS-DEDUCT TH DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(Convert.ToDateTime(rangeFrom)) + "','" + convertDateToString(Convert.ToDateTime(rangeTo)) + "')";

                                        this.DataFromNetsuiteLog.Debug("PushNetsuite: " + insertRequestNetsuiteTH);
                                        entities.Database.ExecuteSqlCommand(insertRequestNetsuiteTH);

                                        var insertRequestNetsuite = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                            "('BCAS-DEDUCT ID DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(Convert.ToDateTime(rangeFrom)) + "','" + convertDateToString(Convert.ToDateTime(rangeTo)) + "')";

                                        this.DataFromNetsuiteLog.Debug("PushNetsuite: " + insertRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(insertRequestNetsuite);

                                        UnfulfillBinTransfer(htWMSItemsQTY, jobID, convertDateToString(rangeFrom), convertDateToString(rangeTo));
                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var insertRequestNetsuiteSG = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                    "('BCAS-DEDUCT SG DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "')";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + insertRequestNetsuiteSG);
                                entities.Database.ExecuteSqlCommand(insertRequestNetsuiteSG);

                                var insertRequestNetsuite = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                    "('BCAS-DEDUCT ID DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "')";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + insertRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(insertRequestNetsuite);

                                var insertRequestNetsuiteTH = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_jobID,rn_updatedDate,rn_rangeFrom,rn_rangeTo) values " +
                                    "('BCAS-DEDUCT TH DUMMY SALES ORDER','" + convertDateToString(DateTime.Now) + "','START','','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "')";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate: " + insertRequestNetsuiteTH);
                                entities.Database.ExecuteSqlCommand(insertRequestNetsuiteTH);

                                scope1.Complete();
                            }
                        }
                        //logout();
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate: Login Netsuite failed.");
                    }
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("BCASDeductDummyUpdate Exception: " + ex.ToString());
                    status = false;
                }
            }//end of scope1
            return status;
        }
        public Boolean BCASDeductDummyUpdate_SG(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);


            // schedule an hour earlier than BCASSalesOrder
            this.DataFromNetsuiteLog.Info("BCASDeCommitUpdate SG ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
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
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG: Login Netsuite success.");
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
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate SG: Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    //
                    //Boolean loginStatus = login();
                    if (loginStatus == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : Login Netsuite success.");
                        using (sdeEntities entities = new sdeEntities())
                        {
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();
                            List<string> _DataExtracted = new List<string>();

                            #region Get ordered qty
                            var query2 = (from josp in entities.wms_jobordscan_pack
                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                          join jos in entities.wms_jobordscan on new { jobID = josp.josp_jobID, moNo = josp.josp_moNo, ordRecNo = josp.josp_ordRecNo } equals new { jobID = jos.jos_job_ID, moNo = jos.jos_moNo, ordRecNo = jos.jos_ordRecNo }
                                          where jos.jos_businessChannel_code == "BC"
                                          && josp.josp_rangeTo > rangeFrom
                                          && josp.josp_rangeTo <= rangeTo
                                          && jos.jos_country_tag == "SG"
                                          && jos.jos_netsuiteProgressDummy == null
                                          //&& jos.jos_moNo != "M58815"
                                          //&& josp.josp_pack_ID != "1e493f11-006c-49e3-b9a7-e5d2307d1cf9"
                                          //select new { item_internalID = (jompd.nsjompd_item_internalID), qty = (jomp.nsjomp_ordQty * jompd.nsjompd_sku_qty) }).ToList();
                                          select new
                                          {
                                              jos.jos_jobordscan_ID,
                                              jos.jos_job_ID,
                                              jos.jos_moNo,
                                              jos.jos_rangeTo,
                                              jos.jos_country_tag,
                                              josp.josp_pack_ID,
                                              jomp.nsjomp_jobOrdMaster_pack_ID,
                                              jompd.nsjompd_item_ID,
                                              jompd.nsjompd_isbn,
                                              item_internalID = (jompd.nsjompd_item_internalID),
                                              jomp.nsjomp_ordQty,
                                              josp.josp_ordFulFill,
                                              jompd.nsjompd_sku_qty,
                                              calcqty = (jomp.nsjomp_ordFulfill * jompd.nsjompd_sku_qty),//cpng
                                              calcfulfillQty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty),
                                              qtyUnFulFill = (josp.josp_ordUnFulFill * jompd.nsjompd_sku_qty)// cpng
                                          }).ToList();


                            #region data4bcasFulfill
                            var groupQ3 = from p in query2
                                          let k = new
                                          {
                                              p.jos_job_ID,
                                              p.jos_moNo,
                                              p.jos_rangeTo,
                                              p.jos_country_tag,
                                              p.josp_pack_ID,
                                              p.nsjomp_jobOrdMaster_pack_ID,
                                              p.nsjompd_item_ID,
                                              p.nsjompd_isbn,
                                              p.item_internalID,
                                              p.nsjompd_sku_qty,
                                              //p.calcqty,
                                              //p.calcfulfillQty
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              g.Key.jos_job_ID,
                                              g.Key.jos_moNo,
                                              g.Key.jos_rangeTo,
                                              g.Key.jos_country_tag,
                                              g.Key.josp_pack_ID,
                                              g.Key.nsjomp_jobOrdMaster_pack_ID,
                                              g.Key.nsjompd_item_ID,
                                              g.Key.nsjompd_isbn,
                                              g.Key.item_internalID,
                                              ordQty = g.Sum(p => p.nsjomp_ordQty),
                                              ordFulFill = g.Sum(p => p.josp_ordFulFill),
                                              g.Key.nsjompd_sku_qty,
                                              qty = g.Sum(p => p.calcqty),
                                              fulfillQty = g.Sum(p => p.calcfulfillQty)
                                          };

                            foreach (var data in groupQ3)
                            {
                                _DataExtracted.Add(data.jos_job_ID + "," + data.jos_moNo + "," + convertDateToString(Convert.ToDateTime(data.jos_rangeTo)) + "," + data.jos_country_tag + "," + data.josp_pack_ID + "," + data.nsjomp_jobOrdMaster_pack_ID + "," + data.nsjompd_item_ID + "," + data.item_internalID + "," + data.ordQty.ToString() + "," + data.ordFulFill.ToString() + "," + data.nsjompd_sku_qty.ToString() + "," + data.nsjompd_isbn);
                            }
                            #endregion

                            //cpng start
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG: groupNotFulfill start");
                            Hashtable htWMSItemsQTY = new Hashtable(); // cpng added: item qty sync down to wms
                            var groupNotFulfill = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      itemInternalID = g.Key.itemInternalID,
                                                      SyncQty = g.Sum(p => p.calcqty),//cpng
                                                      fulFillQty = g.Sum(p => p.calcfulfillQty),
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
                            // join jos in entities.wms_jobordscan on new { jobID = josp.josp_jobID, moNo = josp.josp_moNo, ordRecNo = josp.josp_ordRecNo } equals new { jobID = jos.jos_job_ID, moNo = jos.jos_moNo, ordRecNo = jos.jos_ordRecNo }
                            // where jos.jos_businessChannel_code == "BC"
                            // && josp.josp_rangeTo > rangeFrom
                            // && josp.josp_rangeTo <= rangeTo
                            // && jos.jos_country_tag == "SG"
                            // && jos.jos_netsuiteProgressDummy == null
                            // select new { josp, jomp }).ToList().ForEach(x => x.josp.josp_ordUnFulFill = x.jomp.nsjomp_ordQty - x.josp.josp_ordFulFill);
                            //entities.SaveChanges();

                            var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                            " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                            " join netsuite_jobordmaster_packdetail jompd on jomp.nsjomp_jobOrdMaster_pack_ID = jompd.nsjompd_jobOrdMaster_pack_ID " +
                            " join wms_jobordscan jos on  jos.jos_job_ID = josp.josp_jobID and  jos.jos_moNo = josp.josp_moNo and  jos.jos_ordRecNo = josp.josp_ordRecNo " +
                            " set josp.josp_ordUnFulFill = jomp.nsjomp_ordFulfill - josp.josp_ordFulFill " +
                            " where jos.jos_businessChannel_code = 'BC' " +
                            " and josp.josp_rangeTo > '" + convertDateToString(rangeFrom) + "' and josp.josp_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                            " and jos.jos_country_tag = 'SG' " +
                            " and jos.jos_netsuiteProgressDummy is null ";

                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                            entities.Database.ExecuteSqlCommand(updateUnfulfill2);

                            //cpng end

                            #endregion

                            if (_DataExtracted.Count() > 0)
                            {
                                for (int _i = 0; _i < _DataExtracted.Count(); _i++)
                                {
                                    string[] array_extracted = Convert.ToString(_DataExtracted[_i]).Split(',');
                                    Int32 _ordQty = 0;
                                    Int32 _fulfillQty = 0;
                                    Int32 _skuQty = 0;

                                    if (!String.IsNullOrEmpty(array_extracted[8]) && (!String.IsNullOrWhiteSpace(array_extracted[8])))
                                    {
                                        _ordQty = Convert.ToInt32(array_extracted[8]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[9]) && (!String.IsNullOrWhiteSpace(array_extracted[9])))
                                    {
                                        _fulfillQty = Convert.ToInt32(array_extracted[9]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[10]) && (!String.IsNullOrWhiteSpace(array_extracted[10])))
                                    {
                                        _skuQty = Convert.ToInt32(array_extracted[10]);
                                    }

                                    var insertBcasExtracted = "insert into bcas_ordersfulfill (of_jobID, of_moNo, of_rangeTo, of_country_tag, of_pack_ID, of_jobOrdMaster_ID, of_item_ID" +
                                        ", of_item_internalID, of_ordQty, of_ordFulfillQty, of_ordSkuQty, of_createdDate, of_netsuiteDummySo, of_netsuiteSalesorder, of_netsuiteAdjustment, of_item_ISBN) values ('" + array_extracted[0] + "', '" + array_extracted[1] + "','" + array_extracted[2] + "','" + array_extracted[3] + "'," +
                                        "'" + array_extracted[4] + "','" + array_extracted[5] + "', '" + array_extracted[6] + "', '" + array_extracted[7] + "', " + _ordQty + "," +
                                        _fulfillQty + ", " + _skuQty + ", '" + convertDateToString(DateTime.Now) + "', '" + gjob_id.ToString() + "', '', '', '" + array_extracted[11] + "') ";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + insertBcasExtracted);
                                    entities.Database.ExecuteSqlCommand(insertBcasExtracted);

                                }
                                Int32 soCount = 0;
                                rowCount = soCount + 1;
                                status = true;
                            }//end of _DataExtracted

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    String jobID = gjob_id.ToString();

                                    if ((jobID != null))
                                    {
                                        String refNo = "JOBORDSCAN.JOS_JOB_ID.ALL" + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + ".ALL";
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'BCAS-DEDUCT SG DUMMY SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'TRUE', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + @Resource.BCAS_DUMMYSALES_INTERNALID_SG + "', '')";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_status = 'UPLOADED', rn_completedAt = '" + convertDateToString(DateTime.Now) + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT SG DUMMY SALES ORDER' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updateTask2 = "update wms_jobordscan set jos_netsuiteProgressDummy = '" + jobID + "' where jos_netsuiteProgressDummy is null " +
                                                            "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and jos_country_tag = 'SG' " +
                                                            "and jos_businessChannel_code = 'BC'";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + updateTask2);
                                        entities.Database.ExecuteSqlCommand(updateTask2);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteDummySo = '" + jobID + "' where of_netsuiteDummySo = '" + gjob_id.ToString() + "' " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_country_tag = 'SG' ";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        UnfulfillBinTransfer(htWMSItemsQTY, jobID, convertDateToString(rangeFrom), convertDateToString(rangeTo));

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT SG DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        //logout();
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate SG : Login Netsuite failed.");
                    }
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("BCASDeductDummyUpdate SG  Exception: " + ex.ToString());
                    status = false;
                }
            }//end of scope1
            return status;
        }
        public Boolean BCASDeductDummyUpdate_ID(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            this.DataFromNetsuiteLog.Info("BCASDeCommitUpdate ID ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
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
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: Login Netsuite success.");
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
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate ID: Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    //
                    //  Boolean loginStatus = login();
                    if (loginStatus == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: Login Netsuite success.");
                        using (sdeEntities entities = new sdeEntities())
                        {
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();
                            List<string> _DataExtracted = new List<string>();

                            #region Get ordered qty
                            var IDquery = (from q1 in entities.wms_jobordscan
                                           where q1.jos_businessChannel_code == "BC"
                                           && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                           && q1.jos_country_tag == "ID"
                                           && q1.jos_netsuiteProgressDummy == null
                                           select new { q1.jos_job_ID, q1.jos_moNo })
                                          .Distinct()
                                          .ToList();

                            List<string> _IDjob = new List<string>();
                            foreach (var q1 in IDquery)
                            {
                                _IDjob.Add(q1.jos_job_ID + "-" + q1.jos_moNo);
                            }

                            var query2 = (from ji in entities.netsuite_jobitem
                                          where (_IDjob.Contains(ji.nsji_nsj_jobID + "-" + ji.nsji_moNo))
                                          select new
                                          {
                                              ji.nsji_nsj_jobID,
                                              ji.nsji_moNo,
                                              ji.nsji_item_ID,
                                              item_internalID = (ji.nsji_item_internalID),
                                              qty = (ji.nsji_item_qty),
                                              fulfillQty = (ji.nsji_item_qty)
                                          }).ToList();

                            foreach (var data in query2)
                            {
                                _DataExtracted.Add(data.nsji_nsj_jobID + "," + data.nsji_moNo + "," + convertDateToString(rangeTo) + ",ID,-,-," + data.nsji_item_ID + "," + data.item_internalID + "," + data.qty.ToString() + "," + data.fulfillQty.ToString() + "," + "1" + "," + data.nsji_item_ID);
                            }


                            #endregion

                            if (_DataExtracted.Count() > 0)
                            {
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: Number of data extracted is " + _DataExtracted.Count());
                                for (int _i = 0; _i < _DataExtracted.Count(); _i++)
                                {
                                    string[] array_extracted = Convert.ToString(_DataExtracted[_i]).Split(',');
                                    Decimal _ordQty = 0;
                                    Decimal _fulfillQty = 0;
                                    Decimal _skuQty = 0;

                                    if (!String.IsNullOrEmpty(array_extracted[8]) && (!String.IsNullOrWhiteSpace(array_extracted[8])))
                                    {
                                        _ordQty = Convert.ToDecimal(array_extracted[8]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[9]) && (!String.IsNullOrWhiteSpace(array_extracted[9])))
                                    {
                                        _fulfillQty = Convert.ToDecimal(array_extracted[9]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[10]) && (!String.IsNullOrWhiteSpace(array_extracted[10])))
                                    {
                                        _skuQty = Convert.ToDecimal(array_extracted[10]);
                                    }

                                    var insertBcasExtracted = "insert into bcas_ordersfulfill (of_jobID, of_moNo, of_rangeTo, of_country_tag, of_pack_ID, of_jobOrdMaster_ID, of_item_ID" +
                                        ", of_item_internalID, of_ordQty, of_ordFulfillQty, of_ordSkuQty, of_createdDate, of_netsuiteDummySo, of_netsuiteSalesorder, of_netsuiteAdjustment, of_item_ISBN) values ('" + array_extracted[0] + "', '" + array_extracted[1] + "','" + array_extracted[2] + "','" + array_extracted[3] + "'," +
                                        "'" + array_extracted[4] + "','" + array_extracted[5] + "', '" + array_extracted[6] + "', '" + array_extracted[7] + "', " + _ordQty + "," +
                                        _fulfillQty + ", " + _skuQty + ", '" + convertDateToString(DateTime.Now) + "', '" + gjob_id.ToString() + "', '', '', '" + array_extracted[11] + "') ";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + insertBcasExtracted);
                                    entities.Database.ExecuteSqlCommand(insertBcasExtracted);
                                }
                                Int32 soCount = 0;
                                rowCount = soCount + 1;
                                status = true;
                            }

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    String jobID = gjob_id.ToString();

                                    String refNo = "JOBORDSCAN.JOS_JOB_ID.ALL" + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + ".ALL";
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'BCAS-DEDUCT ID DUMMY SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                        "'TRUE', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + @Resource.BCAS_DUMMYSALES_INTERNALID_ID + "', '')";
                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);
                                    
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_status = 'UPLOADED', rn_completedAt = '" + convertDateToString(DateTime.Now) + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT ID DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    var updateTask2 = "update wms_jobordscan set jos_netsuiteProgressDummy = '" + jobID + "' where jos_netsuiteProgressDummy is null " +
                                                        "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                        "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                        "and jos_country_tag = 'ID' " +
                                                        "and jos_businessChannel_code = 'BC'";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + updateTask2);
                                    entities.Database.ExecuteSqlCommand(updateTask2);

                                    var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteDummySo = '" + jobID + "' where of_netsuiteDummySo = '" + gjob_id.ToString() + "' " +
                                                        "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                        "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                        "and of_country_tag = 'ID' ";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + updateBcasFulfill);
                                    entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT ID DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate ID: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                            }
                        }
                        scope1.Complete();
                        //logout();
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate ID: Login Netsuite failed.");
                    }
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("BCASDeductDummyUpdate ID Exception: " + ex.ToString());
                    status = false;
                }
            }//end of scope1
            return status;
        }
        //#1075 -begin
        public Boolean BCASDeductDummyUpdate_TH(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            this.DataFromNetsuiteLog.Info("BCASDeCommitUpdate TH ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
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
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH: Login Netsuite success.");
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
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate TH: Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    //
                    //Boolean loginStatus = login();
                    if (loginStatus == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : Login Netsuite success.");
                        using (sdeEntities entities = new sdeEntities())
                        {
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();
                            List<string> _DataExtracted = new List<string>();

                            #region Get ordered qty
                            var query2 = (from josp in entities.wms_jobordscan_pack
                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                          join jos in entities.wms_jobordscan on new { jobID = josp.josp_jobID, moNo = josp.josp_moNo, ordRecNo = josp.josp_ordRecNo } equals new { jobID = jos.jos_job_ID, moNo = jos.jos_moNo, ordRecNo = jos.jos_ordRecNo }
                                          where jos.jos_businessChannel_code == "BC"
                                          && josp.josp_rangeTo > rangeFrom
                                          && josp.josp_rangeTo <= rangeTo
                                          && jos.jos_country_tag == "TH"
                                          && jos.jos_netsuiteProgressDummy == null
                                          select new
                                          {
                                              jos.jos_jobordscan_ID,
                                              jos.jos_job_ID,
                                              jos.jos_moNo,
                                              jos.jos_rangeTo,
                                              jos.jos_country_tag,
                                              josp.josp_pack_ID,
                                              jomp.nsjomp_jobOrdMaster_pack_ID,
                                              jompd.nsjompd_item_ID,
                                              jompd.nsjompd_isbn,
                                              item_internalID = (jompd.nsjompd_item_internalID),
                                              jomp.nsjomp_ordQty,
                                              josp.josp_ordFulFill,
                                              jompd.nsjompd_sku_qty,
                                              calcqty = (jomp.nsjomp_ordFulfill * jompd.nsjompd_sku_qty),//cpng
                                              calcfulfillQty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty),
                                              qtyUnFulFill = (josp.josp_ordUnFulFill * jompd.nsjompd_sku_qty)// cpng
                                          }).ToList();


                            #region data4bcasFulfill
                            var groupQ3 = from p in query2
                                          let k = new
                                          {
                                              p.jos_job_ID,
                                              p.jos_moNo,
                                              p.jos_rangeTo,
                                              p.jos_country_tag,
                                              p.josp_pack_ID,
                                              p.nsjomp_jobOrdMaster_pack_ID,
                                              p.nsjompd_item_ID,
                                              p.nsjompd_isbn,
                                              p.item_internalID,
                                              p.nsjompd_sku_qty,
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              g.Key.jos_job_ID,
                                              g.Key.jos_moNo,
                                              g.Key.jos_rangeTo,
                                              g.Key.jos_country_tag,
                                              g.Key.josp_pack_ID,
                                              g.Key.nsjomp_jobOrdMaster_pack_ID,
                                              g.Key.nsjompd_item_ID,
                                              g.Key.nsjompd_isbn,
                                              g.Key.item_internalID,
                                              ordQty = g.Sum(p => p.nsjomp_ordQty),
                                              ordFulFill = g.Sum(p => p.josp_ordFulFill),
                                              g.Key.nsjompd_sku_qty,
                                              qty = g.Sum(p => p.calcqty),
                                              fulfillQty = g.Sum(p => p.calcfulfillQty)
                                          };

                            foreach (var data in groupQ3)
                            {
                                _DataExtracted.Add(data.jos_job_ID + "," + data.jos_moNo + "," + convertDateToString(Convert.ToDateTime(data.jos_rangeTo)) + "," + data.jos_country_tag + "," + data.josp_pack_ID + "," + data.nsjomp_jobOrdMaster_pack_ID + "," + data.nsjompd_item_ID + "," + data.item_internalID + "," + data.ordQty.ToString() + "," + data.ordFulFill.ToString() + "," + data.nsjompd_sku_qty.ToString() + "," + data.nsjompd_isbn);
                            }
                            #endregion

                            //cpng start
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH: groupNotFulfill start");
                            Hashtable htWMSItemsQTY = new Hashtable(); // cpng added: item qty sync down to wms
                            var groupNotFulfill = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      itemInternalID = g.Key.itemInternalID,
                                                      SyncQty = g.Sum(p => p.calcqty),//cpng
                                                      fulFillQty = g.Sum(p => p.calcfulfillQty),
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
                            // join jos in entities.wms_jobordscan on new { jobID = josp.josp_jobID, moNo = josp.josp_moNo, ordRecNo = josp.josp_ordRecNo } equals new { jobID = jos.jos_job_ID, moNo = jos.jos_moNo, ordRecNo = jos.jos_ordRecNo }
                            // where jos.jos_businessChannel_code == "BC"
                            // && josp.josp_rangeTo > rangeFrom
                            // && josp.josp_rangeTo <= rangeTo
                            // && jos.jos_country_tag == "TH"
                            // && jos.jos_netsuiteProgressDummy == null
                            // select new { josp, jomp }).ToList().ForEach(x => x.josp.josp_ordUnFulFill = x.jomp.nsjomp_ordQty - x.josp.josp_ordFulFill);
                            //entities.SaveChanges();

                            var updateUnfulfill2 = "update wms_jobordscan_pack josp " +
                            " join netsuite_jobordmaster_pack jomp on josp.josp_pack_ID = jomp.nsjomp_jobOrdMaster_pack_ID " +
                            " join netsuite_jobordmaster_packdetail jompd on jomp.nsjomp_jobOrdMaster_pack_ID = jompd.nsjompd_jobOrdMaster_pack_ID " +
                            " join wms_jobordscan jos on  jos.jos_job_ID = josp.josp_jobID and  jos.jos_moNo = josp.josp_moNo and  jos.jos_ordRecNo = josp.josp_ordRecNo " +
                            " set josp.josp_ordUnFulFill = jomp.nsjomp_ordFulfill - josp.josp_ordFulFill " +
                            " where jos.jos_businessChannel_code = 'BC' " +
                            " and josp.josp_rangeTo > '" + convertDateToString(rangeFrom) + "' and josp.josp_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                            " and jos.jos_country_tag = 'TH' " +
                            " and jos.jos_netsuiteProgressDummy is null ";

                            this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateUnfulfill2);
                            entities.Database.ExecuteSqlCommand(updateUnfulfill2);

                            //cpng end

                            #endregion

                            if (_DataExtracted.Count() > 0)
                            {
                                for (int _i = 0; _i < _DataExtracted.Count(); _i++)
                                {
                                    string[] array_extracted = Convert.ToString(_DataExtracted[_i]).Split(',');
                                    Int32 _ordQty = 0;
                                    Int32 _fulfillQty = 0;
                                    Int32 _skuQty = 0;

                                    if (!String.IsNullOrEmpty(array_extracted[8]) && (!String.IsNullOrWhiteSpace(array_extracted[8])))
                                    {
                                        _ordQty = Convert.ToInt32(array_extracted[8]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[9]) && (!String.IsNullOrWhiteSpace(array_extracted[9])))
                                    {
                                        _fulfillQty = Convert.ToInt32(array_extracted[9]);
                                    }
                                    if (!String.IsNullOrEmpty(array_extracted[10]) && (!String.IsNullOrWhiteSpace(array_extracted[10])))
                                    {
                                        _skuQty = Convert.ToInt32(array_extracted[10]);
                                    }

                                    var insertBcasExtracted = "insert into bcas_ordersfulfill (of_jobID, of_moNo, of_rangeTo, of_country_tag, of_pack_ID, of_jobOrdMaster_ID, of_item_ID" +
                                        ", of_item_internalID, of_ordQty, of_ordFulfillQty, of_ordSkuQty, of_createdDate, of_netsuiteDummySo, of_netsuiteSalesorder, of_netsuiteAdjustment, of_item_ISBN) values ('" + array_extracted[0] + "', '" + array_extracted[1] + "','" + array_extracted[2] + "','" + array_extracted[3] + "'," +
                                        "'" + array_extracted[4] + "','" + array_extracted[5] + "', '" + array_extracted[6] + "', '" + array_extracted[7] + "', " + _ordQty + "," +
                                        _fulfillQty + ", " + _skuQty + ", '" + convertDateToString(DateTime.Now) + "', '" + gjob_id.ToString() + "', '', '', '" + array_extracted[11] + "') ";

                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + insertBcasExtracted);
                                    entities.Database.ExecuteSqlCommand(insertBcasExtracted);
                                }
                                Int32 soCount = 0;
                                rowCount = soCount + 1;
                                status = true;
                            }

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    String jobID = gjob_id.ToString();

                                    if ((jobID != null))
                                    {
                                        String refNo = "JOBORDSCAN.JOS_JOB_ID.ALL" + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + ".ALL";
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('UPDATE', 'BCAS-DEDUCT TH DUMMY SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'TRUE', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + @Resource.BCAS_DUMMYSALES_INTERNALID_TH + "', '')";

                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);
                                        
                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_status = 'UPLOADED', rn_completedAt = '" + convertDateToString(DateTime.Now) + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT TH DUMMY SALES ORDER' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updateTask2 = "update wms_jobordscan set jos_netsuiteProgressDummy = '" + jobID + "' where jos_netsuiteProgressDummy is null " +
                                                            "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and jos_country_tag = 'TH' " +
                                                            "and jos_businessChannel_code = 'BC'";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + updateTask2);
                                        entities.Database.ExecuteSqlCommand(updateTask2);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteDummySo = '" + jobID + "' where of_netsuiteDummySo = '" + gjob_id.ToString() + "' " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_country_tag = 'TH' ";
                                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        UnfulfillBinTransfer(htWMSItemsQTY, jobID, convertDateToString(rangeFrom), convertDateToString(rangeTo));

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT TH DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate TH : " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        //logout();
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate TH : Login Netsuite failed.");
                    }
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("BCASDeductDummyUpdate TH  Exception: " + ex.ToString());
                    status = false;
                }
            }
            return status;
        }
        //#1075 -end

        public Boolean BCASDeductDummyUpdate_SG_Reverse(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);


            // schedule an hour earlier than BCASSalesOrder
            this.DataFromNetsuiteLog.Info("BCASDeCommitUpdate SG ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
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
                            this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : Login Netsuite success.");
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
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate SG : Login Netsuite failed. Exception : " + ex.ToString());

                    }
                    //
                    //Boolean loginStatus = login();
                    if (loginStatus == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : Login Netsuite success.");
                        using (sdeEntities entities = new sdeEntities())
                        {
                            AsyncStatusResult job = new AsyncStatusResult();
                            Int32 rowCount = 0;
                            Guid gjob_id = Guid.NewGuid();
                            List<string> _DataExtracted = new List<string>();

                            RecordRef refDummy = new RecordRef();
                            refDummy.internalId = @Resource.BCAS_DUMMYSALES_INTERNALID_SG;

                            SearchPreferences sp = new SearchPreferences();
                            sp.bodyFieldsOnly = false;
                            //TBA
                            netsuiteService.searchPreferences = sp;

                            TransactionSearchAdvanced sotsa = new TransactionSearchAdvanced();
                            TransactionSearch sots = new TransactionSearch();
                            TransactionSearchBasic sotsb = new TransactionSearchBasic();

                            SearchMultiSelectField bcasDummySO = new SearchMultiSelectField();
                            bcasDummySO.@operator = SearchMultiSelectFieldOperator.anyOf;
                            bcasDummySO.operatorSpecified = true;
                            bcasDummySO.searchValue = new RecordRef[] { refDummy };
                            sotsb.internalId = bcasDummySO;

                            sots.basic = sotsb;
                            sotsa.criteria = sots;
                            //TBA
                            netsuiteService.tokenPassport = createTokenPassport();
                            SearchResult sr = netsuiteService.search(sotsa);
                            Record[] srRecord = sr.recordList;

                            SalesOrder decommitSO = new SalesOrder();

                            List<String> deCommitItem = new List<String>();
                            List<Int32> deCommitQty = new List<Int32>();
                            List<String> deCommitItemAdd = new List<String>();
                            List<Int32> deCommitQtyAdd = new List<Int32>();

                            for (int i = 0; i < srRecord.Count(); i++)
                            {
                                SalesOrder so = (SalesOrder)srRecord[i];
                                decommitSO.itemList = so.itemList;

                                var groupQ2 = (from jos in entities.dummysoes
                                              where jos.dso_soInternalId == "146446"
                                              && jos.dso_rangeTo > rangeFrom
                                              && jos.dso_rangeTo <= rangeTo
                                              select new
                                              {
                                                  jos.dso_itemInternalId,
                                                  jos.dso_oriQuantity,
                                                  jos.dso_newQuantity
                                              }).ToList();

                                foreach (var q2 in groupQ2)
                                {
                                    if (Convert.ToInt32(q2.dso_newQuantity) == 0)
                                    {
                                        deCommitItemAdd.Add(q2.dso_itemInternalId);
                                        deCommitQtyAdd.Add(Convert.ToInt32(q2.dso_oriQuantity));
                                    }
                                    else
                                    {
                                        deCommitItem.Add(q2.dso_itemInternalId);
                                        deCommitQty.Add(Convert.ToInt32(q2.dso_oriQuantity));
                                    }
                                }
                            }

                            #region Decommit item
                            SalesOrder[] soList = new SalesOrder[1];
                            if ((deCommitItem.Count() + deCommitItemAdd.Count()) > 0)
                            {
                                decommitSO.internalId = @Resource.BCAS_DUMMYSALES_INTERNALID_SG;

                                RecordRef refSub = new RecordRef();
                                refSub.internalId = @Resource.BCAS_DUMMYSALES_MY;
                                decommitSO.subsidiary = refSub;

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                decommitSO.@class = refBusinessChannel;

                                decommitSO.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                decommitSO.orderStatusSpecified = true;

                                SalesOrderItem[] soii = new SalesOrderItem[decommitSO.itemList.item.Count() + deCommitItemAdd.Count()];
                                SalesOrderItemList soil = new SalesOrderItemList();

                                int recCount = 0;
                                for (int j = 0; j < decommitSO.itemList.item.Count(); j++)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();
                                    RecordRef refItem = new RecordRef();
                                    refItem.internalId = decommitSO.itemList.item[j].item.internalId;
                                    soi.item = refItem;

                                    soi.quantity = decommitSO.itemList.item[j].quantity;
                                    soi.quantitySpecified = true;

                                    soi.amount = 0;
                                    soi.amountSpecified = true;

                                    soi.createPoSpecified = false;

                                    for (int i = 0; i < deCommitItem.Count(); i++)
                                    {
                                        if (deCommitItem[i].Equals(decommitSO.itemList.item[j].item.internalId))
                                        {
                                            soi.quantity = deCommitQty[i];
                                            soi.quantitySpecified = true;
                                            break;
                                        }
                                    }

                                    soii[recCount] = soi;
                                    recCount = recCount + 1;
                                }

                                for (int j = 0; j < deCommitItemAdd.Count(); j++)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();
                                    RecordRef refItem = new RecordRef();
                                    refItem.internalId = deCommitItemAdd[j];
                                    soi.item = refItem;

                                    soi.quantity = deCommitQtyAdd[j];
                                    soi.quantitySpecified = true;

                                    soi.amount = 0;
                                    soi.amountSpecified = true;

                                    soi.createPoSpecified = false;

                                    soii[j] = soi;
                                    recCount = recCount + 1;
                                }

                                soil.replaceAll = true;    //for remove all items
                                soil.item = soii;
                                decommitSO.itemList = soil;

                                Int32 soCount = 0;
                                soList[soCount] = decommitSO;
                                rowCount = soCount + 1;

                                soCount++;
                                status = true;
                            }
                            #endregion

                            if (status == true)
                            {
                                if (rowCount > 0)
                                {
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncUpdateList(soList);
                                    String jobID = job.jobId;

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-DEDUCT SG DUMMY SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASDeductDummyUpdate SG : " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                                }
                            }
                            else if (rowCount == 0)
                            {
                            }
                        }
                        scope1.Complete();
                        //logout();
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Fatal("BCASDeductDummyUpdate SG : Login Netsuite failed.");
                    }
                }
                catch (Exception ex)
                {
                    this.DataFromNetsuiteLog.Error("BCASDeductDummyUpdate SG  Exception: " + ex.ToString());
                    status = false;
                }
            }//end of scope1
            return status;
        }        

        //NETSUITE PHASE II (TRX WITH AMOUNT) -BEGIN
        public Boolean BCASSalesOrder(DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            Boolean status = false;
            this.DataFromNetsuiteLog.Info("BCASSalesOrder ***************");
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
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status1 = netsuiteService.search(basic);
                    if (status1.status.isSuccess == true)
                    {
                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrder: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //
                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASSalesOrder: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();
                        List<String> fulfillItem = new List<String>();
                        List<Int32> fulfillQty = new List<Int32>();

                        var IDquery = (from q1 in entities.wms_jobordscan
                                       where q1.jos_businessChannel_code == "BC"
                                       && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                       && q1.jos_country_tag == "ID"   
                                       select new { q1.jos_job_ID, q1.jos_moNo })
                                      .Distinct()
                                      .ToList();

                        List<string> _IDjob = new List<string>();
                        foreach (var q1 in IDquery)
                        {
                            _IDjob.Add(q1.jos_job_ID + "-" + q1.jos_moNo);
                        }

                        var query1 = (from q1 in entities.wms_jobordscan
                                      where q1.jos_businessChannel_code == "BC"
                                      && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                      select new
                                      {
                                          q1.jos_job_ID,
                                          q1.jos_businessChannel_code,
                                          q1.jos_country_tag,
                                          q1.jos_rangeTo,
                                          mono = q1.jos_moNo.Substring(0, 1)
                                      })
                                        .Distinct()
                                        .OrderBy(x => x.jos_businessChannel_code)
                                        .ThenBy(y => y.jos_country_tag)
                                        .ToList();

                        //status = true;
                        this.DataFromNetsuiteLog.Info("BCASSalesOrder: " + query1.Count() + " records to update.");
                        SalesOrder[] soList = new SalesOrder[query1.Count()];

                        foreach (var q1 in query1)
                        {
                            try
                            {
                                SalesOrder so = new SalesOrder();

                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.BCAS_SALES_CUSTOMFORM_MY;
                                so.customForm = refForm;

                                so.tranDate = DateTime.Now;
                                //so.tranDate = DateTime.Now.AddDays(-2); //for year end closing
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                so.@class = refClass;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                so.location = refLocation;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                if (q1.mono.Equals("E"))
                                {
                                    so.memo = "BCAS REPLACEMENT SALES";
                                }
                                else if (q1.mono.Equals("D"))
                                {
                                    so.memo = "BCAS REDEMPTION SALES";
                                }
                                else
                                {
                                    so.memo = "BCAS SALES";
                                }

                                RecordRef refEntity = new RecordRef();
                                if (q1.jos_country_tag.Equals("ID"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                    so.entity = refEntity;
                                }
                                else if (q1.jos_country_tag.Equals("MY"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                    so.entity = refEntity;
                                }
                                else if (q1.jos_country_tag.Equals("SG"))
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                    so.entity = refEntity;
                                }
                                else if (q1.jos_country_tag.Equals("TH")) //#1076
                                {
                                    refEntity.internalId = @Resource.BCAS_CUSTOMER_TH;
                                    so.entity = refEntity;
                                }

                                //RecordRef refShipAddr = new RecordRef();
                                //refShipAddr.internalId = "1";
                                //so.shipAddressList = refShipAddr;

                                #region ID Sales
                                if (q1.jos_country_tag.Equals("ID")) 
                                     {
                                    //var query3 = (from ji in entities.netsuite_jobitem
                                    //              where ji.nsji_nsj_jobID == q1.jos_job_ID
                                    //              && _IDjob.Contains(ji.nsji_nsj_jobID + "-" + ji.nsji_moNo)
                                    //              select ji).ToList();

                                    var query3 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                  && ji.of_moNo.StartsWith(q1.mono)
                                                  && _IDjob.Contains(ji.of_jobID + "-" + ji.of_moNo)
                                                  && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                  && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                  && nstask.rnt_status == "TRUE"
                                                  select ji).ToList();

                                    var groupQ3 = from p in query3
                                                  let k = new
                                                  {
                                                      //itemInternalID = p.nsji_item_internalID,
                                                      //fulFillQty = p.nsji_item_qty
                                                      itemInternalID = p.of_item_internalID,
                                                      //fulFillQty = p.of_ordQty
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      //fulFillQty = g.Sum(p => p.nsji_item_qty)
                                                      fulFillQty = g.Sum(p => p.of_ordQty)
                                                  };

                                    if (groupQ3.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ3.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ3)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            /*
                                             * form 143 got location in column
                                            */
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            soi.amount = 0;
                                            soi.amountSpecified = true;

                                            //Commit Status
                                            soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                            soi.commitInventorySpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region MY Sales
                                else if (q1.jos_country_tag.Equals("MY"))
                                {
                                    var query2 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  join packdetail in entities.netsuite_jobordmaster_packdetail
                                                  on new { param1 = ji.of_jobID, param2 = ji.of_pack_ID, param3 = ji.of_item_ID } equals
                                                  new { param1 = packdetail.nsjompd_job_ID, param2 = packdetail.nsjompd_jobOrdMaster_pack_ID, param3 = packdetail.nsjompd_item_ID }
                                                  where ji.of_jobID == q1.jos_job_ID
                                                  && ji.of_moNo.StartsWith(q1.mono)
                                                  && ji.of_rangeTo > rangeFrom
                                                  && ji.of_rangeTo <= rangeTo
                                                  && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                  && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                  && nstask.rnt_status == "TRUE"
                                                  select new
                                                  {
                                                      ji.of_jobID,
                                                      ji.of_pack_ID,
                                                      ji.of_ordPack,
                                                      ji.of_ordPackQty,
                                                      ji.of_ordPackPrice,
                                                      ji.of_ordPackGst,
                                                      ji.of_item_internalID,
                                                      ji.of_ordFulfillQty,
                                                      packdetail.nsjompd_tax_code,
                                                      packdetail.nsjompd_item_price,
                                                      qty = (ji.of_ordFulfillQty * ji.of_ordSkuQty),
                                                      grpAmount = ((ji.of_ordFulfillQty * ji.of_ordSkuQty) * packdetail.nsjompd_item_price),
                                                      grpGst = ((ji.of_ordFulfillQty * ji.of_ordSkuQty) * packdetail.nsjompd_gstamount),
                                                      grpDeliveryCharge = ((ji.of_ordFulfillQty * ji.of_ordSkuQty) * packdetail.nsjompd_deliveryCharge),
                                                      grpDeliveryChargeGst = ((ji.of_ordFulfillQty * ji.of_ordSkuQty) * packdetail.nsjompd_deliveryChargeGst)
                                                  }).ToList();
                                   
                                    var findRouding = (from p in query2
                                                       let k = new
                                            {
                                                packIDGrp = p.of_pack_ID,
                                                ordPackGrp = p.of_ordPack,
                                                ordPackQtyGrp = p.of_ordPackQty,
                                                ordPackPriceGrp = p.of_ordPackPrice,
                                                ordPackGstGrp = p.of_ordPackGst,
                                            }
                                            group p by k into g
                                            select new
                                            {
                                                ordpackID = g.Key.packIDGrp,
                                                ordPack = g.Key.ordPackGrp,
                                                ordPackQty = g.Key.ordPackQtyGrp,
                                                ordPackPrice = g.Key.ordPackPriceGrp,
                                                ordPackGst = g.Key.ordPackGstGrp,
                                                sumISBN = g.Count(),
                                                sumISBNfulfilled = g.Sum(p => p.of_ordFulfillQty),
                                                sumOrdPackAmount = g.Sum(p => p.grpAmount),
                                                sumOrdPackGst = g.Sum(p => p.grpGst),
                                                sumOrdPackDeliveryCharge = g.Sum(p => p.grpDeliveryCharge),
                                                sumOrdPackDeliveryChargeGst = g.Sum(p => p.grpDeliveryChargeGst)
                                            }).ToList();

                                    double deliveryCharge = 0;
                                    double deliveryChargeGst = 0;
                                    double roudingAmount = 0;
                                    double roudingGst = 0;
                                    foreach (var grpItem in findRouding)
                                    {
                                        int packFulfilled = 0;
                                        double packAmount = 0;
                                        double packGst = 0;
                                        double totItemsAmount = 0;
                                        double totItemsGst = 0;
                                        packFulfilled = (Convert.ToInt32(grpItem.sumISBNfulfilled) / Convert.ToInt32(grpItem.sumISBN));
                                        packAmount = (packFulfilled * Convert.ToDouble(grpItem.ordPackPrice));
                                        packGst = (packFulfilled * Convert.ToDouble(grpItem.ordPackGst));

                                        totItemsAmount = (Convert.ToDouble(grpItem.sumOrdPackAmount) + Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge));
                                        totItemsGst = (Convert.ToDouble(grpItem.sumOrdPackGst) + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));

                                        if (!q1.mono.Equals("E") && !q1.mono.Equals("D"))
                                        {
                                            deliveryCharge = (deliveryCharge + (Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge) - Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst)));
                                            deliveryChargeGst = (deliveryChargeGst + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));
                                            roudingAmount = roudingAmount + ((packAmount-packGst) - (totItemsAmount-totItemsGst));
                                            roudingGst = roudingGst + (packGst - totItemsGst);
                                        }
                                    }

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.of_item_internalID,
                                                      taxCodeGrp=p.nsjompd_tax_code,
                                                      itemPriceGrp=p.nsjompd_item_price,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      taxCode = g.Key.taxCodeGrp,
                                                      itemPrice = g.Key.itemPriceGrp,
                                                      fulFillQty = g.Sum(p => p.qty),
                                                      totAmount = g.Sum(p => p.grpAmount),
                                                      totGst = g.Sum(p => p.grpGst),
                                                      //totDeliveryCharge = g.Sum(p => p.grpDeliveryCharge),
                                                      //totDeliveryChargeGst = g.Sum(p => p.grpDeliveryChargeGst)
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()+2];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ2)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();
                                            RecordRef refPriceLevel = new RecordRef();

                                            /*
                                             * form 143 got location in column
                                            */
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            if (q1.mono.Equals("E"))
                                            {
                                                soi.quantity = Convert.ToDouble(item.fulFillQty);
                                                soi.quantitySpecified = true;
                                                soi.amount = 0;
                                                soi.amountSpecified = true;
                                            }
                                            else if (q1.mono.Equals("D"))
                                            {
                                                soi.quantity = Convert.ToDouble(item.fulFillQty);
                                                soi.quantitySpecified = true;
                                                soi.amount = 0;
                                                soi.amountSpecified = true;
                                            }
                                            else
                                            {
                                                //Items
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.item;
                                                soi.item = refItem;

                                                //Price Level
                                                refPriceLevel.internalId = "-1";//Custom
                                                soi.price = refPriceLevel;

                                                soi.quantity = Convert.ToDouble(item.fulFillQty);
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate
                                                double calc_rate1=Math.Round((Convert.ToDouble(item.totAmount) - Convert.ToDouble(item.totGst)), 2);
                                                double calc_rate2=Math.Round(calc_rate1/Convert.ToDouble(item.fulFillQty), 2);

                                                soi.rate = Convert.ToString(Math.Round(calc_rate2, 2));
                                                soi.amount = Math.Round(calc_rate1, 2);
                                                soi.amountSpecified = true;

                                                //Gst Amount
                                                soi.tax1Amt = Math.Round(Convert.ToDouble(item.totGst), 2);
                                                soi.tax1AmtSpecified = true;

                                                //Tax Code
                                                if (Convert.ToDouble(item.totGst) != 0)
                                                {
                                                    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                                }
                                                else
                                                {
                                                    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                                }

                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                                //}else if (item.taxCode == "SR-0%")
                                                //{
                                                //    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                                //}

                                                soi.taxCode = refTaxCode;
                                            }

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[4];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; 
                                            //scfr2.internalId = "2759"; 
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; 
                                            //scfr3.internalId = "2760"; 
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; 
                                            //scfr4.internalId = "2804"; 
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //StringCustomFieldRef scfr5 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_line_number";
                                            //scfr4.internalId = "2886";
                                            //scfr4.value = "1";
                                            //cfrList2[3] = scfr5;

                                            //soi.customFieldList = cfrList2;
                                            // ----------------

                                            //Commit Status
                                            soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                            soi.commitInventorySpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }

                                        #region Delivery Charge
                                        if ((deliveryCharge != 0) || (deliveryChargeGst != 0))
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_DC_INTERNALID;
                                            soi.item = refItem;

                                            soi.quantity = 1;
                                            soi.quantitySpecified = true;

                                            if (deliveryCharge != 0)
                                            {
                                                soi.rate = Convert.ToString(Math.Round(deliveryCharge, 2));
                                            }
                                            else
                                            {
                                                soi.rate = Convert.ToString(Math.Round(deliveryChargeGst, 2));
                                            }

                                            soi.amount = Math.Round(deliveryCharge,2);
                                            soi.amountSpecified = true;

                                            soi.tax1Amt = Math.Round(deliveryChargeGst,2);
                                            soi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(deliveryChargeGst, 2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            soi.taxCode = refTaxCode;

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[4];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date";
                                            //scfr2.internalId = "2759";
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date";
                                            //scfr3.internalId = "2760";
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type";
                                            //scfr4.internalId = "2804";
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //StringCustomFieldRef scfr5 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_line_number";
                                            //scfr4.internalId = "2886";
                                            //scfr4.value = "1";
                                            //cfrList2[3] = scfr5;

                                            //soi.customFieldList = cfrList2;
                                            // ----------------

                                            //To set Non-inventory location - WY-08.JAN.2016
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        #endregion
                                        #region Rounding
                                        if ((roudingAmount != 0) || (roudingGst!=0))
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_ROUDING_INTERNALID;
                                            soi.item = refItem;

                                            soi.quantity = 1;
                                            soi.quantitySpecified = true;

                                            if (roudingAmount != 0)
                                            {
                                                soi.rate = Convert.ToString(roudingAmount);
                                            } 
                                            else
                                            {
                                                soi.rate = Convert.ToString(roudingGst);
                                            }

                                            soi.amount = Math.Round(roudingAmount,2);
                                            soi.amountSpecified = true;

                                            soi.tax1Amt = Math.Round(roudingGst,2);
                                            soi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(roudingGst,2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            soi.taxCode = refTaxCode;

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[4];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date";
                                            //scfr2.internalId = "2759";
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date";
                                            //scfr3.internalId = "2760";
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type";
                                            //scfr4.internalId = "2804";
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //StringCustomFieldRef scfr5 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_line_number";
                                            //scfr4.internalId = "2886";
                                            //scfr4.value = "1";
                                            //cfrList2[3] = scfr5;

                                            //soi.customFieldList = cfrList2;
                                            // ----------------

                                            //To set Non-inventory location - WY-08.JAN.2016
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        #endregion

                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region SG Sales
                                else if (q1.jos_country_tag.Equals("SG")) 
                                {
                                    var query2 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                    && ji.of_moNo.StartsWith(q1.mono)
                                                    && ji.of_rangeTo > rangeFrom
                                                    && ji.of_rangeTo <= rangeTo
                                                    && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                    && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                    && nstask.rnt_status == "TRUE"
                                                  select new { ji.of_item_internalID, qty = (ji.of_ordFulfillQty * ji.of_ordSkuQty) }).ToList();

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.of_item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      fulFillQty = g.Sum(p => p.qty)
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ2)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            /*
                                             * form 143 got location in column
                                            */
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            //soi.amount = 0;
                                            //soi.amountSpecified = true;

                                            //Commit Status
                                            soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                            soi.commitInventorySpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region TH Sales
                                else if (q1.jos_country_tag.Equals("TH")) 
                                {
                                    //#1076 
                                    var query2 = (from ji in entities.bcas_ordersfulfill
                                                  join nstask in entities.requestnetsuite_task on ji.of_netsuiteDummySo equals nstask.rnt_jobID
                                                  where ji.of_jobID == q1.jos_job_ID
                                                    && ji.of_moNo.StartsWith(q1.mono)
                                                    && ji.of_rangeTo > rangeFrom
                                                    && ji.of_rangeTo <= rangeTo
                                                    && (ji.of_netsuiteSalesorder == null || ji.of_netsuiteSalesorder == "")
                                                    && nstask.rnt_description.Contains("BCAS-DEDUCT")
                                                    && nstask.rnt_status == "TRUE"
                                                  select new { ji.of_item_internalID, qty = (ji.of_ordFulfillQty * ji.of_ordSkuQty) }).ToList();
                                    

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      itemInternalID = p.of_item_internalID,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      item = g.Key.itemInternalID,
                                                      fulFillQty = g.Sum(p => p.qty)
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                        SalesOrderItemList soil = new SalesOrderItemList();
                                        Int32 itemCount = 0;

                                        foreach (var item in groupQ2)
                                        {
                                            SalesOrderItem soi = new SalesOrderItem();

                                            /*
                                             * form 143 got location in column
                                            */
                                            if (refForm.internalId == "143")
                                            {
                                                RecordRef refLocationCol = new RecordRef();
                                                refLocationCol.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                                soi.location = refLocationCol;
                                            }

                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            soi.item = refItem;

                                            soi.quantity = Convert.ToDouble(item.fulFillQty);
                                            soi.quantitySpecified = true;

                                            //soi.amount = 0;
                                            //soi.amountSpecified = true;

                                            //Commit Status
                                            soi.commitInventory = SalesOrderItemCommitInventory._doNotCommit;
                                            soi.commitInventorySpecified = true;

                                            soii[itemCount] = soi;
                                            itemCount++;
                                        }
                                        soil.item = soii;
                                        so.itemList = soil;
                                        soList[soCount] = so;

                                        rowCount = soCount + 1;

                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + q1.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + q1.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER','" + refNo + "','" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateBcasFulfill = "update bcas_ordersfulfill set of_netsuiteSalesorder = '" + gjob_id.ToString() + "' where (of_netsuiteSalesorder is null or of_netsuiteSalesorder = '') " +
                                                            "and of_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                                            "and of_rangeTo <= '" + convertDateToString(rangeTo) + "' " +
                                                            "and of_jobID = '" + q1.jos_job_ID + "' " +
                                                            "and of_moNo like '" + q1.mono + "%' ";

                                        this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateBcasFulfill);
                                        entities.Database.ExecuteSqlCommand(updateBcasFulfill);

                                        soCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASSalesOrder Exception: (" + q1.jos_job_ID + "," + q1.jos_rangeTo + ")" + ex.ToString());
                                status = false;
                                if (rowCount == 0)
                                {
                                    rowCount++;
                                }
                                break;
                            }
                        }//end of bcas SO

                        if (status == true)
                        {
                            if (rowCount > 0)
                            {
                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                job = netsuiteService.asyncAddList(soList);
                                String jobID = job.jobId;

                                if ((jobID!=null) && (jobID.StartsWith("ASYNC")==true)) {
                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-SALES ORDER' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'BCAS-SALES ORDER' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("BCASSalesOrder: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
                        
                            scope1.Complete();
                        }
                    }//end of sdeEntities
                    //logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSalesOrder: Login Netsuite failed.");
                }
            }//end of scope1
            //}
            return status;
        }
        public Boolean BCASInvoiceCreation(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            this.DataFromNetsuiteLog.Info("BCASInvoiceCreation *****************");
            Boolean status = false;

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
                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASInvoiceCreation: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //
                //Boolean loginStatus = login();

                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("BCASInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.bcas_salestransaction
                                         where (q1.bst_ifUpdatedDate > rangeFrom && q1.bst_ifUpdatedDate <= rangeTo)
                                         && q1.bst_ifInternalID != null
                                         select new
                                         {
                                             q1.bst_soInternalID,
                                             q1.bst_postingDate,
                                             isFirstRun = q1.bst_invProgressStatus == null ? "Y" : "N"
                                         }).Distinct().ToList();

                        var qFilterMono = (from d in qListMono
                                           where d.isFirstRun == "Y"
                                           select new
                                           {
                                               d.bst_soInternalID,
                                               d.bst_postingDate,
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("BCASInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.salesOrder;
                                refSO.internalId = i.bst_soInternalID;
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
                                    #region Main Information
                                    //createdfrom 
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                    inv.createdFrom = refCreatedFrom;

                                    //Form 
                                    RecordRef refForm = new RecordRef();
                                    refForm.internalId = @Resource.BCAS_INVOICE_CUSTOMFORM_GMY;
                                    inv.customForm = refForm;

                                    inv.tranDate = Convert.ToDateTime(i.bst_postingDate);
                                    inv.tranDateSpecified = true;

                                    inv.memo = inv2.memo;
                                    #endregion

                                    if (inv2.itemList != null)
                                    {
                                        //inv.itemList = inv2.itemList;
                                        //for (int j = 0; j < inv.itemList.item.Count(); j++)
                                        //{
                                        //    if (inv.itemList.item[j].item.internalId == "34838")
                                        //    {
                                                //inv.itemList.item[j].rate = "0";
                                                //inv.itemList.item[j].tax1Amt = 0;
                                                //inv.itemList.item[j].grossAmt = 0;
                                        //    }
                                        //    if (inv.itemList.item[j].item.internalId == "35248")
                                        //    {
                                                //inv.itemList.item[j].rate = "0";
                                                //inv.itemList.item[j].tax1Amt = 0;
                                                //inv.itemList.item[j].grossAmt = 0;
                                        //    }
                                        //}                                        

                                        invList[invCount] = inv;
                                        rowCount = invCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-INVOICE', 'BCASINVOICECREATION.SOINTERNALID." + i.bst_soInternalID + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + i.bst_soInternalID + "')";
                                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updSalesTrx = "UPDATE bcas_salestransaction SET bst_invProgressStatus = '" + gjob_id.ToString() + "', bst_invSeqNo = '" + rowCount + "', " +
                                                          "bst_invJobID = '" + gjob_id.ToString() + "' " +
                                                          "WHERE bst_invProgressStatus IS NULL AND bst_soInternalID = '" + i.bst_soInternalID + "' " +
                                                          "AND bst_ifUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                          "AND bst_ifUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        invCount++;
                                        status = true;
                                    }
                                }
                                else
                                {
                                    var updSalesTrx = "UPDATE bcas_salestransaction SET bst_invProgressStatus = 'NO RECORD FOUND' " +
                                                      "WHERE bst_invProgressStatus IS NULL AND bst_soInternalID = '" + i.bst_soInternalID + "' " +
                                                      "AND bst_ifUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                      "AND bst_ifUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);
                                }

                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASInvoiceCreation Exception: " + ex.ToString());
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
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updSalesTrx = "UPDATE bcas_salestransaction SET bst_invJobID = '" + jobID + "' WHERE bst_invJobID = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("BCASInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("BCASInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASInvoiceCreation: Login Netsuite failed.");
                }
            }//end of scopeOuter
            //logout();
            return status;
        }
        public Boolean BCASCreditMemo(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            this.DataFromNetsuiteLog.Info("BCASCreditMemo *****************");
            Boolean status = false;

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
                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASCreditMemo: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //
                //Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("BCASCreditMemo: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Int32 raCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var grpReturn = (from q1 in entities.netsuite_jobordmaster
                                         where q1.nsjom_createdDate > rangeFrom
                                         && q1.nsjom_createdDate <= rangeTo
                                         && q1.nsjom_moNo.StartsWith("R")
                                         select new
                                         {
                                             q1.nsjom_jobOrdMaster_ID,
                                             q1.nsjom_nsj_job_ID,
                                             q1.nsjom_jobmo_id,
                                             q1.nsjom_ordRecNo,
                                             q1.nsjom_ordStudent,
                                             q1.nsjom_moNo,
                                             q1.nsjom_country
                                         }).Distinct().ToList();

                        CreditMemo[] raList = new CreditMemo[grpReturn.Count];
                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                #region CREDIT MEMO
                                CreditMemo raInv2 = new CreditMemo();

                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.BCAS_CREDITMEMO_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                RecordRef refEntity = new RecordRef();
                                switch (q1.nsjom_country)
                                {
                                    case "MY"://hard code
                                        refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                        break;
                                    case "SG"://hard code
                                        refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                        break;
                                    case "ID"://hard code
                                        refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                        break;
                                }
                                raInv2.entity = refEntity;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                raInv2.@class = refClass;

                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                raInv2.location = refLocationSO;

                                raInv2.memo = "Refund for " + q1.nsjom_ordStudent + ", order number: " + q1.nsjom_moNo;

                                raInv2.tranDate = Convert.ToDateTime(DateTime.Now);
                                raInv2.tranDateSpecified = true;
                                #endregion

                                #region ID Sales
                                if (q1.nsjom_country.Equals("ID"))
                                {
                                }
                                #endregion
                                #region MY Sales
                                else if (q1.nsjom_country.Equals("MY"))
                                {
                                   
                                    var query2 = (from ji in entities.netsuite_jobordmaster
                                                  join jipack in entities.netsuite_jobordmaster_pack on ji.nsjom_jobOrdMaster_ID equals jipack.nsjomp_jobOrdMaster_ID
                                                  join packdetail in entities.netsuite_jobordmaster_packdetail on jipack.nsjomp_jobOrdMaster_pack_ID equals packdetail.nsjompd_jobOrdMaster_pack_ID
                                                  where ji.nsjom_nsj_job_ID == q1.nsjom_nsj_job_ID
                                                  && ji.nsjom_jobOrdMaster_ID == q1.nsjom_jobOrdMaster_ID   //kang Wrong amount total for items. Need to put in filter jobOrdMaster_ID
                                                  && ji.nsjom_moNo == q1.nsjom_moNo
                                                  && ji.nsjom_country == q1.nsjom_country
                                                  && ji.nsjom_createdDate > rangeFrom
                                                  && ji.nsjom_createdDate <= rangeTo
                                                  select new
                                                  {
                                                      ji.nsjom_nsj_job_ID,
                                                      ji.nsjom_moNo,
                                                      jipack.nsjomp_jobOrdMaster_pack_ID,
                                                      jipack.nsjomp_ordPack,
                                                      jipack.nsjomp_ordQty,
                                                      jipack.nsjomp_ordPrice,
                                                      jipack.nsjomp_gstamount,
                                                      packdetail.nsjompd_item_internalID,
                                                      packdetail.nsjompd_tax_code,
                                                      packdetail.nsjompd_item_price,
                                                      qty = (jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty),
                                                      grpAmount = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_item_price),
                                                      grpGst = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_gstamount),
                                                      grpDeliveryCharge = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_deliveryCharge),
                                                      grpDeliveryChargeGst = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_deliveryChargeGst)
                                                  }).ToList();

                                    var findRouding = (from p in query2
                                                       let k = new
                                                       {
                                                           packIDGrp = p.nsjomp_jobOrdMaster_pack_ID,
                                                           ordPackGrp = p.nsjomp_ordPack,
                                                           ordPackQtyGrp = p.nsjomp_ordQty,
                                                           ordPackPriceGrp = p.nsjomp_ordPrice,
                                                           ordPackGstGrp = p.nsjomp_gstamount,
                                                       }
                                                       group p by k into g
                                                       select new
                                                       {
                                                           ordpackID = g.Key.packIDGrp,
                                                           ordPack = g.Key.ordPackGrp,
                                                           ordPackQty = g.Key.ordPackQtyGrp,
                                                           ordPackPrice = g.Key.ordPackPriceGrp,
                                                           ordPackGst = g.Key.ordPackGstGrp,
                                                           sumISBN = g.Count(),
                                                           sumISBNfulfilled = g.Sum(p => p.qty),
                                                           sumOrdPackAmount = g.Sum(p => p.grpAmount),
                                                           sumOrdPackGst = g.Sum(p => p.grpGst),
                                                           sumOrdPackDeliveryCharge = g.Sum(p => p.grpDeliveryCharge),
                                                           sumOrdPackDeliveryChargeGst = g.Sum(p => p.grpDeliveryChargeGst)
                                                       }).ToList();

                                    double deliveryCharge = 0;
                                    double deliveryChargeGst = 0;
                                    double roudingAmount = 0;
                                    double roudingGst = 0;
                                    foreach (var grpItem in findRouding)
                                    {
                                        int packFulfilled = 0;
                                        double packAmount = 0;
                                        double packGst = 0;
                                        double totItemsAmount = 0;
                                        double totItemsGst = 0;
                                        packFulfilled = (Convert.ToInt32(grpItem.sumISBNfulfilled) / Convert.ToInt32(grpItem.sumISBN));
                                        packAmount = (packFulfilled * Convert.ToDouble(grpItem.ordPackPrice));
                                        packGst = (packFulfilled * Convert.ToDouble(grpItem.ordPackGst));
                                        totItemsAmount = (Convert.ToDouble(grpItem.sumOrdPackAmount) + Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge));
                                        totItemsGst = (Convert.ToDouble(grpItem.sumOrdPackGst) + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));
                                        deliveryCharge = (deliveryCharge + (Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge) - Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst)));
                                        deliveryChargeGst = (deliveryChargeGst + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));
                                        roudingAmount = roudingAmount + ((packAmount - packGst) - (totItemsAmount - totItemsGst));
                                        roudingGst = roudingGst + (packGst - totItemsGst);
                                    }

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      moNoGrp = p.nsjom_moNo,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      moNo = g.Key.moNoGrp,
                                                      fulFillQty = g.Sum(p => p.qty),
                                                      totAmount = g.Sum(p => p.grpAmount),
                                                      totGst = g.Sum(p => p.grpGst),
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        CreditMemoItem[] rasoii = new CreditMemoItem[3];
                                        CreditMemoItemList rasoil = new CreditMemoItemList();
                                        Int32 itemCount = 0;

                                        #region Return-Actual
                                        foreach (var item in groupQ2)
                                        {
                                            CreditMemoItem rasoi = new CreditMemoItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();
                                            RecordRef refPriceLevel = new RecordRef();

                                            //Items
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_RETNNONINV_INTERNALID;
                                            rasoi.item = refItem;

                                            //Price Level
                                            refPriceLevel.internalId = "-1";//Custom
                                            rasoi.price = refPriceLevel;

                                            rasoi.quantity = Convert.ToDouble(1);
                                            rasoi.quantitySpecified = true;

                                            //Unit Price/Rate
                                            double calc_rate1 = Math.Round((Convert.ToDouble(item.totAmount) - Convert.ToDouble(item.totGst)), 2);
                                            double calc_rate2 = Math.Round(calc_rate1 / Convert.ToDouble(1), 2);

                                            rasoi.rate = Convert.ToString(Math.Round(calc_rate2, 2));
                                            rasoi.amount = Math.Round(calc_rate1, 2);
                                            rasoi.amountSpecified = true;

                                            //Gst Amount
                                            rasoi.tax1Amt = Math.Round(Convert.ToDouble(item.totGst), 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Convert.ToDouble(item.totGst) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }

                                            //if (item. == "ZRL")
                                            //{
                                            //    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            //}
                                            //else if (item.taxCode == "SR-0%")
                                            //{
                                            //    refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            //}

                                            rasoi.taxCode = refTaxCode;

                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion
                                        #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling
                                        if ((deliveryCharge != 0) || (deliveryChargeGst != 0))
                                        {
                                            CreditMemoItem rasoi = new CreditMemoItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_DC_INTERNALID;
                                            rasoi.item = refItem;

                                            rasoi.quantity = 1;
                                            rasoi.quantitySpecified = true;

                                            rasoi.rate = Convert.ToString(Math.Round(deliveryCharge, 2));

                                            rasoi.amount = Math.Round(deliveryCharge, 2);
                                            rasoi.amountSpecified = true;

                                            rasoi.tax1Amt = Math.Round(deliveryChargeGst, 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(deliveryChargeGst, 2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            rasoi.taxCode = refTaxCode;

                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion
                                        #region Rounding
                                        if ((roudingAmount != 0) || (roudingGst != 0))
                                        {
                                            CreditMemoItem rasoi = new CreditMemoItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_ROUDING_INTERNALID;
                                            rasoi.item = refItem;

                                            rasoi.quantity = 1;
                                            rasoi.quantitySpecified = true;

                                            rasoi.rate = Convert.ToString(roudingAmount);

                                            rasoi.amount = Math.Round(roudingAmount, 2);
                                            rasoi.amountSpecified = true;

                                            rasoi.tax1Amt = Math.Round(roudingGst, 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(roudingGst, 2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            rasoi.taxCode = refTaxCode;
                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion

                                        rasoil.item = rasoii;
                                        raInv2.itemList = rasoil;
                                        raList[raCount] = raInv2;
                                        rowCount = raCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-CREDIT MEMO', '" + q1.nsjom_nsj_job_ID + '.' + q1.nsjom_moNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        DateTime _postDate = Convert.ToDateTime(DateTime.Now);
                                        var insSalesTrx = "insert into bcas_otherstransaction (bot_refNo, bot_invDate, bot_seqNo, bot_trxType, bot_invInternalID, " +
                                            "bot_trxProgressStatus, bot_subsidiary, bot_subsidiaryInternalID, bot_salesType, bot_postingdate) " +
                                            "values ('" + q1.nsjom_nsj_job_ID + '.' + q1.nsjom_moNo + '.' + q1.nsjom_country + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'BCAS-CREDIT MEMO', " +
                                            "'" + q1.nsjom_moNo + "','" + gjob_id.ToString() + "', 'MY','3','RETURN','" + convertDateToString(_postDate) + "')";
                                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + insSalesTrx);
                                        entities.Database.ExecuteSqlCommand(insSalesTrx);

                                        raCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region SG Sales
                                else if (q1.nsjom_country.Equals("SG"))
                                {
                                }
                                #endregion
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASCreditMemo Exception: " + ex.ToString());
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
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update bcas_otherstransaction set bot_trxProgressStatus = '" + jobID + "' where bot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("BCASCreditMemo: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("BCASCreditMemo Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASCreditMemo: Login Netsuite failed.");
                }
            }//end of scopeOuter
            //logout();
            return status;
        }

        public Boolean BCASReturnAuthorize(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            this.DataFromNetsuiteLog.Info("BCASReturnAuthorize *****************");
            Boolean status = false;

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
                        this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: Login Netsuite success.");
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
                    this.DataFromNetsuiteLog.Fatal("BCASReturnAuthorize: Login Netsuite failed. Exception : " + ex.ToString());

                }
                //

                // Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("BCASReturnAuthorize: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Int32 raCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var grpReturn = (from q1 in entities.netsuite_jobordmaster
                                         where q1.nsjom_createdDate > rangeFrom
                                         && q1.nsjom_createdDate <= rangeTo
                                         && q1.nsjom_moNo.StartsWith("R")
                                         select new
                                         {
                                             q1.nsjom_jobOrdMaster_ID,
                                             q1.nsjom_nsj_job_ID,
                                             q1.nsjom_jobmo_id,
                                             q1.nsjom_ordRecNo,
                                             q1.nsjom_ordStudent,
                                             q1.nsjom_moNo,
                                             q1.nsjom_country
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];
                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                refInv.type = InitializeRefType.cashSale;
                                refInv.internalId = "000000";
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                //TBA
                                netsuiteService.tokenPassport = createTokenPassport();
                                ReadResponse rrInv = netsuiteService.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.BCAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.nsjom_country)
                                    {
                                        case "MY"://hard code
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                                            break;
                                        case "SG"://hard code
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                                            break;
                                        case "ID"://hard code
                                            refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = "Refund for " + q1.nsjom_ordStudent;

                                raInv2.tranDate = Convert.ToDateTime(DateTime.Now);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region ID Sales
                                if (q1.nsjom_country.Equals("ID"))
                                {
                                }
                                #endregion
                                #region MY Sales
                                else if (q1.nsjom_country.Equals("MY"))
                                {
                                    var query2 = (from ji in entities.netsuite_jobordmaster
                                                  join jipack in entities.netsuite_jobordmaster_pack on ji.nsjom_jobOrdMaster_ID equals jipack.nsjomp_jobOrdMaster_ID
                                                  join packdetail in entities.netsuite_jobordmaster_packdetail on jipack.nsjomp_jobOrdMaster_pack_ID equals packdetail.nsjompd_jobOrdMaster_pack_ID
                                                  where ji.nsjom_nsj_job_ID == q1.nsjom_nsj_job_ID
                                                  && ji.nsjom_moNo == q1.nsjom_moNo
                                                  && ji.nsjom_country == q1.nsjom_country
                                                  && ji.nsjom_createdDate > rangeFrom
                                                  && ji.nsjom_createdDate <= rangeTo
                                                  select new
                                                  {
                                                      ji.nsjom_nsj_job_ID,
                                                      ji.nsjom_moNo,
                                                      jipack.nsjomp_jobOrdMaster_pack_ID,
                                                      jipack.nsjomp_ordPack,
                                                      jipack.nsjomp_ordQty,
                                                      jipack.nsjomp_ordPrice,
                                                      jipack.nsjomp_gstamount,
                                                      packdetail.nsjompd_item_internalID,
                                                      packdetail.nsjompd_tax_code,
                                                      packdetail.nsjompd_item_price,
                                                      qty = (jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty),
                                                      grpAmount = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_item_price),
                                                      grpGst = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_gstamount),
                                                      grpDeliveryCharge = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_deliveryCharge),
                                                      grpDeliveryChargeGst = ((jipack.nsjomp_ordQty * packdetail.nsjompd_sku_qty) * packdetail.nsjompd_deliveryChargeGst)
                                                  }).ToList();

                                    var findRouding = (from p in query2
                                                       let k = new
                                                       {
                                                           packIDGrp = p.nsjomp_jobOrdMaster_pack_ID,
                                                           ordPackGrp = p.nsjomp_ordPack,
                                                           ordPackQtyGrp = p.nsjomp_ordQty,
                                                           ordPackPriceGrp = p.nsjomp_ordPrice,
                                                           ordPackGstGrp = p.nsjomp_gstamount,
                                                       }
                                                       group p by k into g
                                                       select new
                                                       {
                                                           ordpackID = g.Key.packIDGrp,
                                                           ordPack = g.Key.ordPackGrp,
                                                           ordPackQty = g.Key.ordPackQtyGrp,
                                                           ordPackPrice = g.Key.ordPackPriceGrp,
                                                           ordPackGst = g.Key.ordPackGstGrp,
                                                           sumISBN = g.Count(),
                                                           sumISBNfulfilled = g.Sum(p => p.qty),
                                                           sumOrdPackAmount = g.Sum(p => p.grpAmount),
                                                           sumOrdPackGst = g.Sum(p => p.grpGst),
                                                           sumOrdPackDeliveryCharge = g.Sum(p => p.grpDeliveryCharge),
                                                           sumOrdPackDeliveryChargeGst = g.Sum(p => p.grpDeliveryChargeGst)
                                                       }).ToList();

                                    double deliveryCharge = 0;
                                    double deliveryChargeGst = 0;
                                    double roudingAmount = 0;
                                    double roudingGst = 0;
                                    foreach (var grpItem in findRouding)
                                    {
                                        int packFulfilled = 0;
                                        double packAmount = 0;
                                        double packGst = 0;
                                        double totItemsAmount = 0;
                                        double totItemsGst = 0;
                                        packFulfilled = (Convert.ToInt32(grpItem.sumISBNfulfilled) / Convert.ToInt32(grpItem.sumISBN));
                                        packAmount = (packFulfilled * Convert.ToDouble(grpItem.ordPackPrice));
                                        packGst = (packFulfilled * Convert.ToDouble(grpItem.ordPackGst));
                                        totItemsAmount = (Convert.ToDouble(grpItem.sumOrdPackAmount) + Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge));
                                        totItemsGst = (Convert.ToDouble(grpItem.sumOrdPackGst) + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));
                                        deliveryCharge = (deliveryCharge + (Convert.ToDouble(grpItem.sumOrdPackDeliveryCharge) - Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst)));
                                        deliveryChargeGst = (deliveryChargeGst + Convert.ToDouble(grpItem.sumOrdPackDeliveryChargeGst));
                                        roudingAmount = roudingAmount + ((packAmount - packGst) - (totItemsAmount - totItemsGst));
                                        roudingGst = roudingGst + (packGst - totItemsGst);
                                    }

                                    var groupQ2 = from p in query2
                                                  let k = new
                                                  {
                                                      moNoGrp = p.nsjom_moNo,
                                                  }
                                                  group p by k into g
                                                  select new
                                                  {
                                                      moNo = g.Key.moNoGrp,
                                                      fulFillQty = g.Sum(p => p.qty),
                                                      totAmount = g.Sum(p => p.grpAmount),
                                                      totGst = g.Sum(p => p.grpGst),
                                                  };

                                    if (groupQ2.Count() > 0)
                                    {
                                        ReturnAuthorizationItem[] rasoii = new ReturnAuthorizationItem[3];
                                        ReturnAuthorizationItemList rasoil = new ReturnAuthorizationItemList();
                                        Int32 itemCount = 0;

                                        #region Return-Actual
                                        foreach (var item in groupQ2)
                                        {
                                            ReturnAuthorizationItem rasoi = new ReturnAuthorizationItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();
                                            RecordRef refPriceLevel = new RecordRef();

                                            //Items
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_RETNNONINV_INTERNALID;
                                            rasoi.item = refItem;

                                            //Price Level
                                            refPriceLevel.internalId = "-1";//Custom
                                            rasoi.price = refPriceLevel;

                                            rasoi.quantity = Convert.ToDouble(1);
                                            rasoi.quantitySpecified = true;

                                            //Unit Price/Rate
                                            double calc_rate1 = Math.Round((Convert.ToDouble(item.totAmount) - Convert.ToDouble(item.totGst)), 2);
                                            double calc_rate2 = Math.Round(calc_rate1 / Convert.ToDouble(1), 2);

                                            rasoi.rate = Convert.ToString(Math.Round(calc_rate2, 2));
                                            rasoi.amount = Math.Round(calc_rate1, 2);
                                            rasoi.amountSpecified = true;

                                            //Gst Amount
                                            rasoi.tax1Amt = Math.Round(Convert.ToDouble(item.totGst), 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Convert.ToDouble(item.totGst) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            rasoi.taxCode = refTaxCode;

                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion
                                        #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling
                                        if ((deliveryCharge != 0) || (deliveryChargeGst != 0))
                                        {
                                            ReturnAuthorizationItem rasoi = new ReturnAuthorizationItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_DC_INTERNALID;
                                            rasoi.item = refItem;

                                            rasoi.quantity = 1;
                                            rasoi.quantitySpecified = true;

                                            rasoi.rate = Convert.ToString(Math.Round(deliveryCharge, 2));

                                            rasoi.amount = Math.Round(deliveryCharge, 2);
                                            rasoi.amountSpecified = true;

                                            rasoi.tax1Amt = Math.Round(deliveryChargeGst, 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(deliveryChargeGst, 2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            rasoi.taxCode = refTaxCode;

                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion
                                        #region Rounding
                                        if ((roudingAmount != 0) || (roudingGst != 0))
                                        {
                                            ReturnAuthorizationItem rasoi = new ReturnAuthorizationItem();
                                            RecordRef refItem = new RecordRef();
                                            RecordRef refTaxCode = new RecordRef();

                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.BCAS_MY_ROUDING_INTERNALID;
                                            rasoi.item = refItem;

                                            rasoi.quantity = 1;
                                            rasoi.quantitySpecified = true;

                                            rasoi.rate = Convert.ToString(roudingAmount);

                                            rasoi.amount = Math.Round(roudingAmount, 2);
                                            rasoi.amountSpecified = true;

                                            rasoi.tax1Amt = Math.Round(roudingGst, 2);
                                            rasoi.tax1AmtSpecified = true;

                                            //Tax Code
                                            if (Math.Round(roudingGst, 2) != 0)
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_SR_INTERNALID;
                                            }
                                            else
                                            {
                                                refTaxCode.internalId = @Resource.BCAS_MY_TAXCODE_ZRL_INTERNALID;
                                            }
                                            rasoi.taxCode = refTaxCode;
                                            rasoii[itemCount] = rasoi;
                                            itemCount++;
                                        }
                                        #endregion

                                        rasoil.item = rasoii;
                                        raInv2.itemList = rasoil;
                                        raList[raCount] = raInv2;
                                        rowCount = raCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-RETURN AUTHORIZATION', '" + q1.nsjom_nsj_job_ID + '.' + q1.nsjom_moNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        DateTime _postDate = Convert.ToDateTime(DateTime.Now);
                                        var insSalesTrx = "insert into bcas_otherstransaction (bot_refNo, bot_invDate, bot_seqNo, bot_trxType, bot_invInternalID, " +
                                            "bot_trxProgressStatus, bot_subsidiary, bot_subsidiaryInternalID, bot_salesType, bot_postingdate) " +
                                            "values ('" + q1.nsjom_nsj_job_ID + '.' + q1.nsjom_moNo + '.' + q1.nsjom_country + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'BCAS-RETURN AUTHORIZATION', " +
                                            "'" + q1.nsjom_moNo + "','" + gjob_id.ToString() + "', 'MY','3','RETURN','" + convertDateToString(_postDate) + "')";
                                        this.DataFromNetsuiteLog.Debug("BCASReturnAuthorizes: " + insSalesTrx);
                                        entities.Database.ExecuteSqlCommand(insSalesTrx);

                                        raCount++;
                                        status = true;
                                    }
                                }
                                #endregion
                                #region SG Sales
                                else if (q1.nsjom_country.Equals("SG"))
                                {
                                }
                                #endregion
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("BCASReturnAuthorize Exception: " + ex.ToString());
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
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncAddList(raList);
                                    String jobID = job.jobId;
                                    

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    var updTrx = "update bcas_otherstransaction set bot_trxProgressStatus = '" + jobID + "' where bot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: " + updTrx);
                                    entities.Database.ExecuteSqlCommand(updTrx);

                                    //scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("BCASReturnAuthorize: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("BCASReturnAuthorize Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASReturnAuthorize: Login Netsuite failed.");
                }
            }//end of scopeOuter
            //logout();
            return status;
        }

        public void UnfulfillBinTransfer(Hashtable htWMSItemsQTY, string jobID, string rangeFrom, string rangeTo)
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
                        IA.binNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN_COMMIT };
                        IA.toBinNumber = new RecordRef { internalId = @Resource.BCAS_DEFAULT_BIN_UNFULFILL };
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
                BT.location = new RecordRef { internalId = @Resource.BCAS_DUMMYSALES_LOCATION };// cpng temp
                BT.memo = "BCAS REVERSE BIN";
                if (binItemCount > 0)
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

                    //Boolean loginStatus = false;
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
                            this.DataFromNetsuiteLog.Debug("UnfulfillBinTransfer: Login Netsuite success.");
                            //loginStatus = true;
                            //TBA
                            netsuiteService.tokenPassport = createTokenPassport();
                            var btResponse = netsuiteService.add(BT);// perform bin transfer for reverse
                            if (!btResponse.status.isSuccess)
                            {
                                this.DataFromNetsuiteLog.Fatal("BCAS Bin Transfer Unfulfill Exception: " + btResponse.status.statusDetail[0].message.ToString()
                                    + " Please check jobID = " + jobID
                                    + " Range from = " + rangeFrom
                                    + " Range to = " + rangeTo);
                            }
                        }
                        else
                        {
                            // loginStatus = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        // loginStatus = false;
                        this.DataFromNetsuiteLog.Fatal("UnfulfillBinTransfer: Login Netsuite failed. Exception : " + ex.ToString());

                    }

                    //

                }

            }
            /*cpng end*/
        }
        //NETSUITE PHASE II (TRX WITH AMOUNT) -END

        /*
        public Boolean BCASSOFulfillmentUpdate(sdeEntities entities, RequestNetsuiteEntity r)
        {
            this.DataFromNetsuiteLog.Info("BCASSOFulfillmentUpdate *****************");

            Boolean status = false;
            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("BCASSOFulfillmentUpdate: Login Netsuite success.");

                    AsyncStatusResult job = new AsyncStatusResult();
                    Int32 rowCount = 0;
                    Guid gjob_id = Guid.NewGuid();

                    var salesOrder = (from t in entities.requestnetsuite_task
                                      where t.rnt_jobID == r.rn_jobID
                                      select t).ToList();

                    this.DataFromNetsuiteLog.Info("BCASSOFulfillmentUpdate: " + salesOrder.Count() + " records to update.");

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
                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-SALES ORDER FULFILLMENT (BCAS-SALES ORDER)', 'REQUESTNETSUITETASK.RNT_ID." + so.rnt_id.ToString() + "', '" + gjob_id.ToString() + "'," +
                                "'START', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + rowCount + "','" + so.rnt_createdFromInternalId + "')";
                                this.DataFromNetsuiteLog.Debug("BCASSOFulfillmentUpdate: " + insertTask);
                                entities.Database.ExecuteSqlCommand(insertTask);

                                fulFillCount++;
                                this.DataFromNetsuiteLog.Debug("BCASSOFulfillmentUpdate: Sales order internalID_moNo: " + so.rnt_createdFromInternalId);

                                status = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("BCASSOFulfillmentUpdate Exception: " + ex.ToString());
                            status = false;
                        }
                    }//end of ordMaster
                    if (status == true)
                    {
                        job = service.asyncAddList(iffList);
                        String jobID = job.jobId;

                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                        this.DataFromNetsuiteLog.Debug("BCASSOFulfillmentUpdate: " + updateTask);
                        entities.Database.ExecuteSqlCommand(updateTask);

                        var insertRequestNetsuite = "insert into requestnetsuite (rn_sche_transactionType,rn_createdDate,rn_status,rn_seqNo,rn_jobID,rn_rangeFrom," +
                            "rn_rangeTo,rn_updatedDate) values ('BCAS-SALES ORDER FULFILLMENT','" + convertDateToString(DateTime.Now) + "','START','1'," +
                            "'" + jobID + "','" + convertDateToString(Convert.ToDateTime(r.rn_rangeFrom)) + "','" + convertDateToString(Convert.ToDateTime(r.rn_rangeTo)) +
                            "','" + convertDateToString(DateTime.Now) + "')";
                        this.DataFromNetsuiteLog.Debug("BCASSOFulfillmentUpdate: " + insertRequestNetsuite);
                        entities.Database.ExecuteSqlCommand(insertRequestNetsuite);
                        scope1.Complete();
                    }
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("BCASSOFulfillmentUpdate: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        */
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
