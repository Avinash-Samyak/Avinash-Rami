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

namespace sde.Models
{
    public class linq_stocktransfer 
    {
        //NetSuite
        private static NetSuiteService service = new NetSuiteService();
         private static List<string> jobIDList = new List<string>();
        private static object syncRoot = new object();
        private static string jobId; 
        private static Timer addListTimer;
        private static Boolean cancelation = false;
        private static Boolean netsuiteResponse = true;
        
        //Progress
        private static IDictionary<string, int> ProcessStatus { get; set; }
        private static object[] ProcessData; 
         
        public linq_stocktransfer()
        {
            if (ProcessStatus == null)
            {
                ProcessStatus = new Dictionary<string, int>();
            } 
        } 

        public string NetsuiteStockTransfer(string id)
        {  
            string errorMessage = null;
            int countNo = 0;
            ProcessData = new object[4];
            Boolean status = false;
            //using (TransactionScope scope1 = new TransactionScope())
            //{
            Boolean loginStatus = login(service);
            if (loginStatus == true)
            {
                using (sdeEntities entities = new sdeEntities())
                {
                    AsyncStatusResult job = new AsyncStatusResult();
                    Int32 daCount = 0;
                    Guid gjob_id = Guid.NewGuid();


                    var directTransfer = (from nat in entities.netsuite_transfer
                                          select nat).ToList();

                    InventoryTransfer[] invTransferList = new InventoryTransfer[directTransfer.Count()];

                    ProcessData[0] = "StockTransferListPrepare";  //Progress - Task Id
                    ProcessData[1] = directTransfer.Count(); //Progress - Total Records 
                    ProcessData[2] = 0;    //Progress - Reset
                    ProcessData[3] = ""; //Progress - Reset 

                    if (directTransfer.Count > 0)
                    {
                        foreach (var d in directTransfer)
                        {
                            if (cancelation == true)
                            {
                                ProcessData[0] = "Canceled";
                                ProcessData[1] = "";
                                ProcessData[2] = "";
                                ProcessData[3] = "";
                                cancelation = false;
                                return id;
                            }
                            lock (syncRoot)
                            {
                                countNo = countNo + 1; //Progress - Counting for current record
                                ProcessData[2] = countNo; //Progress - Current Record 
                                try
                                {
                                    Int32 itemCount = 0;
                                    InventoryTransfer invTransfer = new InventoryTransfer();
                                    InventoryTransferInventoryList itil = new InventoryTransferInventoryList();

                                    RecordRef refSubsidiary = new RecordRef();
                                    refSubsidiary.internalId = d.nat_subsidiaryID.ToString();

                                    RecordRef refFromLocation = new RecordRef();
                                    refFromLocation.internalId = d.nat_fromLocID.ToString();

                                    RecordRef refToLocation = new RecordRef();
                                    refToLocation.internalId = d.nat_toLocID.ToString();

                                    RecordRef refBusinessChannel = new RecordRef();
                                    refBusinessChannel.internalId = d.nat_businessChannelID.ToString();

                                    RecordRef refPostingPeriod = new RecordRef();
                                    refPostingPeriod.internalId = d.nat_postingPeriodID.ToString();

                                    invTransfer.subsidiary = refSubsidiary;
                                    invTransfer.@class = refBusinessChannel;
                                    invTransfer.location = refFromLocation;
                                    invTransfer.transferLocation = refToLocation;
                                    invTransfer.tranDate = Convert.ToDateTime(d.nat_Date);
                                    invTransfer.tranDateSpecified = true;
                                    invTransfer.externalId = "Stock Transfer " + d.nat_id.ToString();
                                    invTransfer.postingPeriod = refPostingPeriod;
                                    invTransfer.memo = d.nat_Memo;

                                    ProcessData[3] = invTransfer.memo;  //Progess - Current Memo Info

                                    var directTransferItem = (from natd in entities.netsuite_transferdetail
                                                              where natd.natd_businessChannelID == d.nat_businessChannelID &&
                                                              natd.natd_fromLocID == d.nat_fromLocID &&
                                                              natd.natd_toLocID == d.nat_toLocID &&
                                                              natd.natd_subsidiaryID == d.nat_subsidiaryID &&
                                                              natd.natd_date == d.nat_Date &&
                                                              natd.nat_postingPeriodID == d.nat_postingPeriodID
                                                              select natd).ToList();


                                    if (directTransferItem.Count() > 0)
                                    {
                                        if (cancelation == true)
                                        {
                                            ProcessData[0] = "Canceled";
                                            ProcessData[1] = "";
                                            ProcessData[2] = "";
                                            ProcessData[3] = "";
                                            cancelation = false;
                                            return id;
                                        }

                                        InventoryTransferInventory[] items = new InventoryTransferInventory[directTransferItem.Count()];

                                        foreach (var i in directTransferItem)
                                        {
                                            RecordRef refItem = new RecordRef();
                                            refItem.internalId = i.natd_itemInternalID.ToString();

                                            InventoryTransferInventory item = new InventoryTransferInventory();

                                            item.item = refItem;
                                            item.adjustQtyBy = Double.Parse(i.natd_qty.ToString());
                                            item.adjustQtyBySpecified = true;
                                            //item.description = i.natd_itemID;
                                            items[itemCount] = item;
                                            itemCount++;
                                        }
                                        itil.inventory = items;
                                        invTransfer.inventoryList = itil;
                                        invTransferList[daCount] = invTransfer;

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
                                    errorMessage = ex.Message;
                                    //this.DataFromNetsuiteLog.Error(ex.ToString()); 
                                    status = false;
                                    ProcessData[0] = "StockTransferListPrepareError"; //Progress - Error Task Id
                                    ProcessData[1] = 0;//Progress - Nothing
                                    ProcessData[2] = 0;//Progress - Nothing
                                    ProcessData[3] = errorMessage; //Progress - Error Message thrown
                                    return id;
                                }
                            }
                        }//end of directAdj
                    }
                    else
                    {
                        status = false;
                        errorMessage = "No data found from database. Please load the csv file.";
                        ProcessData[0] = "StockTransferListPrepareError"; //Progress - Error Task Id
                        ProcessData[1] = 0;//Progress - Nothing
                        ProcessData[2] = 0;//Progress - Nothing
                        ProcessData[3] = errorMessage; //Progress - Error Message thrown
                        return id;
                    }
                    if (status == true)
                    {
                        try
                        {
                            login(service);
                            InventoryTransfer[] divInvTransferList = new InventoryTransfer[400];

                            ProcessData[0] = "SyncNetsuite";  //Progress - Task Id
                            if (directTransfer.Count() % 400 == 0)
                            {
                                ProcessData[1] = directTransfer.Count() / 400;  //Progress - Total Records
                            }
                            else
                            {
                                ProcessData[1] = directTransfer.Count() / 400 + 1;  //Progress - Total Records
                            }
                            ProcessData[2] = 0;    //Progress - Reset
                            ProcessData[3] = ""; //Progress - Reset 

                            for (int i = 0; i < invTransferList.Length; i = i + 400)
                            {

                                if (invTransferList.Length - i < 400)
                                {
                                    ProcessData[2] = i / 400 + 1;//Progress - Current Record
                                    AsyncStatusResult asyncRes = new AsyncStatusResult();
                                    Array.Resize(ref divInvTransferList, invTransferList.Length - i);
                                    Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, invTransferList.Length - i);
                                    asyncRes = service.asyncAddList(divInvTransferList);
                                    jobId = asyncRes.jobId;
                                    jobIDList.Add(jobId);

                                }
                                else
                                {
                                    ProcessData[2] = i / 400 + 1;//Progress - Current Record
                                    AsyncStatusResult asyncRes = new AsyncStatusResult();
                                    Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, 400);
                                    asyncRes = service.asyncAddList(divInvTransferList);
                                    jobId = asyncRes.jobId;
                                    jobIDList.Add(jobId);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            ProcessData[0] = "SyncNetsuiteError";
                            ProcessData[1] = 0;//Progress - Nothing
                            ProcessData[2] = 0;//Progress - Nothing
                            ProcessData[3] = ex.Message; //Progress - Error Message thrown
                            return id;
                        }

                        ProcessData[0] = "NetsuiteProcessing";
                        ProcessData[2] = "0";
                        ProcessData[3] = "";

                        addListTimer = new System.Timers.Timer(300000);
                        addListTimer.Elapsed += onTimedEvent;
                        addListTimer.Enabled = true;

                        while (addListTimer.Enabled == true)
                        {
                            if (netsuiteResponse == false)
                            {
                                addListTimer.Stop();
                            }
                        }

                    }
                    ;
                }//end of sdeEntities
            }
            else
            {
                errorMessage = "Netsuite Login Failed";
                ProcessData[0] = "LoginError"; //Progress - Error Task Id
                ProcessData[1] = 0;//Progress - Nothing
                ProcessData[2] = 0;//Progress - Nothing
                ProcessData[3] = errorMessage; //Progress - Error Message thrown
                //throw new Exception(errorMessage);
            }
            //}//end of scope1

            return id;
            //return View();
        }
        

        /// <summary>
        /// Adds the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        public void Add(string id)
        {
            lock (syncRoot)
            {
                ProcessStatus.Add(id, 0);
            }
        }

        public void Cancel(string id)
        {
                cancelation = true;
        }

        /// <summary>
        /// Removes the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        public void Remove(string id)
        {
            lock (syncRoot)
            {
                ProcessStatus.Remove(id);
            }
        }



        /// <summary>
        /// Gets the status.
        /// </summary>
        /// <param name="id">The id.</param>
        public object[] GetStatus(string id)
        {
            lock (syncRoot)
            { 
                return ProcessData;
            }
        }

        public static Boolean login(NetSuiteService service)
        {
            try
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
                passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;
                return service.login(passport).status.isSuccess;
            }
            catch
            {
                return false;
            }
        }

        private static void onTimedEvent(Object source, ElapsedEventArgs e)
        {
            try
            {
                login(service);
                List<string> completedJobList = new List<string>();
                List<string> unCompletedJobList = new List<string>();
                foreach (string jobID in jobIDList)
                {
                    AsyncStatusResult result = service.checkAsyncStatus(jobID);
                    if (result.status == AsyncStatusType.pending || result.status == AsyncStatusType.processing)
                    {
                        unCompletedJobList.Add(jobID);
                    }
                    else
                    {
                        completedJobList.Add(jobID);
                    }
                }

                ProcessData[0] = "NetsuiteProcessing"; //Progress - Error Task Id
                ProcessData[2] = completedJobList.Count;
                ProcessData[3] = "";

                if (unCompletedJobList.Count == 0)
                {
                    addListTimer.Enabled = false;
                }
            }
            catch ( Exception ex)
            {
                netsuiteResponse = false;
                ProcessData[0] = "NetsuiteResponseError"; //Progress - Error Task Id
                ProcessData[1] = 0;//Progress - Nothing
                ProcessData[2] = 0;//Progress - Nothing
                ProcessData[3] = ex.Message; //Progress - Error Message thrown 
            }
        }
    }
}
