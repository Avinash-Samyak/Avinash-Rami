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
using System.Threading;

namespace sde.Models
{
    public class linq_adjustment  
    {
        //NetSuite
        private static NetSuiteService service = new NetSuiteService();
         private static List<string> jobIDList = new List<string>();
        private static object syncRoot = new object();
        private static string jobId; 
        private static System.Timers.Timer addListTimer;
        private static Boolean cancelation = false;
        private static Boolean netsuiteResponse = true;
        
        //Progress
        private static IDictionary<string, int> ProcessStatus { get; set; }
        private static object[] ProcessData; 
         
        public linq_adjustment()
        {
            if (ProcessStatus == null)
            {
                ProcessStatus = new Dictionary<string, int>();
            } 
        } 

        public string NetsuiteAdjustment(string id)
        {  
            string errorMessage = null;
            Boolean status = false;
            
            int countNo = 0; // Progress - Curent Record
            // Progress - Curent Net Record
            ProcessData = new object[4]; //[0] = Task Id, [1] = Total Records, [2] = Current Records, [3] = Current Memo Info

            Boolean loginStatus = login(service);
   
            if (loginStatus == true)
            { 
                using (sdeEntities entities = new sdeEntities())
                { 
                    AsyncStatusResult job = new AsyncStatusResult();
                    Int32 daCount = 0;
                    Guid gjob_id = Guid.NewGuid();
                     
                    //var directAdj = (from da in entities.wms_directadjustment
                    //                 //where da.da_rangeTo == rangeTo
                    //                 select da).ToList();

                    var directAdj = (from nas in entities.netsuite_adjustment2
                                    select nas).ToList();
                       
                    InventoryAdjustment[] invAdjList = new InventoryAdjustment[directAdj.Count()];

                    ProcessData[0] = "AdjListPrepare";  //Progress - Task Id
                    ProcessData[1] = directAdj.Count(); //Progress - Total Records 
                    ProcessData[2] = 0;    //Progress - Reset
                    ProcessData[3] = ""; //Progress - Reset 
                    if (directAdj.Count > 0)
                    {
                        foreach (var d in directAdj)
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
                                    InventoryAdjustment invAdj = new InventoryAdjustment();
                                    InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                                    RecordRef refAccount = new RecordRef();
                                    refAccount.internalId = d.nas_accountNo.ToString();

                                    RecordRef refSubsidiary = new RecordRef();
                                    refSubsidiary.internalId = d.nas_subsidiaryID.ToString();

                                    RecordRef refPostingPeriod = new RecordRef();
                                    refPostingPeriod.internalId = d.nas_postingPeriodID.ToString();

                                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                    RecordRef refBusinessChannel = new RecordRef();
                                    refBusinessChannel.internalId = d.nas_businessChannelID.ToString();
                                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;

                                    if (d.nas_Type == "RCV" || d.nas_Type.ToUpper() == "RECEIVING")
                                    {
                                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RCV;
                                    }
                                    else if (d.nas_Type == "ADJ" || d.nas_Type.ToUpper() == "ADJUSTMENT")
                                    {
                                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_ADJ;
                                    }
                                    else if (d.nas_Type == "Write-off/Damage" || d.nas_Type.ToUpper() == "WRITEOFF")
                                    {
                                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_WRITEOFFDMG;
                                    }
                                    cfrList[0] = scfr;

                                    invAdj.customFieldList = cfrList;
                                    invAdj.account = refAccount;
                                    invAdj.memo = d.nas_shipmentNo + " " + d.nas_firstShipmentDate.ToString() + " " + d.nas_businessChannel + " " + d.nas_locationID.ToString() + " " + d.nas_Type + " " + d.nas_postingPeriodID.ToString();
                                    invAdj.tranDate = Convert.ToDateTime(d.nas_firstShipmentDate);
                                    invAdj.tranDateSpecified = true;
                                    invAdj.subsidiary = refSubsidiary;
                                    invAdj.@class = refBusinessChannel;
                                    invAdj.postingPeriod = refPostingPeriod;
                                    invAdj.externalId = "Adjustment " + d.nas_ID.ToString();

                                    ProcessData[3] = invAdj.memo;  //Progess - Current Memo Info

                                    var directAdjItem = (from nai in entities.netsuite_adjustmentdetail2
                                                         where nai.nad_shipmentNo == d.nas_shipmentNo &&
                                                         nai.nad_firstShipmentDate == d.nas_firstShipmentDate &&
                                                         nai.nad_type == d.nas_Type &&
                                                         nai.nad_nsLocationID == d.nas_locationID &&
                                                         nai.nad_businessChannel == d.nas_businessChannel &&
                                                         nai.nad_postingPeriodID == d.nas_postingPeriodID
                                                         select nai).ToList();

                                    if (directAdjItem.Count() > 0)
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
                                    errorMessage = ex.Message;
                                    //this.DataFromNetsuiteLog.Error(ex.ToString()); 
                                    status = false;
                                    ProcessData[0] = "AdjListPrepareError"; //Progress - Error Task Id
                                    ProcessData[1] = 0;//Progress - Nothing
                                    ProcessData[2] = 0;//Progress - Nothing
                                    ProcessData[3] = errorMessage; //Progress - Error Message thrown
                                    return id;
                                }

                            }
                        }//end of directAdj
                    }//end of if directAdj.Count > 0
                    else
                    {
                        errorMessage = "No data found from database. Please load csv file into database";
                        status = false;
                        ProcessData[0] = "AdjListPrepareError"; //Progress - Error Task Id
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
                            //job = service.asyncAddList(invAdjList);
                            //WriteResponse[] writeRes = service.addList(invAdjList);
                            InventoryAdjustment[] divInvAdjList = new InventoryAdjustment[400];

                            ProcessData[0] = "SyncNetsuite";  //Progress - Task Id
                            if (directAdj.Count() % 400 == 0)
                            {
                                ProcessData[1] = directAdj.Count() / 400;  //Progress - Total Records
                            }
                            else
                            {
                                ProcessData[1] = directAdj.Count() / 400 + 1;  //Progress - Total Records
                            }
                            ProcessData[2] = 0;    //Progress - Reset
                            ProcessData[3] = ""; //Progress - Reset 

                            for (int i = 0; i < invAdjList.Length; i = i + 400)
                            {
                                if (invAdjList.Length - i < 400)
                                {
                                    ProcessData[2] = i / 400 + 1;//Progress - Current Record
                                    AsyncStatusResult asyncRes = new AsyncStatusResult();
                                    Array.Resize(ref divInvAdjList, invAdjList.Length - i);
                                    Array.ConstrainedCopy(invAdjList, i, divInvAdjList, 0, invAdjList.Length - i);
                                    asyncRes = service.asyncAddList(divInvAdjList);
                                    jobId = asyncRes.jobId;
                                    jobIDList.Add(jobId);
                                }
                                else
                                {
                                    ProcessData[2] = i / 400 + 1;//Progress - Current Record
                                    AsyncStatusResult asyncRes = new AsyncStatusResult();
                                    Array.ConstrainedCopy(invAdjList, 400, divInvAdjList, 0, 400);
                                    asyncRes = service.asyncAddList(divInvAdjList);
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

                            //addListTimer = new System.Timers.Timer(900000);
                            addListTimer = new System.Timers.Timer(300000);//5 mins
                            addListTimer.Elapsed += onTimedEvent;
                            addListTimer.Enabled = true;

                            while (addListTimer.Enabled == true)
                            {
                                if (netsuiteResponse == false)
                                {
                                    addListTimer.Stop();
                                }
                            }
                     
                    };
                }//end of sdeEntities 
            }
            else
            {
                //errorMessage = "Netsuite Login Failed";
                ProcessData[0] = "LoginError"; //Progress - Error Task Id
                ProcessData[1] = 0;//Progress - Nothing
                ProcessData[2] = 0;//Progress - Nothing
                ProcessData[3] = errorMessage; //Progress - Error Message thrown
                //throw new Exception(errorMessage);
            }

            return id;
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
                //passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;
                passport.password = getNetsuitePassword();
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
