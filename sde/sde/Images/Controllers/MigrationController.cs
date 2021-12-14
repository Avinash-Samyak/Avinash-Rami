/*
 * Date          Developer       Issue       Remark 
 * ------------------------------------------------------------------------------------------
 * 19/Mar/2014   David           #361        log4Net
 * 19/Mar/2014   David           #362        log4Net
 */

using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using sde.comNetsuiteServices;
using System.IO;
using System.Text;
using System.Collections;
using log4net;
using System.Reflection;
using log4net.Config;
using System.Messaging;
using System.Xml;
using sde.WCFsde;
using System.Transactions;
using System.Web.SessionState;
using System.Globalization;
using System.Data.Entity.Validation;
using System.Data.Entity.Infrastructure;
using sde.Models;
using Microsoft.VisualBasic.FileIO;
using System.Timers;
using System.Data;
using MySql.Data.MySqlClient;
using System.Configuration;
namespace sde.Controllers
{
    [Authorize]
    public class MigrationController : Controller
    {
        //NetSuite
        private static NetSuiteService service = new NetSuiteService();
        private static List<string> jobIDList = new List<string>();
        private static System.Timers.Timer addListTimer;
        private static string jobId;
        private static string mySqlError;
          
        public static Boolean login(NetSuiteService service)
          {
              service.Timeout = 820000000;
              service.CookieContainer = new CookieContainer();

              Passport passport = new Passport();
              passport.account = sde.Resource.NETSUITE_LOGIN_ACCOUNT;         //"3479023"
              passport.email = sde.Resource.NETSUITE_LOGIN_EMAIL;             // "xypang@scholastic.asia"

              RecordRef role = new RecordRef();
              role.internalId = sde.Resource.NETSUITE_LOGIN_ROLE_INTERNALID;  //"18"

              passport.role = role;
              passport.password = sde.Resource.NETSUITE_LOGIN_PASSWORD;
              return service.login(passport).status.isSuccess;
          }

        [HttpGet]
        public ActionResult StockTransfer()
        {
            return View();
        }

        [HttpGet]
        public ActionResult Adjustment()
        {
            return View();
        }
      
        [HttpPost]
        public ActionResult NetSuiteStockTransfer()
        {
            string errorMessage = null;
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

                    foreach (var d in directTransfer)
                    {
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
                        }
                    }//end of directAdj

                    if (status == true)
                    {
                        login(service);
                        InventoryTransfer[] divInvTransferList = new InventoryTransfer[400];
                        for (int i = 0; i < invTransferList.Length; i = i + 400)
                        {

                            if (invTransferList.Length - i < 400)
                            {
                                AsyncStatusResult asyncRes = new AsyncStatusResult();
                                Array.Resize(ref divInvTransferList, invTransferList.Length - i);
                                Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, invTransferList.Length - i);
                                string y = divInvTransferList.Length.ToString();
                                asyncRes = service.asyncAddList(divInvTransferList);
                                jobId = asyncRes.jobId;
                                jobIDList.Add(jobId);

                            }
                            else
                            {
                                AsyncStatusResult asyncRes = new AsyncStatusResult();
                                Array.ConstrainedCopy(invTransferList, i, divInvTransferList, 0, 400);
                                asyncRes = service.asyncAddList(divInvTransferList);
                                jobId = asyncRes.jobId;
                                jobIDList.Add(jobId);
                            }
                        }
                        addListTimer = new System.Timers.Timer(900000);
                        addListTimer.Elapsed += onTimedEvent;
                        addListTimer.Enabled = true;

                        while (addListTimer.Enabled == true)
                        {
                        }

                    }
                    ;
                }//end of sdeEntities
            }
            else
            {
                throw new Exception(errorMessage);
            }
            //}//end of scope1

            return Content("bah");
            //return View();
        }

        [HttpPost]
        public ActionResult NetsuiteAdjustment_Old()
        {
            string errorMessage = null;
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

                    //var directAdj = (from da in entities.wms_directadjustment
                    //                 //where da.da_rangeTo == rangeTo
                    //                 select da).ToList();

                    var directAdj = (from nas in entities.netsuite_adjustment2
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
                            invAdj.memo = d.nas_shipmentNo + " " + d.nas_firstShipmentDate.ToString() + " " + d.nas_businessChannel + " " + d.nas_locationID.ToString();
                            invAdj.tranDate = Convert.ToDateTime(d.nas_firstShipmentDate);
                            invAdj.tranDateSpecified = true;
                            invAdj.subsidiary = refSubsidiary;
                            invAdj.@class = refBusinessChannel;
                            invAdj.postingPeriod = refPostingPeriod;
                            invAdj.externalId = "Adjustment " + d.nas_ID.ToString();

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
                        }
                    }//end of directAdj

                    if (status == true)
                    {
                        login(service);
                        InventoryAdjustment[] divInvAdjList = new InventoryAdjustment[400];

                        List<string> jobIDList = new List<string>();

                        for (int i = 0; i < invAdjList.Length; i = i + 400)
                        {

                            if (invAdjList.Length - i < 400)
                            {
                                AsyncStatusResult asyncRes = new AsyncStatusResult();
                                Array.Resize(ref divInvAdjList, invAdjList.Length - i);
                                Array.ConstrainedCopy(invAdjList, i, divInvAdjList, 0, invAdjList.Length - i);
                                string y = divInvAdjList.Length.ToString();
                                asyncRes = service.asyncAddList(divInvAdjList);
                                jobId = asyncRes.jobId;
                                jobIDList.Add(jobId);

                            }
                            else
                            {
                                AsyncStatusResult asyncRes = new AsyncStatusResult();
                                Array.ConstrainedCopy(invAdjList, i, divInvAdjList, 0, 400);
                                asyncRes = service.asyncAddList(divInvAdjList);
                                jobId = asyncRes.jobId;
                                jobIDList.Add(jobId);
                            }
                        }

                        addListTimer = new System.Timers.Timer(900000);
                        addListTimer.Elapsed += onTimedEvent;
                        addListTimer.Enabled = true;

                        while (addListTimer.Enabled == true)
                        {
                        }


                    }
                    else
                    {
                        errorMessage = "Data retrieving failed or no data for adjustment available in database";
                        throw new Exception(errorMessage);
                    }
                    ;
                }//end of sdeEntities
            }
            else
            {
                errorMessage = "Netsuite Login Failed";
                throw new Exception(errorMessage);
            }
            //}//end of scope1
            //TempData["Alert"] = "Alert message";    
            //return JavaScript("<script language='javascript' type='text/javascript'>alert('Data Already Exists');</script>");
            //return RedirectToAction("Index", "Dashboard");
            return Content("bah");
            //return View();
        }

        [HttpPost]
        public ActionResult adjCsvFileUpload(HttpPostedFileBase file)
        {
            // Verify that the user selected a file
            if (file != null && file.ContentLength > 0)
            {
                // extract only the fielname
                var fileName = Path.GetFileName(file.FileName);
                // store the file inside ~/App_Data/uploads folder
                var path = Path.Combine(Server.MapPath("~/App_Data/"), fileName);
                file.SaveAs(path);

                DataTable csvData = GetDataTabletFromCSVFile(path.ToString());
                DBCONNECT delete = new DBCONNECT();
                if (delete.deleteAll() == false)
                {
                    return Json(mySqlError);
                }
                List<shipmentObject> adjList = new List<shipmentObject>();
                List<shipmentObject> adjDetailList = new List<shipmentObject>();
                List<string> groupingCheck = new List<string>();
                for (int i = 2; i < csvData.Rows.Count; i++)
                {
                    try
                    {
                        shipmentObject obj = new shipmentObject();
                        obj.analysisCode = csvData.Rows[i][0].ToString();
                        obj.shipmentNo = csvData.Rows[i][1].ToString();
                        string date1 = csvData.Rows[i][2].ToString();

                        if (date1.Length > 16)
                        {
                            csvData.Rows[i][2] = date1.Substring(0, date1.LastIndexOf(":"));
                        }

                        obj.shipmentDate = DateTime.Parse(csvData.Rows[i][2].ToString());
                        obj.ISBN13 = csvData.Rows[i][3].ToString();
                        obj.ISBN10 = csvData.Rows[i][4].ToString();
                        obj.receivedQty = Decimal.Parse(csvData.Rows[i][5].ToString());
                        obj.USDCost = Decimal.Parse(csvData.Rows[i][6].ToString());
                        obj.localCost = Decimal.Parse(csvData.Rows[i][7].ToString());
                        obj.totalAmt = Decimal.Parse(csvData.Rows[i][8].ToString());

                        string date = csvData.Rows[i][9].ToString();

                        if (date.Length > 16)
                        {
                            csvData.Rows[i][9] = date.Substring(0, date.LastIndexOf(":"));
                        }

                        obj.firstShipDate = DateTime.Parse(csvData.Rows[i][9].ToString());
                        obj.imasItem = csvData.Rows[i][10].ToString();
                        obj.Type = csvData.Rows[i][11].ToString();
                        obj.Location = csvData.Rows[i][12].ToString();
                        obj.BusinessChannel = csvData.Rows[i][13].ToString();
                        obj.nsLocation = csvData.Rows[i][14].ToString();
                        obj.itemInternalID = Int32.Parse(csvData.Rows[i][15].ToString());
                        obj.locationID = Int32.Parse(csvData.Rows[i][16].ToString());
                        obj.BusinessChannelID = Int32.Parse(csvData.Rows[i][17].ToString());
                        //obj.Subsidiary = csvData.Rows[i][19].ToString();
                        obj.subsidiaryID = Int32.Parse(csvData.Rows[i][18].ToString());
                        obj.accountNo = Int32.Parse(Resource.ADJUSTMENT_ACCOUNT_ID);
                        obj.postingPeriodID = Int32.Parse(csvData.Rows[i][19].ToString());


                        String group = obj.shipmentNo + obj.firstShipDate.Date + obj.Type + obj.BusinessChannel + obj.locationID + obj.postingPeriodID.ToString();
                        if (groupingCheck.Contains(group) == false)
                        {
                            groupingCheck.Add(group);
                            adjList.Add(obj);
                        }
                        adjDetailList.Add(obj);
                    }
                    catch (Exception ex)
                    {
                        return Json(ex.Message + "at row " + i.ToString());
                    }
                }

                DBCONNECT bulkInsert = new DBCONNECT();
                if (bulkInsert.bulkInsertAdjustment(adjList) == true)
                {
                    if (bulkInsert.bulkInsertAdjustmentDetail(adjDetailList) == false)
                    {
                        return Json(mySqlError);
                    }
                }
                else
                {
                    return Json(mySqlError);
                }

                return Json(file.FileName + " was loaded into db succesfully");
            }
            else
            {
                return Json(file.FileName + " is not a valid file. Please follow the file template format");
            }

            
        }

        [HttpPost]
        public ActionResult stCsvFileUpload(HttpPostedFileBase file)
        {
            // Verify that the user selected a file
            if (file != null && file.ContentLength > 0)
            {
                // extract only the fielname
                var fileName = Path.GetFileName(file.FileName);
                // store the file inside ~/App_Data/uploads folder
                var path = Path.Combine(Server.MapPath("~/App_Data/"), fileName);
                file.SaveAs(path);

                DataTable csvData = GetDataTabletFromCSVFile(path.ToString());
                DBCONNECT delete = new DBCONNECT();
                if (delete.deleteAllTransfer() == false)
                {
                    return Json(mySqlError);
                }
                List<string> groupingCheck = new List<string>();
                List<stockTransferObject> transferlist = new List<stockTransferObject>();
                List<stockTransferObject> transferdetailList = new List<stockTransferObject>();

                for (int i = 0; i < csvData.Rows.Count; i++)
                //for (int i = 1; i < 50000; i++)
                {

                    try
                    {
                        stockTransferObject obj = new stockTransferObject();
                        obj.Subsidiary = csvData.Rows[i][0].ToString();
                        obj.date = csvData.Rows[i][1].ToString();
                        obj.date = Convert.ToDateTime(obj.date).ToString("yyyy-MM-dd");
                        obj.PostingPeriod = csvData.Rows[i][2].ToString();
                        obj.fromLocation = csvData.Rows[i][3].ToString();
                        obj.toLocation = csvData.Rows[i][4].ToString();
                        obj.BusinessChannel = csvData.Rows[i][5].ToString();
                        obj.Memo = csvData.Rows[i][6].ToString();
                        obj.imasItem = csvData.Rows[i][7].ToString();
                        obj.ISBN13 = csvData.Rows[i][8].ToString();
                        obj.ISBN10 = csvData.Rows[i][9].ToString();
                        obj.quantity = Int32.Parse(csvData.Rows[i][10].ToString());
                        obj.subsidiaryID = Int32.Parse(csvData.Rows[i][11].ToString());
                        obj.postingPeriodID = Int32.Parse(csvData.Rows[i][12].ToString());
                        obj.fromLocationID = Int32.Parse(csvData.Rows[i][13].ToString());
                        obj.toLocationID = Int32.Parse(csvData.Rows[i][14].ToString());
                        transferdetailList.Add(obj);

                        String x = obj.fromLocationID.ToString() + obj.toLocationID.ToString() + obj.subsidiaryID + obj.BusinessChannelID + obj.postingPeriodID + obj.date;
                        if (groupingCheck.Contains(x) == false)
                        {
                            groupingCheck.Add(x);
                            transferlist.Add(obj);
                        obj.BusinessChannelID = Int32.Parse(csvData.Rows[i][15].ToString());
                        obj.itemInternalID = Int32.Parse(csvData.Rows[i][16].ToString());

                        }
                    }
                    catch (Exception ex)
                    {
                        return Json(ex.Message + "at row " + i.ToString());
                    }
                }

                DBCONNECT bulkinsert = new DBCONNECT();

                if (bulkinsert.bulkInsertTransfer(transferlist) == true)
                {
                    if (bulkinsert.bulkInsertTransferDetail(transferdetailList) == false)
                    {
                        return Json(mySqlError);
                    }
                }
                else
                {
                    return Json(mySqlError);
                }

                return Json(file.FileName + " was loaded into db succesfully");
            }
            else
            {
                return Json(file.FileName + " is not a valid file. Please follow the file template.");
            }
        }

        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    //read column names
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                 
            }
            return csvData;
        }

        private static void onTimedEvent(Object source, ElapsedEventArgs e)
        {
            login(service);

            foreach (string jobID in jobIDList)
            {
                AsyncStatusResult result = service.checkAsyncStatus(jobID);
                if (result.status == AsyncStatusType.pending || result.status == AsyncStatusType.processing)
                {
                    return;
                }
            }

            addListTimer.Enabled = false;
        }

        public class stockTransferObject
        {
            public string Memo, ISBN13, ISBN10, imasItem, fromLocation, toLocation, Subsidiary, BusinessChannel, analysisCode, shipmentNo;
            public int quantity, postingPeriodID;
            public decimal price;
            public int itemInternalID, subsidiaryID, BusinessChannelID, fromLocationID, toLocationID;
            public string date;
            public string PostingPeriod;

        }

        public class shipmentObject
        {
            public string analysisCode, shipmentNo, ISBN13, ISBN10, imasItem, Type, Location, nsLocation, Subsidiary, BusinessChannel, postingPeriod;
            public decimal receivedQty;
            public decimal USDCost, localCost, totalAmt;
            public DateTime shipmentDate, firstShipDate;
            public int itemInternalID, subsidiaryID, BusinessChannelID, locationID, accountNo, postingPeriodID;

        }

        public class DBCONNECT
        {
            public Boolean bulkInsertTransferDetail(List<stockTransferObject> list)
            {
                string query = "";
                string insertQuery = String.Format(@"INSERT INTO netsuite_transferdetail (`natd_subsidiaryID`,`natd_itemID`,`natd_itemInternalID`,`natd_qty`,`natd_imasID`,`natd_businessChannelID`,`natd_toLocID`,`natd_fromLocID`,`nat_postingPeriodID`,`natd_date`) VALUES");
                string bulkInsertQuery = "";
                int count = 0;
                for (int i = 0; i < list.Count; i++)
                {
                    count++;
                    query = query + String.Format("('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}'),", list[i].subsidiaryID, list[i].ISBN13, list[i].itemInternalID, list[i].quantity, list[i].imasItem, list[i].BusinessChannelID, list[i].toLocationID, list[i].fromLocationID, list[i].postingPeriodID, list[i].date);

                    if (count == 10000)
                    {
                        bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);
                        try
                        {
                            MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                            try
                            {
                                connection.Open();
                            }
                            catch (MySqlException ex)
                            {
                                mySqlError = ex.Message;
                                return false;
                                
                            }

                            //Create Command
                            MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                            //Create a data reader and Execute the command

                            MySqlDataReader dataReader = cmd.ExecuteReader();
                            dataReader.Close();
                            connection.Close();
                        }
                        catch (Exception ex)
                        {
                            mySqlError = ex.Message;
                            return false;
                        }
                        count = 0;
                        query = "";
                    }
                }

                bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);
                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {
                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                    //Create a data reader and Execute the command

                    MySqlDataReader dataReader = cmd.ExecuteReader();
                    dataReader.Close();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                return true;
            }
            public Boolean bulkInsertTransfer(List<stockTransferObject> list)
            {
                string query = "";
                string insertQuery = String.Format(@"INSERT INTO netsuite_transfer (`nat_subsidiary`,`nat_subsidiaryID`,`nat_fromLoc`,`nat_fromLocID`,`nat_toLoc`,`nat_toLocID`,`nat_businessChannel`,`nat_businessChannelID`,`nat_postingPeriod`,`nat_postingPeriodID`,`nat_date`,`nat_Memo`) VALUES");
                foreach (stockTransferObject abc in list)
                {
                    query = query + String.Format("('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}'),", abc.Subsidiary, abc.subsidiaryID, abc.fromLocation, abc.fromLocationID, abc.toLocation, abc.toLocationID, abc.BusinessChannel, abc.BusinessChannelID, abc.PostingPeriod, abc.postingPeriodID, abc.date, abc.Memo);

                }

                string bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);
                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {
                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                    //Create a data reader and Execute the command

                    MySqlDataReader dataReader = cmd.ExecuteReader();
                    dataReader.Close();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                return true;
            }
            public Boolean deleteAllTransfer()
            {

                //string query = String.Format(@"INSERT INTO netsuite_adjustmentitem (`nai_nsitemInternalID`,`nai_adjustmentID`,`nai_itemID`,`nai_nsLocation`,`nai_nsLocationID`,`nai_nsSubsidiaryID`,`nai_subsidiary`,`nai_nsbusinessChannelID`,`nai_businessChannel`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')", abc.itemInternalID, x, abc.imasItem, abc.nsLocation, abc.locationID, abc.subsidiaryID, abc.Subsidiary, abc.BusinessChannelID, abc.BusinessChannel);
                string query = "delete from sde.netsuite_transfer";
                string query2 = "delete from sde.netsuite_transferdetail";

                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {
                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(query, connection);
                    //Create a data reader and Execute the command
                    MySqlDataReader dataReader = cmd.ExecuteReader();
                    dataReader.Close();

                    MySqlCommand cmd2 = new MySqlCommand(query2, connection);
                    MySqlDataReader dataReader2 = cmd2.ExecuteReader();
                    dataReader2.Close();

                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                return true;
            }
            public Boolean bulkInsertAdjustmentDetail(List<shipmentObject> list)
            {
                string query = "";
                string insertQuery = String.Format(@"INSERT INTO netsuite_adjustmentdetail2 (`nad_shipmentNo`,`nad_ISBN13`,`nad_ISBN10`,`nad_receivedQty`,`nad_usdCost`,`nad_localCost`,`nad_totalAmt`,`nad_imasItemID`,`nad_imasLocation`,`nad_nsLocation`,`nad_nsLocationID`,`nad_nsItemID`,`nad_firstShipmentDate`,`nad_type`,`nad_businessChannel`,`nad_postingPeriodID`) VALUES");
                string bulkInsertQuery = "";
                int count = 0;
                foreach (shipmentObject abc in list)
                {
                    count++;
                    query = query + String.Format("('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}'),", abc.shipmentNo, abc.ISBN13, abc.ISBN10, abc.receivedQty, abc.USDCost, abc.localCost, abc.totalAmt, abc.imasItem, abc.Location, abc.nsLocation, abc.locationID, abc.itemInternalID, abc.firstShipDate.ToString("yyyy-MM-dd HH:mm:ss"), abc.Type, abc.BusinessChannel, abc.postingPeriodID);
                    if (count == 10000)
                    {
                        bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);

                        try
                        {
                            MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                            try
                            {
                                connection.Open();
                            }
                            catch (MySqlException ex)
                            {
                                mySqlError = ex.Message;
                                return false;
                            }

                            //Create Command
                            MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                            //Create a data reader and Execute the command
                            MySqlDataReader dataReader = cmd.ExecuteReader();
                            dataReader.Close();
                            connection.Close();
                        }
                        catch (Exception ex)
                        {
                            mySqlError = ex.Message;
                            return false;
                        }
                        count = 0;
                        query = "";
                    }
                }

                bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);
                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {
                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                    //Create a data reader and Execute the command
                    MySqlDataReader dataReader = cmd.ExecuteReader();
                    dataReader.Close();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }

                return true;
            }
            public Boolean bulkInsertAdjustment(List<shipmentObject> list)
            {
                string query = "";
                string insertQuery = String.Format(@"INSERT INTO netsuite_adjustment2 (`nas_analysisCode`,`nas_shipmentNo`,`nas_shipmentDate`,`nas_firstShipmentDate`,`nas_Type`,`nas_Subsidiary`,`nas_subsidiaryID`,`nas_businessChannel`,`nas_businessChannelID`,`nas_accountNo`,`nas_postingPeriod`,`nas_postingPeriodID`,`nas_locationID`) VALUES");
                foreach (shipmentObject abc in list)
                {
                    query = query + String.Format("('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}'),", abc.analysisCode, abc.shipmentNo.Trim(), abc.shipmentDate.ToString("yyyy-MM-dd"), abc.firstShipDate.ToString("yyyy-MM-dd HH:mm:ss"), @abc.Type, abc.Subsidiary, abc.subsidiaryID, abc.BusinessChannel, abc.BusinessChannelID, abc.accountNo, abc.postingPeriod, abc.postingPeriodID, abc.locationID);
                }

                string bulkInsertQuery;
                try
                {
                    bulkInsertQuery = insertQuery + query.Substring(0, query.Length - 1);
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {
                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(bulkInsertQuery, connection);
                    //Create a data reader and Execute the command
                    MySqlDataReader dataReader = cmd.ExecuteReader();
                    dataReader.Close();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                return true;
            }
            public Boolean deleteAll()
            {

                //int x = getMaxID();
                //string query = String.Format(@"INSERT INTO netsuite_adjustmentitem (`nai_nsitemInternalID`,`nai_adjustmentID`,`nai_itemID`,`nai_nsLocation`,`nai_nsLocationID`,`nai_nsSubsidiaryID`,`nai_subsidiary`,`nai_nsbusinessChannelID`,`nai_businessChannel`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')", abc.itemInternalID, x, abc.imasItem, abc.nsLocation, abc.locationID, abc.subsidiaryID, abc.Subsidiary, abc.BusinessChannelID, abc.BusinessChannel);
                string query = "delete  from sde.netsuite_adjustment2";
                string query2 = "delete  from sde.netsuite_adjustmentdetail2";
                try
                {
                    MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
                    try
                    {

                        connection.Open();
                    }
                    catch (MySqlException ex)
                    {
                        mySqlError = ex.Message;
                        return false;
                    }

                    //Create Command
                    MySqlCommand cmd = new MySqlCommand(query, connection);

                    //Create a data reader and Execute the command
                    MySqlDataReader dataReader = cmd.ExecuteReader();

                    dataReader.Close();
                    MySqlCommand cmd2 = new MySqlCommand(query2, connection);
                    MySqlDataReader dataReader2 = cmd2.ExecuteReader();
                    dataReader2.Close();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    mySqlError = ex.Message;
                    return false;
                }
                return true;
            }
            //public Boolean CheckrowCount(int adjustmentcount, int adjustmentdetailcount)
            //{
            //    int detailcheck, adjustmentcheck;
            //    string query = String.Format("Select count(*) from netsuite_adjustment2");
            //    string query2 = String.Format("Select count(*) from netsuite_adjustmentdetail2");

            //    try
            //    {
            //        MySqlConnection connection = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["mysql2"].ToString());
            //        try
            //        {
            //            connection.Open();
            //        }
            //        catch (MySqlException ex)
            //        {
            //            mySqlError = ex.Message;
            //            return false;
            //        }

            //        //Create Command
            //        MySqlCommand cmd = new MySqlCommand(query, connection);
            //        //Create a data reader and Execute the command
            //        adjustmentcheck = Int32.Parse(cmd.ExecuteScalar().ToString());

            //        Console.WriteLine("netsuiteadjustment2 row count : " + adjustmentcheck.ToString());
            //        if (adjustmentcheck != adjustmentcount)
            //        {
            //            //Console.WriteLine("Grouping count not tally. please check your csv file and database");
            //            return false;
            //        }

            //        cmd = new MySqlCommand(query2, connection);
            //        //Create a data reader and Execute the command
            //        detailcheck = Int32.Parse(cmd.ExecuteScalar().ToString());
            //        Console.WriteLine("netsuiteadjustmentdetail2 row count : " + detailcheck.ToString());
            //        if (detailcheck != adjustmentdetailcount)
            //        {
            //            //Console.WriteLine("Total count not tally. please check your csv file and database");
            //            return false;
            //        }
            //        //dataReader.Close();
            //        connection.Close();
            //        return true;
            //    }
            //    catch (Exception ex)
            //    {
            //        mySqlError = ex.Message;
            //        return false;
            //    }

               
            //}

        }

        #region ProgressBar

        delegate string ProcessTask(string id);
        linq_adjustment NetsuiteAdj = new linq_adjustment();

        ProcessTask task;
        public void StartNetsuiteAdjProcess(string id)
        {
            NetsuiteAdj.Add(id);
            ProcessTask processTask = new ProcessTask(NetsuiteAdj.NetsuiteAdjustment);
            processTask.BeginInvoke(id, new AsyncCallback(EndNetsuiteAdjgProcess), processTask);
        }

        public void EndNetsuiteAdjgProcess(IAsyncResult result)
        {
            ProcessTask processTask = (ProcessTask)result.AsyncState;
            string id = processTask.EndInvoke(result);
            NetsuiteAdj.Remove(id);
        }

        public ActionResult GetCurrentProgress(string id)
        {
            this.ControllerContext.HttpContext.Response.AddHeader("cache-control", "no-cache");
            object[] currentProgress = NetsuiteAdj.GetStatus(id);
            return Json(currentProgress, JsonRequestBehavior.AllowGet);
        }


        public ActionResult StopCurrentProgress(string id)
        {
            NetsuiteAdj.Cancel(id);
            return RedirectToAction("Adjustment");
            
        }

        #endregion 

        #region ProgressBarStockTransfer

        delegate string STProcessTask(string id);
        linq_stocktransfer nsStockTransfer = new linq_stocktransfer();

        public void StartNetsuiteSTProcess(string id)
        {
            nsStockTransfer.Add(id);
            STProcessTask processTask = new STProcessTask(nsStockTransfer.NetsuiteStockTransfer);
            processTask.BeginInvoke(id, new AsyncCallback(EndNetsuiteSTProcess), processTask);
        }

        public void EndNetsuiteSTProcess(IAsyncResult result)
        {
            STProcessTask processTask = (STProcessTask)result.AsyncState;
            string id = processTask.EndInvoke(result);
            nsStockTransfer.Remove(id);
        }

        public ActionResult GetSTCurrentProgress(string id)
        {
            this.ControllerContext.HttpContext.Response.AddHeader("cache-control", "no-cache");
            object[] currentProgress = nsStockTransfer.GetStatus(id);
            return Json(currentProgress, JsonRequestBehavior.AllowGet);
        }


        public ActionResult StopSTCurrentProgress(string id)
        {
            nsStockTransfer.Cancel(id);
            return RedirectToAction("stocktransfer");

        }
        #endregion
    }

}
