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
    public class BackOrderController : Controller
    {
        private static NetSuiteService service = new NetSuiteService();
        private static List<string> jobIDList = new List<string>();
        private static System.Timers.Timer addListTimer;
        private static string jobId;
        private static string mySqlError;

        [HttpGet]
        public ActionResult BackOrderHistory()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var _backOrd = (from n in entities.backorder_cancellation
                                where n.boc_netsuiteProcess != "NEW"
                                && n.boc_netsuiteProcess != "SUBMITTED"
                                select new cls_backorderCancel()
                                {
                                    boc_isbn = n.boc_isbn,
                                    boc_moNo = n.boc_moNo,
                                    boc_quantity = n.boc_quantity,
                                    boc_createdDate = n.boc_createdDate,
                                    boc_sourceFile = n.boc_sourceFile,
                                    boc_insertSequence = n.boc_insertSequence,
                                    boc_netsuiteProcess = n.boc_netsuiteProcess
                                }).OrderBy(x => x.boc_sourceFile).ThenBy(y => y.boc_insertSequence).ToList();


                backorderCancelList rnList = new backorderCancelList();
                rnList._backorderCancelList = _backOrd.ToList();

                return View(new Tuple<sde.Models.backorderCancelList>(rnList));
            }
        }

        [HttpGet]
        public ActionResult BackOrder()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                var _backOrd = (from n in entities.backorder_cancellation
                                        where n.boc_netsuiteProcess != "DONE"
                                        select new cls_backorderCancel()
                                        {
                                            boc_isbn = n.boc_isbn,
                                            boc_moNo = n.boc_moNo,
                                            boc_quantity = n.boc_quantity,
                                            boc_createdDate = n.boc_createdDate,
                                            boc_sourceFile = n.boc_sourceFile,
                                            boc_insertSequence = n.boc_insertSequence,
                                            boc_netsuiteProcess = n.boc_netsuiteProcess
                                        }).OrderBy(x => x.boc_sourceFile).ThenBy(y => y.boc_insertSequence).ToList();


                backorderCancelList rnList = new backorderCancelList();
                rnList._backorderCancelList = _backOrd.ToList();

                return View(new Tuple<sde.Models.backorderCancelList>(rnList));
            }
        }

        [HttpPost]
        public ActionResult DeleteBackOrder(string file)
        {
            object[] rtnResult = new object[1];
            using (sdeEntities entities = new sdeEntities())
            {
                var listToRemove = (from rr in entities.backorder_cancellation
                                    select rr).ToList();
                foreach (var rrRec in listToRemove)
                {
                    entities.backorder_cancellation.Remove(rrRec);
                }
                entities.SaveChanges();

                rtnResult[0] = "SUCCESS";
                return Json(rtnResult, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        public ActionResult SubmitBackOrder(string file)
        {
            object[] rtnResult = new object[1];
            using (sdeEntities entities = new sdeEntities())
            {
                var listToRemove = (from rr in entities.backorder_cancellation
                                    select rr).ToList();
                foreach (var rrRec in listToRemove)
                {
                    rrRec.boc_netsuiteProcess = "SUBMITTED";
                }
                entities.SaveChanges();

                rtnResult[0] = "SUCCESS";
                return Json(rtnResult, JsonRequestBehavior.AllowGet);
            }
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

                /*
                DBCONNECT delete = new DBCONNECT();
                if (delete.deleteAll() == false)
                {
                    return Json(mySqlError);
                }
                */

                List<backOrderCancel> backOrderList = new List<backOrderCancel>();
                for (int i = 2; i < csvData.Rows.Count; i++)
                {
                    try
                    {
                        backOrderCancel obj = new backOrderCancel();
                        obj.isbn = csvData.Rows[i][0].ToString();
                        obj.moNo = csvData.Rows[i][1].ToString();
                        obj.qty = csvData.Rows[i][2].ToString();
                        backOrderList.Add(obj);
                    }
                    catch (Exception ex)
                    {
                        return Json(ex.Message + "at row " + i.ToString());
                    }
                }

                DBCONNECT bulkInsert = new DBCONNECT();
                if (bulkInsert.bulkInsertBackorder(backOrderList, file.FileName) == false)
                {
                    return Json(mySqlError);
                }

                return RedirectToAction("BackOrder", "BackOrder");
                //return Json(file.FileName + " was loaded into db succesfully");
            }
            else
            {
                return Json(file.FileName + " is not a valid file. Please follow the file template format");
            }
        }

        public class backOrderCancel
        {
            public string isbn, moNo, qty;
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

        public class DBCONNECT
        {
            public Boolean bulkInsertBackorder(List<backOrderCancel> list, string _sourceFile = "")
            {
                string query = "";
                string insertQuery = String.Format(@"INSERT INTO backorder_cancellation (`boc_isbn`,`boc_moNo`,`boc_quantity`,`boc_createdDate`,`boc_sourceFile`,`boc_insertSequence`) VALUES");
                string bulkInsertQuery = "";

                int count = 0, insertSeq = 0;
                foreach (backOrderCancel abc in list)
                {
                    count++;
                    insertSeq++;
                    query = query + String.Format("('{0}','{1}','{2}','{3}','{4}','{5}'),", Convert.ToString(abc.isbn).Trim(), Convert.ToString(abc.moNo).Trim(), Convert.ToString(abc.qty).Trim(), DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), _sourceFile, insertSeq);

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
        }

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

    }
}
