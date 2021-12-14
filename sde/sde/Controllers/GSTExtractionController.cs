using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Net;
using sde.Models;
using log4net;
using sde.comNetsuiteServices;
using sde.WCF;
using System.Transactions;
using System.IO;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Configuration;
using System.Data.Odbc;     

namespace sde.Controllers
{
    public class GSTExtractionController : Controller
    {
        private readonly ILog GSTExtractionLog = LogManager.GetLogger("");   

        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult GSTDataExtract()
        { 
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_ISAAC;
            oCon.Open();
            OdbcCommand cmd = oCon.CreateCommand();

            List<cls_gstDS> gstDSDataList = new List<cls_gstDS>();
            gstDSList DSEntityList = new gstDSList();

            List<cls_gstBC> gstBCDataList = new List<cls_gstBC>();
            gstBCList BCEntityList = new gstBCList();

            List<cls_gstBCPeriodTo> gstBCPeriodToList = new List<cls_gstBCPeriodTo>();
            gstBCListPeriodTo BCPeriodToList = new gstBCListPeriodTo();

            List<cls_gstDSPeriodTo> gstDSPeriodToList = new List<cls_gstDSPeriodTo>();
            gstDSListPeriodTo DSPeriodToList = new gstDSListPeriodTo();


            List<cls_gstBCPeriodTo> listBCCurrWeek = new List<cls_gstBCPeriodTo>();
            String currDate = convertDateToString(DateTime.Now.Date);
            Int32 BCLastWeek = 0;
            Int32 BCLastYear = 0;
            Int32 BCCounter = 1;
            Int32 BCDiffCounter = 0;
            Int32 BCMaxWeek = 0;
            Int32 DSLastWeek = 0;
            Int32 DSLastYear = 0;
            Int32 DSCounter = 1;
            Int32 DSDiffCounter = 0;
            Int32 week = 0;
            Int32 year = 0;
            string DSYearWeek = "";
            string BCYearWeek = "";
            try
            {
                #region Get Direct Sales Entities 
                cmd.CommandText = "SELECT gc_entity,gc_lastmodifieddate,gc_fromPeriod,gc_toPeriod,gc_totRecord,gc_status,gc_totRecordExport,gc_totAmountExport,gc_totGstAmountExport " +
                                  " ,gc_exportfoldername,gc_exportfilename,gc_description " +
                                  " FROM gstextract_ctrl WHERE gc_entity LIKE 'ds%' AND gc_isUserVisible = 'Y' ORDER BY gc_fromPeriod,gc_toPeriod,gc_description  ";

                OdbcDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    cls_gstDS gstData = new cls_gstDS();
                    String DSEntity = (reader.GetValue(0) == DBNull.Value) ? String.Empty : reader.GetString(0);
                    String DSPeriodTo = (reader.GetValue(3) == DBNull.Value) ? String.Empty : reader.GetString(3);

                    gstData.dateSubmit = reader.GetDateTime(1);
                    gstData.fromPeriod = (reader.GetValue(2) == DBNull.Value) ? String.Empty : reader.GetString(2);
                    gstData.toPeriod = DSPeriodTo;
                    gstData.totalRowsInput = reader.GetInt32(4);
                    gstData.status = (reader.GetValue(5) == DBNull.Value) ? String.Empty : reader.GetString(5);
                    gstData.totalRowsExport = reader.GetInt32(6);
                    gstData.totalAmountExport = reader.GetDecimal(7);
                    gstData.totalGstAmountExport = reader.GetDecimal(8);
                    gstData.exportFolderPath = (reader.GetValue(9) == DBNull.Value) ? String.Empty : reader.GetString(9);
                    gstData.exportFileName = (reader.GetValue(10) == DBNull.Value) ? String.Empty : reader.GetString(10);
                    gstData.entity = (reader.GetValue(11) == DBNull.Value) ? String.Empty : reader.GetString(11);
                    
                    if (DSEntity == "ds_customer")
                    {
                        DSLastWeek = Convert.ToInt32(DSPeriodTo.Substring(4, 2));
                        DSLastYear = Convert.ToInt32(DSPeriodTo.Substring(0, 4));
                        DSYearWeek = DSPeriodTo.Substring(0, 4) + DSPeriodTo.Substring(4, 2);
                    }
                    gstDSDataList.Add(gstData);
                }
                DSEntityList.gstDSEntity = gstDSDataList;
                reader.Close();
                reader.Dispose();
                #endregion

                #region Get Book Clubs Entites 
                cmd.CommandText = "SELECT gc_entity,gc_lastmodifieddate,gc_fromPeriod,gc_toPeriod,gc_totRecord,gc_status,gc_totRecordExport,gc_totAmountExport,gc_totGstAmountExport " +
                                  " ,gc_exportfoldername,gc_exportfilename,gc_description " +
                                  " FROM gstextract_ctrl WHERE gc_entity LIKE 'bc%'  AND gc_isUserVisible = 'Y' ORDER BY gc_fromPeriod,gc_toPeriod,gc_description ";

                OdbcDataReader readerBC = cmd.ExecuteReader();
                while (readerBC.Read())
                {
                    cls_gstBC gstData = new cls_gstBC();
                    String BCEntity = (readerBC.GetValue(0) == DBNull.Value) ? String.Empty : readerBC.GetString(0);
                    String BCPeriodTo = (readerBC.GetValue(3) == DBNull.Value) ? String.Empty : readerBC.GetString(3);
                    gstData.dateSubmit = readerBC.GetDateTime(1);
                    gstData.fromPeriod = (readerBC.GetValue(2) == DBNull.Value) ? String.Empty : readerBC.GetString(2);
                    gstData.toPeriod = BCPeriodTo;
                    gstData.totalRowsInput = readerBC.GetInt32(4);
                    gstData.status = (readerBC.GetValue(5) == DBNull.Value) ? String.Empty : readerBC.GetString(5);
                    gstData.totalRowsExport = readerBC.GetInt32(6);
                    gstData.totalAmountExport = readerBC.GetDecimal(7);
                    gstData.totalGstAmountExport = readerBC.GetDecimal(8);
                    gstData.exportFolderPath = (readerBC.GetValue(9) == DBNull.Value) ? String.Empty : readerBC.GetString(9);
                    gstData.exportFileName = (readerBC.GetValue(10) == DBNull.Value) ? String.Empty : readerBC.GetString(10);
                    gstData.entity = (readerBC.GetValue(11) == DBNull.Value) ? String.Empty : readerBC.GetString(11);

                    if (BCEntity == "bc_customer")
                    {
                        BCLastWeek = Convert.ToInt32(BCPeriodTo.Substring(2, 2));
                        BCLastYear = Convert.ToInt32(BCPeriodTo.Substring(0, 2));
                        BCYearWeek = "20" + BCPeriodTo.Substring(0, 2) + BCPeriodTo.Substring(2, 2);
                    }

                    gstBCDataList.Add(gstData);
                }

                BCEntityList.gstBCEntity = gstBCDataList;
                readerBC.Close();
                readerBC.Dispose();
                #endregion

                //KANG bug fixed - select invalid periodWeek and grolierWeek from periodDB
                #region Get Book Clubs Period To
                cmd.CommandText = "SELECT SUBSTRING(periodFiscal,4,2) AS periodYear, CONVERT(VARCHAR(2),periodWeek) AS periodWeek, periodMonth FROM periodDB where convert(int, convert(varchar(4), periodYear) + right('00' + convert(varchar(2), periodWeek), 2)) > " + Convert.ToInt32(BCYearWeek) +
                             " AND periodEnd <= '" + currDate + "' order by periodYear, periodWeek";

                OdbcDataReader readerBCPeriodTo = cmd.ExecuteReader();
                while (readerBCPeriodTo.Read())
                {
                    cls_gstBCPeriodTo gstData = new cls_gstBCPeriodTo();
                    String pYear = (readerBCPeriodTo.GetValue(0) == DBNull.Value) ? String.Empty : readerBCPeriodTo.GetString(0);
                    String pWeek = (readerBCPeriodTo.GetValue(1) == DBNull.Value) ? String.Empty : readerBCPeriodTo.GetString(1);

                    gstData.periodFiscal = pYear + pWeek.PadLeft(2, '0');
                    gstData.periodMonth = readerBCPeriodTo.GetInt32(2);
                    gstBCPeriodToList.Add(gstData);
                }
                BCPeriodToList.gstBCEntityPeriodTo = gstBCPeriodToList;
                readerBCPeriodTo.Close();
                readerBCPeriodTo.Dispose();
                #endregion

                #region Get Direct Sales Period To

                cmd.CommandText = "SELECT CONVERT(VARCHAR(4),grolierYear) AS grolierYear,  right('00' + convert(varchar(2), grolierWeek), 2) AS grolierWeek, periodMonth FROM periodDB where convert(int, convert(varchar(4), grolierYear) + right('00' + convert(varchar(2), grolierWeek), 2)) > " + Convert.ToInt32(DSYearWeek) +
                          " AND periodEnd <= '" + currDate + "' ORDER BY grolierYear, grolierWeek";


                OdbcDataReader readerDSPeriodTo = cmd.ExecuteReader();
                while (readerDSPeriodTo.Read())
                {
                    cls_gstDSPeriodTo gstData = new cls_gstDSPeriodTo();
                    String pYear = (readerDSPeriodTo.GetValue(0) == DBNull.Value) ? String.Empty : readerDSPeriodTo.GetString(0);
                    String pWeek = (readerDSPeriodTo.GetValue(1) == DBNull.Value) ? String.Empty : readerDSPeriodTo.GetString(1);

                    gstData.periodFiscal = pYear + pWeek.PadLeft(2, '0');
                    gstData.periodMonth = readerDSPeriodTo.GetInt32(2);
                    gstDSPeriodToList.Add(gstData);
                }
                DSPeriodToList.gstDSEntityPeriodTo = gstDSPeriodToList;
                readerDSPeriodTo.Close();
                readerDSPeriodTo.Dispose();
                #endregion
                //End Kang

                //#region Get Book Clubs Period To
                //cmd.CommandText = "SELECT MAX(periodWeek) AS periodWeek  FROM periodDB WHERE SUBSTRING(periodFiscal,4,2) = '" + Convert.ToString(BCLastYear) + "' ";
                //OdbcDataReader readerMaxWeek = cmd.ExecuteReader();
                //if (readerMaxWeek.HasRows == true)
                //{
                //    BCMaxWeek = readerMaxWeek.GetInt32(0);
                //}
                //readerMaxWeek.Close();
                //readerMaxWeek.Dispose();

                //cmd.CommandText = "SELECT periodWeek,grolierWeek FROM periodDB WHERE periodStart <='" + currDate + "' AND periodEnd >='" + currDate + "' ";

                ////KANG bug fixed - select invalid periodWeek and grolierWeek from periodDB
                ////cmd.CommandText = "SELECT periodWeek,grolierWeek FROM periodDB WHERE periodStart <='" + currDate + "' AND periodEnd >='" + currDate + "' "; 

                //cmd.CommandText = "SELECT periodWeek,grolierWeek FROM periodDB where convert(int, convert(varchar(4), grolierYear) + right('00' + convert(varchar(2), grolierWeek), 2)) > " + Convert.ToInt32(DSYearWeek) +
                //                  " AND periodEnd <= '" + currDate + "' "; 
                //OdbcDataReader readerBCPeriodTo = cmd.ExecuteReader();
                //Int32 periodWeek = 0;
                //Int32 grolierWeek = 0;
                //if (readerBCPeriodTo.HasRows == true)
                //{
                //    periodWeek = readerBCPeriodTo.GetInt32(0);
                //    grolierWeek = readerBCPeriodTo.GetInt32(1);  
                //}
                //readerBCPeriodTo.Close();
                //readerBCPeriodTo.Dispose();
               
                //String cYearNWeek = string.Empty;
                //week = BCLastWeek + 1;
                //year = BCLastYear;

                //if (periodWeek < BCLastWeek)
                //{
                //    BCLastWeek = BCMaxWeek - BCLastWeek;
                //    BCDiffCounter = periodWeek + BCLastWeek;
                //}
                //else
                //{
                //    BCDiffCounter = periodWeek - BCLastWeek;
                //}

                //while (BCCounter < BCDiffCounter)
                //{
                //    Int32 counter = 0;
                //    if (week > BCMaxWeek)
                //    {
                //        week = 1;
                //        year = BCLastYear + 1;
                //    }  
                //    counter = BCDiffCounter - BCCounter;
                //    if (counter <= 1)
                //    {
                //        cYearNWeek = cYearNWeek + "'" + year.ToString() + week.ToString() + "'";
                //    }
                //    else
                //    {
                //        cYearNWeek = cYearNWeek + "'" + year.ToString() + week.ToString() + "',";
                //    }
                //    week++;
                //    BCCounter++;
                //}
                //if (cYearNWeek == string.Empty)
                //{
                //    cYearNWeek = "''";
                //} 
                //cmd.CommandText = "SELECT SUBSTRING(periodFiscal,4,2) AS periodYear, CONVERT(VARCHAR(2),periodWeek) AS periodWeek, periodMonth " +
                //                  "FROM periodDB WHERE SUBSTRING(periodFiscal,4,2) + CONVERT(VARCHAR(2),periodWeek) IN (" + cYearNWeek + ") ";
                //readerBCPeriodTo = cmd.ExecuteReader();
                //while (readerBCPeriodTo.Read())
                //{
                //    cls_gstBCPeriodTo gstData = new cls_gstBCPeriodTo();
                //    String pYear = (readerBCPeriodTo.GetValue(0) == DBNull.Value) ? String.Empty : readerBCPeriodTo.GetString(0);
                //    String pWeek = (readerBCPeriodTo.GetValue(1) == DBNull.Value) ? String.Empty : readerBCPeriodTo.GetString(1);

                //    gstData.periodFiscal = pYear + pWeek.PadLeft(2, '0');
                //    gstData.periodMonth = readerBCPeriodTo.GetInt32(2);
                //    gstBCPeriodToList.Add(gstData); 
                //}
                //BCPeriodToList.gstBCEntityPeriodTo = gstBCPeriodToList;
                //readerBCPeriodTo.Close();
                //readerBCPeriodTo.Dispose();
                //#endregion
                 
                //#region Get Direct Sales Period To 
                //if (grolierWeek < DSLastWeek)
                //{
                //    DSLastWeek = 52 - DSLastWeek;
                //    DSDiffCounter = grolierWeek + DSLastWeek;
                //}
                //else
                //{
                //    DSDiffCounter = grolierWeek - DSLastWeek;
                //}
                //week = DSLastWeek + 1;
                //year = DSLastYear;
                //cYearNWeek = string.Empty;
                //while (DSCounter < DSDiffCounter)
                //{
                //    Int32 counter = 0;  
                //    if (week > 52)
                //    {
                //        week = 1;
                //        year = DSLastYear + 1;
                //    }
                //    counter = DSDiffCounter - DSCounter; 
                //    if (counter <= 1)
                //    {
                //        cYearNWeek = cYearNWeek + "'" + year.ToString() + week.ToString() + "'";
                //    }
                //    else
                //    {
                //        cYearNWeek = cYearNWeek + "'" + year.ToString() + week.ToString() + "',";
                //    }
                //    week++;
                //    DSCounter++;
                //}
                //if (cYearNWeek == string.Empty)
                //{
                //    cYearNWeek = "''";
                //} 
                //cmd.CommandText = "SELECT CONVERT(VARCHAR(4),grolierYear) AS grolierYear, CONVERT(VARCHAR(2),grolierWeek) AS grolierWeek, periodMonth " +
                //                  "FROM periodDB WHERE CONVERT(VARCHAR(4),grolierYear) + CONVERT(VARCHAR(2),grolierWeek) IN (" + cYearNWeek + ") ";
                //OdbcDataReader readerDSPeriodTo = cmd.ExecuteReader();
                //while (readerDSPeriodTo.Read())
                //{
                //    cls_gstDSPeriodTo gstData = new cls_gstDSPeriodTo();
                //    String pYear = (readerDSPeriodTo.GetValue(0) == DBNull.Value) ? String.Empty : readerDSPeriodTo.GetString(0);
                //    String pWeek = (readerDSPeriodTo.GetValue(1) == DBNull.Value) ? String.Empty : readerDSPeriodTo.GetString(1);

                //    gstData.periodFiscal = pYear + pWeek.PadLeft(2, '0');
                //    gstData.periodMonth = readerDSPeriodTo.GetInt32(2);
                //    gstDSPeriodToList.Add(gstData); 
                //} 
                //DSPeriodToList.gstDSEntityPeriodTo = gstDSPeriodToList;
                //readerDSPeriodTo.Close();
                //readerDSPeriodTo.Dispose(); 
                //#endregion
            }
            catch (Exception ex)
            {
                this.GSTExtractionLog.Error(ex.ToString());
            }
            oCon.Close();

            return View(new Tuple<sde.Models.gstDSList, sde.Models.gstBCList, sde.Models.gstDSListPeriodTo, sde.Models.gstBCListPeriodTo>
                                (DSEntityList, BCEntityList, DSPeriodToList,BCPeriodToList));
            
        }

        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult DSSubmit(String fromPeriod, String toPeriod)
        {
            Boolean isRunning = false;
            Boolean isValid = true;
            object[] rtnResult = new object[1]; 
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_ISAAC))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    if (String.IsNullOrEmpty(toPeriod) || toPeriod == "null")
                    {
                        isValid = false;
                        rtnResult[0] = "EMPTY";
                    }
                    if (isValid == true)
                    {
                        #region Checking is there any process on going
                        command.CommandText = "SELECT DISTINCT gc_status FROM gstextract_ctrl " +
                                              "WHERE gc_entity LIKE 'ds%' ";
                        OdbcDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            String status = (reader.GetValue(0) == DBNull.Value) ? String.Empty : reader.GetString(0);
                            if (status == "START" || status == "PENDING EXPORT" || status == "FINISHED")
                            {
                                isRunning = true;
                                rtnResult[0] = "STARTED";
                                break;
                            }

                        }
                        reader.Close();
                        reader.Dispose();
                        #endregion

                        #region  Update all DS Entity to START
                        if (isRunning == false)
                        {
                            command.CommandText = "UPDATE gstextract_ctrl SET gc_status = 'START',gc_totRecord = 0,gc_remark = 'START', " +
                                                  "gc_totRecordExport = 0,gc_totAmountExport = 0,gc_totGstAmountExport = 0, " +
                                                  "gc_fromPeriod = '" + fromPeriod + "',gc_toPeriod = '" + toPeriod + "',gc_exportfilename =''  " +
                                                  "WHERE gc_entity LIKE 'ds%' AND gc_entity <> 'ds_sales_adjustment' ";

                            int updResult = command.ExecuteNonQuery();
                            rtnResult[0] = "SUCCESS";
                        }
                        #endregion
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    this.GSTExtractionLog.Error(ex.ToString());
                    transaction.Rollback();
                }
            }
            return Json(rtnResult, JsonRequestBehavior.AllowGet); 
        }

        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult BCSubmit(String fromPeriod, String toPeriod)
        {
            Boolean isRunning = false;
            Boolean isValid = true;
            object[] rtnResult = new object[1];
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_ISAAC))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    command.Connection = oCon;
                    command.Transaction = transaction;

                    if (String.IsNullOrEmpty(toPeriod) || toPeriod == "null")
                    {
                        isValid = false;
                        rtnResult[0] = "EMPTY";
                    }
                    if (isValid == true)
                    {
                        #region Checking is there any process on going
                        command.CommandText = "SELECT DISTINCT gc_status FROM gstextract_ctrl " +
                                              "WHERE gc_entity LIKE 'bc%' ";
                        OdbcDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            String status = (reader.GetValue(0) == DBNull.Value) ? String.Empty : reader.GetString(0);
                            if (status == "START" || status == "PENDING EXPORT" || status == "FINISHED")
                            {
                                isRunning = true;
                                rtnResult[0] = "STARTED";
                                break;
                            }
                        }
                        reader.Close();
                        reader.Dispose();
                        #endregion
                         
                        #region  Update all BC Entity to START
                        if (isRunning == false)
                        {
                            command.CommandText = "UPDATE gstextract_ctrl SET gc_status = 'START',gc_totRecord = 0,gc_remark = 'START', " +
                                                  "gc_totRecordExport = 0,gc_totAmountExport = 0,gc_totGstAmountExport = 0, " +
                                                  "gc_fromPeriod = '" + fromPeriod + "',gc_toPeriod = '" + toPeriod + "',gc_exportfilename =''  " + 
                                                  "WHERE gc_entity LIKE 'bc%' ";

                            int updResult = command.ExecuteNonQuery();
                            rtnResult[0] = "SUCCESS";
                        }
                        #endregion
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    this.GSTExtractionLog.Error(ex.ToString());
                    transaction.Rollback();
                }
            }
            return Json(rtnResult, JsonRequestBehavior.AllowGet);
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
                this.GSTExtractionLog.Error(ex.ToString());
            }
            return convertedDate;
        }

        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult GSTDataExtractHistoryBC()
        {
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_ISAAC;
            oCon.Open();
            OdbcCommand cmd = oCon.CreateCommand();
              
            List<cls_gstBC> gstBCDataList = new List<cls_gstBC>();
            gstBCList BCEntityList = new gstBCList();

            try
            { 

                #region Get Book Clubs Entites
                cmd.CommandText = "SELECT TOP 50 a.gc_entity,a.gc_status,a.gc_fromPeriod,a.gc_toPeriod,MAX(a.gc_lastmodifieddate) AS gc_lastmodifieddate, " +
                                  " MAX(a.gc_totRecordExport) AS gc_totRecordExport,MAX(a.gc_totAmountExport) AS gc_totAmountExport, " +
                                  " MAX(a.gc_totGstAmountExport) AS gc_totGstAmountExport,MAX(a.gc_exportfilename) AS gc_exportfilename,b.gc_description, " +
                                  " MAX(a.gc_exportfoldername) AS gc_exportfoldername " +
                                  " FROM gstextract_ctrl_history a join gstextract_ctrl b ON a.gc_entity = b.gc_entity " +
                                  " WHERE  a.gc_entity LIKE 'bc%' AND a.gc_status = 'FINISHED' AND b.gc_isUserVisible = 'Y' " +
                                  " GROUP BY a.gc_entity,a.gc_status,a.gc_fromPeriod,a.gc_toPeriod,b.gc_description " +
                                  " ORDER BY a.gc_fromPeriod DESC,a.gc_toPeriod DESC,b.gc_description ASC ";

                OdbcDataReader readerBC = cmd.ExecuteReader();
                while (readerBC.Read())
                {
                    cls_gstBC gstData = new cls_gstBC();

                    gstData.status = (readerBC.GetValue(1) == DBNull.Value) ? String.Empty : readerBC.GetString(1);
                    gstData.fromPeriod = (readerBC.GetValue(2) == DBNull.Value) ? String.Empty : readerBC.GetString(2);
                    gstData.toPeriod = (readerBC.GetValue(3) == DBNull.Value) ? String.Empty : readerBC.GetString(3); 
                    gstData.dateSubmit = readerBC.GetDateTime(4); 
                    gstData.totalRowsExport = readerBC.GetInt32(5);
                    gstData.totalAmountExport = readerBC.GetDecimal(6);
                    gstData.totalGstAmountExport = readerBC.GetDecimal(7);
                    gstData.exportFileName = (readerBC.GetValue(8) == DBNull.Value) ? String.Empty : readerBC.GetString(8);
                    gstData.entity = (readerBC.GetValue(9) == DBNull.Value) ? String.Empty : readerBC.GetString(9);
                    gstData.exportFolderPath = (readerBC.GetValue(10) == DBNull.Value) ? String.Empty : readerBC.GetString(10);

                    gstBCDataList.Add(gstData);
                }

                BCEntityList.gstBCEntity = gstBCDataList;
                readerBC.Close();
                readerBC.Dispose();
                #endregion

            }
            catch (Exception ex)
            {
                this.GSTExtractionLog.Error(ex.ToString());
            }
            oCon.Close();

            return View(new Tuple< sde.Models.gstBCList>
                                (BCEntityList));

        }
          
        [Authorize(Roles = "GSTEXTRACTION")]
        [HttpPost, ActionName("BCHistoryResend")]
        public void BCHistoryResend(IEnumerable<String> resendList)
        {   
            List<cls_gstBC> gstBCDataList = new List<cls_gstBC>();
            gstBCList BCEntityList = new gstBCList();
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_ISAAC))
            {
                OdbcCommand cmd = new OdbcCommand();
                OdbcTransaction transaction = null;
                try
                { 
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    cmd.Connection = oCon;
                    cmd.Transaction = transaction;
                     
                    foreach (string resend_job in resendList)
                    {
                        string qInsert = string.Empty;
                        string[] array_resend_job = resend_job.Split(';');

                        if (array_resend_job.Count() >= 5)
                        {
                            string entity = array_resend_job[0].ToString();
                            string fromPeriod = array_resend_job[1].ToString();
                            string toPeriod = array_resend_job[2].ToString();
                            string exportFilName = array_resend_job[3].ToString();
                            string exportFolderName = array_resend_job[4].ToString();

                            qInsert = "INSERT INTO gstextract_history_resend (gc_entity,gc_fromPeriod,gc_toPeriod,gc_exportfilename,gc_exportfoldername)  " +
                                      "VALUES ('" + entity + "','" + fromPeriod + "','" + toPeriod + "','" + exportFilName + "','" + exportFolderName + "') ";
                            cmd.CommandText = qInsert;
                            cmd.ExecuteNonQuery();
                        }
                    }  
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    this.GSTExtractionLog.Error(ex.ToString());
                    transaction.Rollback();
                }
            }   
        }
         
        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult GSTDataExtractHistoryDS()
        {
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_ISAAC;
            oCon.Open();
            OdbcCommand cmd = oCon.CreateCommand();

            List<cls_gstDS> gstDSDataList = new List<cls_gstDS>();
            gstDSList DSEntityList = new gstDSList();

            try
            {

                #region Get Direct Sales Entites
                cmd.CommandText = "SELECT TOP 50 a.gc_entity,a.gc_status,a.gc_fromPeriod,a.gc_toPeriod,MAX(a.gc_lastmodifieddate) AS gc_lastmodifieddate, " +
                                  " MAX(a.gc_totRecordExport) AS gc_totRecordExport,MAX(a.gc_totAmountExport) AS gc_totAmountExport, " +
                                  " MAX(a.gc_totGstAmountExport) AS gc_totGstAmountExport,MAX(a.gc_exportfilename) AS gc_exportfilename,b.gc_description, " +
                                  " MAX(a.gc_exportfoldername) AS gc_exportfoldername " +
                                  " FROM gstextract_ctrl_history a join gstextract_ctrl b ON a.gc_entity = b.gc_entity " +
                                  " WHERE  a.gc_entity LIKE 'ds%' AND a.gc_status = 'FINISHED' AND b.gc_isUserVisible = 'Y' " +
                                  " GROUP BY a.gc_entity,a.gc_status,a.gc_fromPeriod,a.gc_toPeriod,b.gc_description " +
                                  " ORDER BY a.gc_fromPeriod DESC,a.gc_toPeriod DESC,b.gc_description ASC ";

                OdbcDataReader readerDS = cmd.ExecuteReader();
                while (readerDS.Read())
                {
                    cls_gstDS gstData = new cls_gstDS();

                    gstData.status = (readerDS.GetValue(1) == DBNull.Value) ? String.Empty : readerDS.GetString(1);
                    gstData.fromPeriod = (readerDS.GetValue(2) == DBNull.Value) ? String.Empty : readerDS.GetString(2);
                    gstData.toPeriod = (readerDS.GetValue(3) == DBNull.Value) ? String.Empty : readerDS.GetString(3);
                    gstData.dateSubmit = readerDS.GetDateTime(4);
                    gstData.totalRowsExport = readerDS.GetInt32(5);
                    gstData.totalAmountExport = readerDS.GetDecimal(6);
                    gstData.totalGstAmountExport = readerDS.GetDecimal(7);
                    gstData.exportFileName = (readerDS.GetValue(8) == DBNull.Value) ? String.Empty : readerDS.GetString(8);
                    gstData.entity = (readerDS.GetValue(9) == DBNull.Value) ? String.Empty : readerDS.GetString(9);
                    gstData.exportFolderPath = (readerDS.GetValue(10) == DBNull.Value) ? String.Empty : readerDS.GetString(10);

                    gstDSDataList.Add(gstData);
                }

                DSEntityList.gstDSEntity = gstDSDataList;
                readerDS.Close();
                readerDS.Dispose();
                #endregion

            }
            catch (Exception ex)
            {
                this.GSTExtractionLog.Error(ex.ToString());
            }
            oCon.Close();

            return View(new Tuple<sde.Models.gstDSList>
                                (DSEntityList));

        }

        [Authorize(Roles = "GSTEXTRACTION")]
        [HttpPost, ActionName("DSHistoryResend")]
        public void DSHistoryResend(IEnumerable<String> resendList)
        {
            List<cls_gstDS> gstDSDataList = new List<cls_gstDS>();
            gstDSList DSEntityList = new gstDSList();
            using (OdbcConnection oCon = new OdbcConnection(@sde.Resource.CONNECTIONSTRING_ISAAC))
            {
                OdbcCommand cmd = new OdbcCommand();
                OdbcTransaction transaction = null;
                try
                {
                    oCon.Open();
                    transaction = oCon.BeginTransaction();
                    cmd.Connection = oCon;
                    cmd.Transaction = transaction;

                    foreach (string resend_job in resendList)
                    {
                        string qInsert = string.Empty;
                        string[] array_resend_job = resend_job.Split(';');

                        if (array_resend_job.Count() >= 5)
                        {
                            string entity = array_resend_job[0].ToString();
                            string fromPeriod = array_resend_job[1].ToString();
                            string toPeriod = array_resend_job[2].ToString();
                            string exportFileName = array_resend_job[3].ToString();
                            string exportFolderName = array_resend_job[4].ToString();

                            qInsert = "INSERT INTO gstextract_history_resend (gc_entity,gc_fromPeriod,gc_toPeriod,gc_exportfilename,gc_exportfoldername)  " +
                                      "VALUES ('" + entity + "','" + fromPeriod + "','" + toPeriod + "','" + exportFileName + "','" + exportFolderName + "') ";
                            cmd.CommandText = qInsert;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    this.GSTExtractionLog.Error(ex.ToString());
                    transaction.Rollback();
                }
            }
        }


        /*
        [Authorize(Roles = "GSTEXTRACTION")]
        public ActionResult DSCustomerHistory(String dsPeriodFrom = "",String dsPeriodTo = "")
        {
            OdbcConnection oCon = new OdbcConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_ISAAC;
            oCon.Open();
            OdbcCommand cmd = oCon.CreateCommand();

            List<cls_DSCustomer> DSCustomerList = new List<cls_DSCustomer>();
            DSCustomerList DSCustomerEntity = new DSCustomerList();

            List<cls_gstDSPeriodTo> gstDSPeriodFromList = new List<cls_gstDSPeriodTo>();
            gstDSListPeriodTo DSPeriodFromList = new gstDSListPeriodTo();
             
            List<cls_gstDSPeriodTo> gstDSPeriodToList = new List<cls_gstDSPeriodTo>();
            gstDSListPeriodTo DSPeriodToList = new gstDSListPeriodTo();

            string currDate = convertDateToString(DateTime.Now.Date);
            string cmdText = string.Empty;
            try
            {
                #region Get Direct Sales Entities
                cmd.CommandText = "SELECT dsc_acc_code,dsc_name,dsc_bill_address1,dsc_bill_address2,dsc_bill_city,dsc_bill_postcode,dsc_bill_state,dsc_telephone1 " +
                                  " ,dsc_contract_year+dsc_contract_week AS contractPeriod,dsc_email1,dsc_description " +
                                  " FROM ds_customer WHERE dsc_contract_year + dsc_contract_week BETWEEN '" + dsPeriodFrom + "' AND '" + dsPeriodTo + "' ";

                OdbcDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    cls_DSCustomer custData = new cls_DSCustomer();
                    custData.accCode = (reader.GetValue(0) == DBNull.Value) ? String.Empty : reader.GetString(0);
                    custData.custName = (reader.GetValue(1) == DBNull.Value) ? String.Empty : reader.GetString(1);
                    custData.billAddress1 = (reader.GetValue(2) == DBNull.Value) ? String.Empty : reader.GetString(2);
                    custData.billAddress2 = (reader.GetValue(3) == DBNull.Value) ? String.Empty : reader.GetString(3);
                    custData.billCity = (reader.GetValue(4) == DBNull.Value) ? String.Empty : reader.GetString(4);
                    custData.billPostcode = (reader.GetValue(5) == DBNull.Value) ? String.Empty : reader.GetString(5);
                    custData.billState = (reader.GetValue(6) == DBNull.Value) ? String.Empty : reader.GetString(6);
                    custData.telephone = (reader.GetValue(7) == DBNull.Value) ? String.Empty : reader.GetString(7);
                    custData.contractPeriod = (reader.GetValue(8) == DBNull.Value) ? String.Empty : reader.GetString(8);
                    custData.email = (reader.GetValue(9) == DBNull.Value) ? String.Empty : reader.GetString(9);
                    custData.description = (reader.GetValue(10) == DBNull.Value) ? String.Empty : reader.GetString(10);

                    DSCustomerList.Add(custData);
                }
                DSCustomerEntity.DSCustomerEntity = DSCustomerList;
                reader.Close();
                reader.Dispose();
                #endregion 
                 
                #region Get Direct Sales Period From
                cmdText = "SELECT dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2) FROM ds_customer "; 
                cmdText = cmdText + " GROUP BY dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2) ORDER BY dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2)"; 
                cmd.CommandText = cmdText;
                
                OdbcDataReader readerDSPeriodFrom = cmd.ExecuteReader();
                while (readerDSPeriodFrom.Read())
                {
                    cls_gstDSPeriodTo periodFrom = new cls_gstDSPeriodTo();  
                    periodFrom.periodFrom = (readerDSPeriodFrom.GetValue(0) == DBNull.Value) ? String.Empty : readerDSPeriodFrom.GetString(0);
                    gstDSPeriodFromList.Add(periodFrom);
                }
                DSPeriodFromList.gstDSEntityPeriodTo = gstDSPeriodFromList;
                readerDSPeriodFrom.Close();
                readerDSPeriodFrom.Dispose(); 
                #endregion
                 
                #region Get Direct Sales Period To
                cmdText = "SELECT dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2) FROM ds_customer ";
                cmdText = cmdText + " WHERE dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2) >= '" + dsPeriodTo + "' ";
                cmdText = cmdText + " GROUP BY dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2) ORDER BY dsc_contract_year + RIGHT('0' + CAST(dsc_contract_week AS VARCHAR(2)),2)";
                cmd.CommandText = cmdText;

                OdbcDataReader readerDSPeriodTo = cmd.ExecuteReader();
                while (readerDSPeriodTo.Read())
                {
                    cls_gstDSPeriodTo periodTo = new cls_gstDSPeriodTo();
                    periodTo.periodFrom = (readerDSPeriodTo.GetValue(0) == DBNull.Value) ? String.Empty : readerDSPeriodTo.GetString(0);
                    gstDSPeriodToList.Add(periodTo);
                }
                DSPeriodToList.gstDSEntityPeriodTo = gstDSPeriodToList;
                readerDSPeriodTo.Close();
                readerDSPeriodTo.Dispose();
                #endregion
            }
            catch (Exception ex)
            {
                this.GSTExtractionLog.Error(ex.ToString());
            }
            oCon.Close();

            return View(new Tuple<sde.Models.DSCustomerList, sde.Models.gstDSListPeriodTo, sde.Models.gstDSListPeriodTo>
                                (DSCustomerEntity, DSPeriodFromList, DSPeriodToList));

        }
        */
    }
}
