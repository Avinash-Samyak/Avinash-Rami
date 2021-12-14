﻿using System;
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
using System.IO;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Configuration;         // #605
using MySql.Data.MySqlClient;       // #605

namespace sde.Controllers
{
    public class DashboardMainController : Controller
    {
        //
        // GET: /DashboardMain/

        public ActionResult Main()
        {
            using (sdeEntities entities = new sdeEntities())
            {
                DateTime fromDate = DateTime.Today;
                DateTime toDate = DateTime.Now;

                DateTime fromDate1 = DateTime.Today.AddDays(-1).AddHours(23);
                DateTime toDate1 = DateTime.Today.AddHours(23);

                //DateTime fromDate2 = DateTime.Today.AddDays(-1).AddHours(10);
                //DateTime toDate2 = DateTime.Today.AddHours(10);
                DateTime fromDate2 = DateTime.Today.AddDays(-1).AddHours(23);
                DateTime toDate2 = DateTime.Today.AddHours(23);

                DateTime fromDateAM = DateTime.Today.AddDays(-1).AddHours(23); // DateTime.Today.AddHours(4);
                DateTime toDateAM = DateTime.Today.AddHours(11);
                DateTime fromDatePM = DateTime.Today.AddHours(11);
                DateTime toDatePM = DateTime.Today.AddHours(23);  //DateTime.Today.AddDays(1).AddHours(4);

                var stockPosting = (from c in entities.cpas_stockposting
                                    join m in entities.map_location
                                    on c.spl_ml_location_internalID equals m.ml_location_internalID
                                    where (c.spl_createdDate > fromDate && c.spl_createdDate <= toDate)
                                    select new
                                    {
                                        transactionType = c.spl_transactionType,
                                        description = c.spl_sDesc,
                                        subsidiary = c.spl_subsidiary
                                    }).ToList();
                var groupQ2 = from p in stockPosting
                              let k = new
                              {
                                  transactionType = p.transactionType,
                                  description = p.description,
                                  subsidiary = p.subsidiary
                              }
                              group p by k into g
                              select new cls_cpasSummaryCount()
                              {
                                  transactionType = g.Key.transactionType,
                                  description = g.Key.description,
                                  subsidiary = g.Key.subsidiary,
                                  numOfTransaction = g.Count()
                              };
                cpasSummaryCountList cpasSCList = new cpasSummaryCountList();
                cpasSCList.cpasSummaryCount = groupQ2.OrderBy(x => x.subsidiary).ToList();

                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                var soSync = from ns in entities.netsuite_syncso
                             join mi in entities.map_item on ns.nt2_itemID equals mi.mi_item_isbn
                             where ns.nt2_rangeTo > fromDateAM && ns.nt2_rangeTo <= toDateAM && ns.nt2_progressStatus != null
                             let k = new
                             {
                                 rangeTo = ns.nt2_rangeTo,
                                 progressStatus = ns.nt2_progressStatus,
                                 moNo = ns.nt2_moNo,
                                 customer = ns.nt2_customer,
                                 addressee = ns.nt2_addressee,
                                 country = ns.nt2_country
                             }
                             group ns by k into g
                             select g;
                var soSyncCount = soSync.Count();
                ViewBag.soSyncCount = soSyncCount;

                var soSyncPM = from ns in entities.netsuite_syncso
                             join mi in entities.map_item on ns.nt2_itemID equals mi.mi_item_isbn
                             where ns.nt2_rangeTo > fromDatePM && ns.nt2_rangeTo <= toDatePM && ns.nt2_progressStatus != null
                             let k = new
                             {
                                 rangeTo = ns.nt2_rangeTo,
                                 progressStatus = ns.nt2_progressStatus,
                                 moNo = ns.nt2_moNo,
                                 customer = ns.nt2_customer,
                                 addressee = ns.nt2_addressee,
                                 country = ns.nt2_country
                             }
                             group ns by k into g
                             select g;
                var soSyncCountPM = soSyncPM.Count();
                ViewBag.soSyncCountPM = soSyncCountPM;

                //ANET-26 - Exception Report: Inbound & outbound Reports Start
                #region SO Not Sync

                var soNotSync1 = (from ns in entities.netsuite_newso
                                  join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                  where ns.nt1_rangeTo > fromDate1 && ns.nt1_rangeTo <= toDate1 &&
                                  ns.nt1_credit_hold == 1 && ns.nt1_status == "PENDING FULFILLMENT"
                                  && ns.nt1_synctowms == "1"
                                  && ns.nt1_moNo.Contains("SO-MY")
                                  select ns.nt1_moNo_internalID).Distinct().ToList();

                var soNotSync2 = (from ns in entities.netsuite_newso
                                  join mi in entities.map_item on ns.nt1_itemID equals mi.mi_item_isbn
                                  where ns.nt1_rangeTo > fromDate1 && ns.nt1_rangeTo <= toDate1 &&
                                  ns.nt1_credit_hold == 0 && ns.nt1_status == "PENDING FULFILLMENT"
                                  && ns.nt1_synctowms == "1" && ns.nt1_moNo.Contains("SO-MY") && ns.nt1_fulfilledQty == 0
                                  && !entities.netsuite_syncso.Any(x => x.nt2_moNo == ns.nt1_moNo && x.nt2_itemID == ns.nt1_itemID)
                                  let k = new
                                  {
                                      rangeTo = ns.nt1_rangeTo,
                                      moNo = ns.nt1_moNo,
                                      customer = ns.nt1_customer,
                                      addressee = ns.nt1_addressee,
                                      country = ns.nt1_country
                                  }
                                  group ns by k into g
                                  select g);

                var soNotSyncCount = soNotSync1.Count() + soNotSync2.Count();
                ViewBag.soNotSyncCount = soNotSyncCount;

                #endregion
                //ANET-26 - Exception Report: Inbound & outbound Reports End

                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


                var poSync = from np in entities.netsuite_pr
                             join npi in entities.netsuite_pritem on np.nspr_pr_ID equals npi.nspi_nspr_pr_ID
                             join mi in entities.map_item on npi.nspi_item_ID equals mi.mi_item_isbn
                             where np.nspr_createdDate > fromDate1 && np.nspr_createdDate <= toDate1
                             let k = new
                             {
                                 pr_ID = np.nspr_pr_ID,
                                 rangeTo = np.nspr_rangeTo,
                                 pr_number = np.nspr_pr_number,
                                 pr_supplier = np.nspr_pr_supplier,
                                 pr_location = np.nspr_pr_location
                             }
                             group np by k into g
                             select g;
                var poSyncCount = poSync.Count();
                ViewBag.poSyncCount = poSyncCount;

                var sorSync = (from nr in entities.netsuite_return
                              join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                              join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                              where nr.nsr_createdDate > fromDate1 && nr.nsr_createdDate <= toDate1
                              select new
                              {
                                  schID = nr.nsr_rr_schID,
                                  number = nr.nsr_rr_number,
                                  createdDate = nr.nsr_createdDate,
                                  invoice = nri.nsri_rritem_invoice
                              }).ToList();
                var groupQ4 = from p in sorSync
                              let k = new
                              {
                                  schID = p.schID,
                                  number = p.number,
                                  createdDate = p.createdDate,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select g;
                var sorSyncCount = groupQ4.Count();
                ViewBag.sorSyncCount = sorSyncCount;
                
                var soFulfillmentAM = from wj in entities.wms_jobordscan
                                    where wj.jos_moNo.Contains("SO-MY")
                                    && wj.jos_rangeTo > fromDateAM && wj.jos_rangeTo <= toDateAM
                                    let k = new
                                    {
                                        job_ID = wj.jos_job_ID,
                                        rangeTo = wj.jos_rangeTo,
                                        exportDate = wj.jos_exportDate,
                                        moNo = wj.jos_moNo,
                                        deliveryRef = wj.jos_deliveryRef
                                    }
                                    group wj by k into g
                                    join bb in entities.netsuite_jobmo on new { JobID = g.Key.job_ID, MoNo = g.Key.moNo }
                                    equals new { JobID = bb.nsjm_nsj_job_ID, MoNo = bb.nsjm_moNo } into gj
                                    from foreignData in gj.DefaultIfEmpty()
                                    select g;
                var soFulfillmentAMCount = soFulfillmentAM.Count();
                ViewBag.soFulfillmentAMCount = soFulfillmentAMCount;

                var soFulfillmentPM = from wj in entities.wms_jobordscan
                                      where wj.jos_moNo.Contains("SO-MY")
                                      && wj.jos_rangeTo > fromDatePM && wj.jos_rangeTo <= toDatePM
                                      let k = new
                                      {
                                          job_ID = wj.jos_job_ID,
                                          rangeTo = wj.jos_rangeTo,
                                          exportDate = wj.jos_exportDate,
                                          moNo = wj.jos_moNo,
                                          deliveryRef = wj.jos_deliveryRef
                                      }
                                      group wj by k into g
                                      join bb in entities.netsuite_jobmo on new { JobID = g.Key.job_ID, MoNo = g.Key.moNo }
                                      equals new { JobID = bb.nsjm_nsj_job_ID, MoNo = bb.nsjm_moNo } into gj
                                      from foreignData in gj.DefaultIfEmpty()
                                      select g;
                var soFulfillmentPMCount = soFulfillmentPM.Count();
                ViewBag.soFulfillmentPMCount = soFulfillmentPMCount;

                // start #605
                var date1 = fromDate2.ToString("yyyy-MM-dd HH:mm:ss");
                var date2 = toDate2.ToString("yyyy-MM-dd HH:mm:ss");
                List<cls_incompleteSOFulfillment> tempIFList = new List<cls_incompleteSOFulfillment>();
                var incSOFulfillment = "SELECT a.nt2_rangeTo, a.nt2_moNo, a.nt2_custID, a.nt2_customer, a.nt2_subsidiary " +
                                        "FROM netsuite_syncso a " +
                                        "inner join netsuite_jobordmaster_pack c " +
                                        "on a.nt2_moNo = c.nsjomp_moNo " +
                                        "and a.nt2_progressStatus like CONCAT ('%', c.nsjomp_job_ID) " +
                                        "and a.nt2_item_internalID = c.nsjomp_item_internalID " +
                                        "where a.nt2_lastfulfilleddate > '" + date1 + "' " +
                                        "and a.nt2_lastfulfilleddate <= '" + date2 + "' " +
                                        "and c.nsjomp_ordQty > a.nt2_wmsfulfilledQty " +
                                        "GROUP BY a.nt2_rangeTo, a.nt2_moNo, a.nt2_custID, a.nt2_customer, a.nt2_subsidiary " +
                                        "ORDER BY a.nt2_rangeTo, a.nt2_moNo";

                string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon = new MySqlConnection(connStr);
                mysqlCon.Open();

                MySqlCommand cmd = new MySqlCommand(incSOFulfillment, mysqlCon);
                MySqlDataReader dtr = cmd.ExecuteReader();

                while (dtr.Read())
                {
                    cls_incompleteSOFulfillment incompleteFulfillment = new cls_incompleteSOFulfillment();
                    incompleteFulfillment.rangeTo = (dtr.GetValue(0) == DBNull.Value) ? DateTime.Now : dtr.GetDateTime(0);
                    incompleteFulfillment.moNo = (dtr.GetValue(1) == DBNull.Value) ? String.Empty : dtr.GetString(1);
                    incompleteFulfillment.custID = (dtr.GetValue(2) == DBNull.Value) ? String.Empty : dtr.GetString(2);
                    incompleteFulfillment.customer = (dtr.GetValue(3) == DBNull.Value) ? String.Empty : dtr.GetString(3);
                    incompleteFulfillment.subsidiary = (dtr.GetValue(4) == DBNull.Value) ? String.Empty : dtr.GetString(4);
                    tempIFList.Add(incompleteFulfillment);
                }
                dtr.Close();
                cmd.Dispose();

                var incompleteSoFulfillmentCount = tempIFList.Count();
                ViewBag.incompleteSoFulfillmentCount = incompleteSoFulfillmentCount;

                // end #605

                //ANET-26 - Exception Report: Inbound & outbound Reports Start 
                #region SO Invoice Generate and SO Invoice not generate

                var soInvoiceGenerate = "select distinct jos_moNo as mono from wms_jobordscan " +
                                        "inner join requestnetsuite_task " +
                                        "on wms_jobordscan.jos_moNo = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt_refNO, '.', 3), '.', -1) " +
                                        "where wms_jobordscan.jos_moNo like '%SO-MY%' and " +
                                        "wms_jobordscan.jos_GMYInvoiceProgress is not null " +
                                        "and requestnetsuite_task.rnt_status = 'TRUE' " +
                                        "and requestnetsuite_task.rnt_description in ('UPD-STATUS.GMY INVOICE', 'UPD-STATUS.GMY INVOICE PM') " +
                                        "and jos_rangeTo > '" + date1 + "' and jos_rangeTo <=  '" + date2 + "' " +
                                        "and rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' ";

                soInvoiceGenerate = soInvoiceGenerate + "union select distinct nd.dsf_dropshipmono as monNo  " +
                                   " from requestnetsuite_task rnt " +
                                   " inner join netsuite_dropshipfulfillment nd on rnt.rnt_jobID = nd.dsf_invProgress " +
                                   " and nd.dsf_dropshipmono = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt.rnt_refNO, '.', 3), '.', -1) " +
                                   " where rnt.rnt_description = 'UPD-STATUS.DROPSHIP INVOICE' and rnt.rnt_status = 'TRUE' " +
                                   " and nd.dsf_invProgress is not null and rnt.rnt_createdDate > '" + date1 + "' and rnt.rnt_createdDate <= '" + date2 + "' " +
                                   " and nd.dsf_invUpdatedDate > '" + date1 + "' and nd.dsf_invUpdatedDate <= '" + date2 + "' ";

                string connStr1 = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon1 = new MySqlConnection(connStr1);
                mysqlCon1.Open();

                MySqlCommand cmd1 = new MySqlCommand(soInvoiceGenerate, mysqlCon1);
                MySqlDataReader dtr1 = cmd1.ExecuteReader();

                int invoicegeneratecount = 0;
                while (dtr1.Read())
                {
                    invoicegeneratecount++;
                }

                dtr1.Close();
                cmd1.Dispose();

                ViewBag.soInvoiceGenerate = invoicegeneratecount;

                var soInvoiceNotGenerate = "select distinct jos_moNo as mono from wms_jobordscan " +
                                          "inner join requestnetsuite_task " +
                                          "on wms_jobordscan.jos_moNo = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt_refNO, '.', 3), '.', -1) " +
                                          "where wms_jobordscan.jos_moNo like '%SO-MY%' and " +
                                          "wms_jobordscan.jos_GMYInvoiceProgress is not null " +
                                          "and requestnetsuite_task.rnt_status not in('TRUE') " +
                                          "and requestnetsuite_task.rnt_description in( 'UPD-STATUS.GMY INVOICE', 'UPD-STATUS.GMY INVOICE PM') " +
                                          "and jos_rangeTo > '" + date1 + "' and jos_rangeTo <=  '" + date2 + "' " +
                                          "and rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' " +
                                          "UNION " +
                                          "select distinct jos_moNo as mono from wms_jobordscan " +
                                          "where wms_jobordscan.jos_moNo like '%SO-MY%' and jos_rangeTo > '" + date1 + "' and jos_rangeTo <= '" + date2 + "' " +
                                          "and jos_GMYInvoiceProgress is null ";

                soInvoiceNotGenerate = soInvoiceNotGenerate + "union select distinct nd.dsf_dropshipmono as monNo " +
                                    " from requestnetsuite_task rnt inner join netsuite_dropshipfulfillment nd on rnt.rnt_jobID = nd.dsf_invProgress " +
                                    " and nd.dsf_dropshipmono = SUBSTRING_INDEX(SUBSTRING_INDEX(rnt.rnt_refNO, '.', 3), '.', -1) " +
                                    " where rnt.rnt_description = 'UPD-STATUS.DROPSHIP INVOICE' and rnt.rnt_status not in ( 'TRUE') " +
                                    " and nd.dsf_invProgress is not null " +
                                    " and rnt.rnt_createdDate > '" + date1 + "' and rnt_createdDate <= '" + date2 + "' ";

                //" and nd.dsf_invUpdatedDate > '" + date1 + "' and nd.dsf_invUpdatedDate <= '" + date2 + "' ";

                string connStr2 = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                MySqlConnection mysqlCon2 = new MySqlConnection(connStr2);
                mysqlCon2.Open();

                MySqlCommand cmd2 = new MySqlCommand(soInvoiceNotGenerate, mysqlCon2);
                MySqlDataReader dtr2 = cmd2.ExecuteReader();

                int invoicenotgeneratecount = 0;
                while (dtr2.Read())
                {
                    invoicenotgeneratecount++;
                }

                dtr2.Close();
                cmd2.Dispose();

                ViewBag.soInvoiceNotGenerate = invoicenotgeneratecount;


                #endregion
                //ANET-26 - Exception Report: Inbound & outbound Reports End

                var poReceiveAM = (from wp in entities.wms_poreceive
                                 where wp.po_rangeTo > fromDateAM && wp.po_rangeTo <= toDateAM
                                 join prdata in entities.netsuite_pr on wp.po_pr_ID equals prdata.nspr_pr_ID
                                 join wpi in entities.wms_poreceiveitem on wp.po_poreceive_ID equals wpi.poi_poreceive_ID
                                 join mi in entities.map_item on wpi.poi_item_ID equals mi.mi_item_isbn
                                 select new
                                  {
                                      rangeTo = wp.po_rangeTo,
                                      prNumber = prdata.nspr_pr_number,
                                      internalID = prdata.nspr_pr_internalID,
                                      supplier = prdata.nspr_pr_supplier,
                                      location = prdata.nspr_pr_location,
                                      number = wp.po_poreceive_number,
                                      invoice = wp.po_poreceive_invoice
                                  }).ToList();
                var groupQ3 = from p in poReceiveAM
                              let k = new
                              {
                                  rangeTo = p.rangeTo,
                                  prNumber = p.prNumber,
                                  internalID = p.internalID,
                                  supplier = p.supplier,
                                  location = p.location,
                                  number = p.number,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select g;
                var poReceiveAMCount = groupQ3.Count();
                ViewBag.poReceiveAMCount = poReceiveAMCount;

                var poReceivePM = (from wp in entities.wms_poreceive
                                 where wp.po_rangeTo > fromDatePM && wp.po_rangeTo <= toDatePM
                                 join prdata in entities.netsuite_pr on wp.po_pr_ID equals prdata.nspr_pr_ID
                                 join wpi in entities.wms_poreceiveitem on wp.po_poreceive_ID equals wpi.poi_poreceive_ID
                                 join mi in entities.map_item on wpi.poi_item_ID equals mi.mi_item_isbn
                                 select new
                                 {
                                     rangeTo = wp.po_rangeTo,
                                     prNumber = prdata.nspr_pr_number,
                                     internalID = prdata.nspr_pr_internalID,
                                     supplier = prdata.nspr_pr_supplier,
                                     location = prdata.nspr_pr_location,
                                     number = wp.po_poreceive_number,
                                     invoice = wp.po_poreceive_invoice
                                 }).ToList();
                var groupQ5 = from p in poReceivePM
                              let k = new
                              {
                                  rangeTo = p.rangeTo,
                                  prNumber = p.prNumber,
                                  internalID = p.internalID,
                                  supplier = p.supplier,
                                  location = p.location,
                                  number = p.number,
                                  invoice = p.invoice
                              }
                              group p by k into g
                              select g;
                var poReceivePMCount = groupQ5.Count();
                ViewBag.poReceivePMCount = poReceivePMCount;

                var sorReceive = from nr in entities.netsuite_return
                                 join nri in entities.netsuite_returnitem on nr.nsr_rr_ID equals nri.nsri_nsr_rr_ID
                                 join mi in entities.map_item on nri.nsri_rritem_isbn equals mi.mi_item_isbn
                                 where nr.nsr_rr_returnDate > fromDate2 && nr.nsr_rr_returnDate <= toDate2
                                 select nr;
                var sorReceiveCount = sorReceive.Count();
                ViewBag.sorReceiveCount = sorReceiveCount;
                
                return View(new Tuple<sde.Models.cpasSummaryCountList>(cpasSCList));
            }
        }

    }
}
