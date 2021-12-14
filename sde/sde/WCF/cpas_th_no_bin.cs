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
using System.Data.Objects.SqlClient;

namespace sde.WCF
{
    public class cpas_th_no_bin
    {
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");
        NetSuiteService service = new NetSuiteService();

        #region Netsuite

        #region NETSUITE PHASE II (TRX WITH AMOUNT)


        public Boolean CPASSalesWithPriceInst(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASSalesWithPriceInst ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var cpasSales = (from c in entities.cpas_dataposting
                                         ////cpng start
                                         //join b in entities.map_bin
                                         //on c.spl_ml_location_internalID equals b.mb_bin_internalID
                                         //join m in entities.map_location
                                         //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                         ////cpng end

                                         //cpng ori start
                                         join m in entities.map_location
                                         on c.spl_ml_location_internalID equals m.ml_location_internalID
                                         //cpng ori end
                                         where (c.spl_createdDate > rangeFrom && c.spl_createdDate <= rangeTo)
                                         && (c.spl_subsidiary == "TH")
                                         && (c.spl_transactionType == "SALES")
                                         && (c.spl_noOfInstallments == "INST")
                                         select new
                                         {
                                             id = c.spl_sp_id,
                                             tranType = c.spl_transactionType,
                                             businessChannel = c.spl_mb_businessChannel_internalID,
                                             postingDate = c.spl_postingDate,
                                             memo = c.spl_sDesc,
                                             location_name = c.spl_sLoc,//cpng
                                             location_id = c.spl_ml_location_internalID,
                                             rlocation_name = m.ml_location_name, //cpng
                                             rlocation_id = m.ml_location_internalID, //cpng
                                             subsidiaryName = c.spl_subsidiary,
                                             subsidiary = c.spl_subsidiary_internalID,
                                             salesType = c.spl_noOfInstallments,
                                             isFirstRun = (c.spl_netsuiteProgress == null || c.spl_netsuiteProgress == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var cpasSalesGroup = (from d in cpasSales
                                              where d.isFirstRun == "Y"
                                              select new
                                              {
                                                  d.id,
                                                  d.tranType,
                                                  d.businessChannel,
                                                  d.postingDate,
                                                  d.memo,
                                                  d.location_name,
                                                  d.location_id,
                                                  d.rlocation_name, //cpng
                                                  d.rlocation_id, //cpng
                                                  d.subsidiaryName,
                                                  d.subsidiary,
                                                  d.salesType
                                              }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASSalesWithPriceInst: " + cpasSalesGroup.Count() + " records to update.");

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

                                #region Main Information
                                switch (con.subsidiary)
                                {
                                    case "3"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY; //last time 143, change to 68 on 27/Mar/2015
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                        so.entity = refEntity;
                                        break;
                                    case "5"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG; //last time 138, change to 68 on 27/Mar/2015
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
                                so.tranDate = Convert.ToDateTime(con.postingDate).AddDays(-1);
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = con.subsidiary;
                                so.subsidiary = refSubsidiary;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = con.businessChannel;
                                so.@class = refClass;

                                so.memo = con.memo + "; " + con.location_name + "; " + con.salesType;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                //7 = Indonesia, 6 = Thailand, 9 = India
                                //if (con.subsidiary == "6" || con.subsidiary == "7" || con.subsidiary == "9")
                                //{
                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = con.rlocation_id; //cpng
                                so.location = refLocationSO;
                                //}

                                refNo = con.id;
                                #endregion

                                #region Trx with Sales Amount
                                var conItem = (from i in entities.cpas_dataposting
                                               where i.spl_sp_id == refNo
                                               && i.spl_ml_location_internalID == con.location_id
                                               && (i.spl_createdDate > rangeFrom && i.spl_createdDate <= rangeTo)
                                               && i.spl_subsidiary == con.subsidiaryName
                                               && i.spl_transactionType == con.tranType
                                               && i.spl_noOfInstallments == con.salesType
                                               select i).ToList();

                                var conItemGroup = from p in conItem
                                                   let k = new
                                                   {
                                                       item_name = p.spl_sPID,
                                                       item = p.spl_mi_item_internalID,
                                                       location = p.spl_ml_location_internalID,
                                                       taxCode = p.spl_taxCode
                                                   }
                                                   group p by k into g
                                                   select new
                                                   {
                                                       item_name = g.Key.item_name,
                                                       item = g.Key.item,
                                                       location = g.Key.location,
                                                       taxCode = g.Key.taxCode,
                                                       qty = g.Sum(p => p.spl_dQty),
                                                       nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                       gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                       UFC = g.Sum(p => p.spl_tolUFC),
                                                       deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                   };

                                SalesOrderItem[] soii = new SalesOrderItem[conItemGroup.Count() + 4];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                double tolUFC = 0;
                                double tolDeliveryCharges = 0;
                                double tolNonInvPrice = 0;
                                double tolNonInvGstAmt = 0;
                                double tolDownPayment = 0;
                                //double tolVAT = 0;
                                int nonInvVar = 4;

                                if (conItemGroup.Count() > 0)
                                {
                                    int itemCount = 0;
                                    foreach (var item in conItemGroup)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();
                                        double tolQty = Convert.ToDouble(item.qty);
                                        string unitPrice = string.Empty;
                                        double tolUnitPrice = Convert.ToDouble(item.nettPrice);
                                        double tolGstAmount = Convert.ToDouble(item.gstAmount);
                                        string taxCodeInternalID = string.Empty;

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refPriceLevel = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        if (item.item_name != "DOWNPAYMENT" && item.item_name != "VAT")
                                        {
                                            #region !DOWNPAYMENT
                                            if (tolQty > 0)
                                            {
                                                //Items
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.item;
                                                soi.item = refItem;

                                                /*
                                                 * form 143 got location in column
                                                */
                                                if (refForm.internalId == "143")
                                                {
                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = con.rlocation_id; //cpng
                                                    soi.location = refLocation;
                                                }

                                                //Price Level
                                                refPriceLevel.internalId = "-1";//Custom
                                                soi.price = refPriceLevel;

                                                //Qty
                                                soi.quantity = Convert.ToDouble(tolQty);
                                                soi.quantitySpecified = true;

                                                //Commit Status
                                                //soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                                //soi.commitInventorySpecified = true;

                                                //Unit Price/Rate
                                                unitPrice = Convert.ToString(Math.Round(tolUnitPrice / tolQty, 2));
                                                soi.rate = unitPrice;

                                                //Total Amount
                                                soi.amount = tolUnitPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //temp
                                                    //if (item.taxCode == "ZRL")
                                                    //{
                                                    //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //else
                                                    //    if (item.taxCode == "ZRE")
                                                    //    {
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        if (tolGstAmount > 0)
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        }
                                                    //        else
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        }
                                                    //    }


                                                    //mohan -21/06/2018 start//
                                                    if (item.taxCode == "VAT 7.0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else if (item.taxCode == "VAT 0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //else
                                                    //{
                                                    //    if (tolGstAmount > 0)
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //    }
                                                    //}
                                                    //mohan -21/06/2018 end//

                                                    //Gst Amount
                                                    soi.tax1Amt = tolGstAmount;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = taxCodeInternalID;
                                                    soi.taxCode = refTaxCode;
                                                }

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            else
                                            {
                                                tolNonInvPrice = tolNonInvPrice + tolUnitPrice;
                                                tolNonInvGstAmt = tolNonInvGstAmt + tolGstAmount;
                                            }
                                            #endregion
                                        }
                                        //else if (item.item_name == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            tolDownPayment = Convert.ToDouble(item.nettPrice);
                                        }

                                        tolUFC = tolUFC + Convert.ToDouble(item.UFC);
                                        tolDeliveryCharges = tolDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                    }

                                    //------------------------------------------------------------------------
                                    //Add UFC, Delivery Charges and Non-Inv 
                                    for (int nonInv = 1; nonInv <= nonInvVar; nonInv++)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        /*
                                         * form 143 got location in column
                                        */
                                        if (refForm.internalId == "143")
                                        {
                                            RecordRef refLocation = new RecordRef();
                                            refLocation.internalId = con.rlocation_id; //cpng
                                            soi.location = refLocation;
                                        }

                                        //Price Level
                                        RecordRef refPriceLevel = new RecordRef();
                                        refPriceLevel.internalId = "-1";//Custom
                                        soi.price = refPriceLevel;

                                        if (nonInv == 1)//UFC
                                        {
                                            #region UFC
                                            if (con.salesType == "INST")//Only Inst sales have UFC
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolUFC);

                                                //Total Amount
                                                //soi.amount = (1*tolUFC);
                                                //soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                   // refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //mohan -21/06/2018 end//
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 2)//Delivery Charges
                                        {
                                            #region Delivery Charges
                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                            soi.item = refItem;

                                            //Qty
                                            soi.quantity = 1;
                                            soi.quantitySpecified = true;

                                            //Unit Price/Rate 
                                            soi.rate = Convert.ToString(tolDeliveryCharges);

                                            //Total Amount
                                            soi.amount = tolDeliveryCharges;
                                            soi.amountSpecified = true;

                                            if (con.subsidiaryName == "TH")
                                            {
                                                //Gst Amount
                                                soi.tax1Amt = 0;
                                                soi.tax1AmtSpecified = true;

                                                //Tax Code
                                                //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                //mohan -21/06/2018 start//
                                                refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                //mohan -21/06/2018 end//

                                                soi.taxCode = refTaxCode;
                                            }
                                            soii[itemCount] = soi;
                                            itemCount++;
                                            #endregion
                                        }
                                        if (nonInv == 3)//Non-inventory
                                        {
                                            #region Non Inv
                                            if (tolNonInvPrice > 0)
                                            {
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_NONINV_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolNonInvPrice);

                                                //Total Amount
                                                soi.amount = tolNonInvPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = tolNonInvGstAmt;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolNonInvGstAmt > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //mohan -21/06/2018 start//
                                                    if (tolNonInvGstAmt > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //mohan -21/06/2018 end//
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 4)//Down Payment
                                        {
                                            #region Down Payment
                                            if (tolDownPayment > 0)
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DOWNPAYMENT_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(-1 * tolDownPayment);

                                                //Total Amount
                                                soi.amount = (-1 * tolDownPayment);
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        //if (nonInv == 5)//VAT
                                        //{
                                        //    #region VAT
                                        //    if (tolVAT > 0)
                                        //    {
                                        //        refItem.type = RecordType.nonInventoryResaleItem;
                                        //        refItem.typeSpecified = true;
                                        //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                        //        soi.item = refItem;

                                        //        //Qty
                                        //        soi.quantity = 1;
                                        //        soi.quantitySpecified = true;

                                        //        //Unit Price/Rate 
                                        //        soi.rate = Convert.ToString(tolVAT);

                                        //        //Total Amount
                                        //        soi.amount = tolVAT;
                                        //        soi.amountSpecified = true;

                                        //        if (con.subsidiaryName == "TH")
                                        //        {
                                        //            //Gst Amount
                                        //            soi.tax1Amt = 0;
                                        //            soi.tax1AmtSpecified = true;

                                        //            //Tax Code
                                        //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                        //            soi.taxCode = refTaxCode;
                                        //        }
                                        //        soii[itemCount] = soi;
                                        //        itemCount++;
                                        //    }
                                        //    #endregion
                                        //}
                                    }

                                    soil.item = soii;
                                    so.itemList = soil;
                                    soList[soCount] = so;
                                    rowCount = soCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + con.id + "'  AND spl_subsidiary_internalID = '" + con.subsidiary + "' " +
                                                      "AND spl_sDesc = '" + con.memo + "' " +
                                                      "AND spl_ml_location_internalID = '" + con.location_id + "'" +
                                                      "AND (spl_transactionType = 'SALES') " +
                                                      "AND spl_noOfInstallments = 'INST' " +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-SALES WITH PRICE INST', 'CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + insertTask2);
                                    entities.Database.ExecuteSqlCommand(insertTask2);

                                    var insSalesTrx = "insert into cpas_salestransaction (cst_refNo, cst_createdDate, cst_soSeqNo, cst_sp_id, cst_sDesc, cst_sLoc, " +
                                        "cst_ml_location_internalID, cst_subsidiary, cst_subsidiary_internalID, cst_postingDate, cst_soJobID, cst_salesType) " +
                                        "values ('CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', '" + con.id + "', " +
                                        "'" + con.memo + "','" + con.location_name + "','" + con.location_id + "','" + con.subsidiaryName + "','" + con.subsidiary + "','" + convertDateToString(Convert.ToDateTime(con.postingDate)) + "'," +
                                        "'" + gjob_id.ToString() + "','" + con.tranType + "-" + con.salesType + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    soCount++;
                                    status = true;
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASSalesWithPriceInst Exception: " + ex.ToString());
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
                                job = service.asyncAddList(soList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES WITH PRICE INST' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var updSalesTrx = "update cpas_salestransaction set cst_soJobID = '" + jobID + "' where cst_soJobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updSalesTrx);
                                entities.Database.ExecuteSqlCommand(updSalesTrx);

                                var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updDataPost);
                                entities.Database.ExecuteSqlCommand(updDataPost);

                                scope1.Complete();
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES WITH PRICE INST' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASSalesWithPriceInst: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean CPASSalesWithPriceCad(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASSalesWithPriceCad ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var cpasSales = (from c in entities.cpas_dataposting
                                         ////cpng start
                                         //join b in entities.map_bin
                                         //on c.spl_ml_location_internalID equals b.mb_bin_internalID
                                         //join m in entities.map_location
                                         //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                         ////cpng end
                                         //cpng ori start
                                         join m in entities.map_location
                                         on c.spl_ml_location_internalID equals m.ml_location_internalID
                                         //cpng ori end
                                         where (c.spl_createdDate > rangeFrom && c.spl_createdDate <= rangeTo)
                                         && (c.spl_subsidiary == "TH")
                                         && (c.spl_transactionType == "SALES")
                                         && (c.spl_noOfInstallments == "CAD")
                                         select new
                                         {
                                             id = c.spl_sp_id,
                                             tranType = c.spl_transactionType,
                                             businessChannel = c.spl_mb_businessChannel_internalID,
                                             postingDate = c.spl_postingDate,
                                             memo = c.spl_sDesc,
                                             location_name = c.spl_sLoc,//cpng
                                             location_id = c.spl_ml_location_internalID,
                                             rlocation_name = m.ml_location_name, //cpng
                                             rlocation_id = m.ml_location_internalID, //cpng
                                             subsidiaryName = c.spl_subsidiary,
                                             subsidiary = c.spl_subsidiary_internalID,
                                             salesType = c.spl_noOfInstallments,
                                             isFirstRun = (c.spl_netsuiteProgress == null || c.spl_netsuiteProgress == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var cpasSalesGroup = (from d in cpasSales
                                              where d.isFirstRun == "Y"
                                              select new
                                              {
                                                  d.id,
                                                  d.tranType,
                                                  d.businessChannel,
                                                  d.postingDate,
                                                  d.memo,
                                                  d.location_name,
                                                  d.location_id,
                                                  d.rlocation_name, //cpng
                                                  d.rlocation_id, //cpng
                                                  d.subsidiaryName,
                                                  d.subsidiary,
                                                  d.salesType
                                              }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASSalesWithPriceCad: " + cpasSalesGroup.Count() + " records to update.");

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
                                #region Main Information
                                switch (con.subsidiary)
                                {
                                    case "3"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY; //last time 143, change to 68 on 27/Mar/2015
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_MY_TRADE;
                                        so.entity = refEntity;
                                        break;
                                    case "5"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG; //last time 138, change to 68 on 27/Mar/2015
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

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_TRADE;
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
                                so.tranDate = Convert.ToDateTime(con.postingDate).AddDays(-1);
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = con.subsidiary;
                                so.subsidiary = refSubsidiary;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = con.businessChannel;
                                so.@class = refClass;

                                so.memo = con.memo + "; " + con.location_name + "; " + con.salesType;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                //7 = Indonesia, 6 = Thailand, 9 = India
                                //if (con.subsidiary == "6" || con.subsidiary == "7" || con.subsidiary == "9")
                                //{
                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = con.rlocation_id;//cpng
                                so.location = refLocationSO;
                                //}

                                refNo = con.id;
                                #endregion

                                #region Trx with Sales Amount
                                var conItem = (from i in entities.cpas_dataposting
                                               where i.spl_sp_id == refNo
                                               && i.spl_ml_location_internalID == con.location_id
                                               && (i.spl_createdDate > rangeFrom && i.spl_createdDate <= rangeTo)
                                               && i.spl_subsidiary == con.subsidiaryName
                                               && i.spl_transactionType == con.tranType
                                               && i.spl_noOfInstallments == con.salesType
                                               select i).ToList();

                                var conItemGroup = from p in conItem
                                                   let k = new
                                                   {
                                                       item_name = p.spl_sPID,
                                                       item = p.spl_mi_item_internalID,
                                                       location = p.spl_ml_location_internalID,
                                                       taxCode = p.spl_taxCode
                                                   }
                                                   group p by k into g
                                                   select new
                                                   {
                                                       item_name = g.Key.item_name,
                                                       item = g.Key.item,
                                                       location = g.Key.location,
                                                       taxCode = g.Key.taxCode,
                                                       qty = g.Sum(p => p.spl_dQty),
                                                       nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                       gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                       UFC = g.Sum(p => p.spl_tolUFC),
                                                       deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                   };

                                SalesOrderItem[] soii = new SalesOrderItem[conItemGroup.Count() + 4];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                double tolUFC = 0;
                                double tolDeliveryCharges = 0;
                                double tolNonInvPrice = 0;
                                double tolNonInvGstAmt = 0;
                                double tolDownPayment = 0;
                                //double tolVAT = 0;
                                int nonInvVar = 4;

                                if (conItemGroup.Count() > 0)
                                {
                                    int itemCount = 0;
                                    foreach (var item in conItemGroup)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();
                                        double tolQty = Convert.ToDouble(item.qty);
                                        string unitPrice = string.Empty;
                                        double tolUnitPrice = Convert.ToDouble(item.nettPrice);
                                        double tolGstAmount = Convert.ToDouble(item.gstAmount);
                                        string taxCodeInternalID = string.Empty;

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refPriceLevel = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        if (item.item_name != "DOWNPAYMENT" && item.item_name != "VAT")
                                        {
                                            if (tolQty > 0)
                                            {
                                                //Items
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.item;
                                                soi.item = refItem;

                                                /*
                                                 * form 143 got location in column
                                                */
                                                if (refForm.internalId == "143")
                                                {
                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = con.rlocation_id;//cpng
                                                    soi.location = refLocation;
                                                }

                                                //Price Level
                                                refPriceLevel.internalId = "-1";//Custom
                                                soi.price = refPriceLevel;

                                                //Qty
                                                soi.quantity = Convert.ToDouble(tolQty);
                                                soi.quantitySpecified = true;

                                                //Commit Status
                                                //soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                                //soi.commitInventorySpecified = true;

                                                //Unit Price/Rate
                                                unitPrice = Convert.ToString(Math.Round(tolUnitPrice / tolQty, 2));
                                                soi.rate = unitPrice;

                                                //Total Amount
                                                soi.amount = tolUnitPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    ////temp
                                                    //if (item.taxCode == "ZRL")
                                                    //{
                                                    //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //else
                                                    //    if (item.taxCode == "ZRE")
                                                    //    {
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        if (tolGstAmount > 0)
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        }
                                                    //        else
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        }
                                                    //    }

                                                    //mohan -21/06/2018 start//
                                                    if (item.taxCode == "VAT 7.0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else if (item.taxCode == "VAT 0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //else
                                                    //{
                                                    //    if (tolGstAmount > 0)
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //    }
                                                    //}
                                                    //mohan -21/06/2018 end//

                                                    //Gst Amount
                                                    soi.tax1Amt = tolGstAmount;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = taxCodeInternalID;
                                                    soi.taxCode = refTaxCode;
                                                }

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            else
                                            {
                                                tolNonInvPrice = tolNonInvPrice + tolUnitPrice;
                                                tolNonInvGstAmt = tolNonInvGstAmt + tolGstAmount;
                                            }
                                        }
                                        //else if (item.item_name == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            tolDownPayment = Convert.ToDouble(item.nettPrice);
                                        }

                                        tolUFC = tolUFC + Convert.ToDouble(item.UFC);
                                        tolDeliveryCharges = tolDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                    }

                                    //------------------------------------------------------------------------
                                    //Add UFC,DeliveryCharges and Non-Inv 
                                    for (int nonInv = 1; nonInv <= nonInvVar; nonInv++)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        /*
                                         * form 143 got location in column
                                        */
                                        if (refForm.internalId == "143")
                                        {
                                            RecordRef refLocation = new RecordRef();
                                            refLocation.internalId = con.rlocation_id;//cpng
                                            soi.location = refLocation;
                                        }

                                        //Price Level
                                        RecordRef refPriceLevel = new RecordRef();
                                        refPriceLevel.internalId = "-1";//Custom
                                        soi.price = refPriceLevel;

                                        if (nonInv == 1)//UFC
                                        {
                                            #region UFC
                                            if (con.salesType == "INST")//Only Inst sales have UFC
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolUFC);

                                                //Total Amount
                                                //soi.amount = (1*tolUFC);
                                                //soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                   // refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //mohan -21/06/2018 end//
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 2)//Delivery Charges
                                        {
                                            #region Delivery Charges
                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                            soi.item = refItem;

                                            //Qty
                                            soi.quantity = 1;
                                            soi.quantitySpecified = true;

                                            //Unit Price/Rate 
                                            soi.rate = Convert.ToString(tolDeliveryCharges);

                                            //Total Amount
                                            soi.amount = tolDeliveryCharges;
                                            soi.amountSpecified = true;

                                            if (con.subsidiaryName == "TH")
                                            {
                                                //Gst Amount
                                                soi.tax1Amt = 0;
                                                soi.tax1AmtSpecified = true;

                                                //Tax Code
                                               // refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                //mohan -21/06/2018 start//
                                                refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                //mohan -21/06/2018 end//
                                                soi.taxCode = refTaxCode;
                                            }
                                            soii[itemCount] = soi;
                                            itemCount++;
                                            #endregion
                                        }
                                        if (nonInv == 3)//Non-inventory
                                        {
                                            #region Non Inv
                                            if (tolNonInvPrice > 0)
                                            {
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_NONINV_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolNonInvPrice);

                                                //Total Amount
                                                soi.amount = tolNonInvPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = tolNonInvGstAmt;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolNonInvGstAmt > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}

                                                    //mohan -21/06/2018 start//
                                                    if (tolNonInvGstAmt > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //mohan -21/06/2018 end//
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 4)//Down Payment
                                        {
                                            #region Down Payment
                                            if (tolDownPayment > 0)
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DOWNPAYMENT_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(-1 * tolDownPayment);

                                                //Total Amount
                                                soi.amount = (-1 * tolDownPayment);
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                    soi.taxCode = refTaxCode;
                                                }
                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        //if (nonInv == 5)//VAT
                                        //{
                                        //    #region VAT
                                        //    if (tolVAT > 0)
                                        //    {
                                        //        refItem.type = RecordType.nonInventoryResaleItem;
                                        //        refItem.typeSpecified = true;
                                        //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                        //        soi.item = refItem;

                                        //        //Qty
                                        //        soi.quantity = 1;
                                        //        soi.quantitySpecified = true;

                                        //        //Unit Price/Rate 
                                        //        soi.rate = Convert.ToString(tolVAT);

                                        //        //Total Amount
                                        //        soi.amount = tolVAT;
                                        //        soi.amountSpecified = true;

                                        //        if (con.subsidiaryName == "TH")
                                        //        {
                                        //            //Gst Amount
                                        //            soi.tax1Amt = 0;
                                        //            soi.tax1AmtSpecified = true;

                                        //            //Tax Code
                                        //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                        //            soi.taxCode = refTaxCode;
                                        //        }
                                        //        soii[itemCount] = soi;
                                        //        itemCount++;
                                        //    }
                                        //    #endregion
                                        //}
                                    }

                                    soil.item = soii;
                                    so.itemList = soil;
                                    soList[soCount] = so;
                                    rowCount = soCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + con.id + "'  AND spl_subsidiary_internalID = '" + con.subsidiary + "' " +
                                                      "AND spl_sDesc = '" + con.memo + "' " +
                                                      "AND spl_ml_location_internalID = '" + con.location_id + "'" +
                                                      "AND (spl_transactionType = 'SALES') " +
                                                      "AND spl_noOfInstallments = 'CAD' " +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-SALES WITH PRICE CAD', 'CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + insertTask2);
                                    entities.Database.ExecuteSqlCommand(insertTask2);

                                    var insSalesTrx = "insert into cpas_salestransaction (cst_refNo, cst_createdDate, cst_soSeqNo, cst_sp_id, cst_sDesc, cst_sLoc, " +
                                        "cst_ml_location_internalID,cst_subsidiary,cst_subsidiary_internalID,cst_postingDate,cst_soJobID,cst_salesType) " +
                                        "values ('CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', '" + con.id + "', " +
                                        "'" + con.memo + "','" + con.location_name + "','" + con.location_id + "','" + con.subsidiaryName + "','" + con.subsidiary + "','" + convertDateToString(Convert.ToDateTime(con.postingDate)) + "'," +
                                        "'" + gjob_id.ToString() + "', '" + con.tranType + "-" + con.salesType + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    soCount++;
                                    status = true;
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASSalesWithPriceCad Exception: " + ex.ToString());
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
                                job = service.asyncAddList(soList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES WITH PRICE CAD' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var updSalesTrx = "update cpas_salestransaction set cst_soJobID = '" + jobID + "' where cst_soJobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + updSalesTrx);
                                entities.Database.ExecuteSqlCommand(updSalesTrx);

                                var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updDataPost);
                                entities.Database.ExecuteSqlCommand(updDataPost);

                                scope1.Complete();
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES WITH PRICE CAD' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceCad: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASSalesWithPriceCad: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean CPASSalesUnship(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASSalesUnship ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("CPASSalesUnship: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var cpasSales = (from c in entities.cpas_dataposting
                                         ////cpng start
                                         //join b in entities.map_bin
                                         //on c.spl_ml_location_internalID equals b.mb_bin_internalID
                                         //join m in entities.map_location
                                         //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                         ////cpng end
                                         //cpng ori start
                                         join m in entities.map_location
                                         on c.spl_ml_location_internalID equals m.ml_location_internalID
                                         //cpng ori end
                                         where (c.spl_createdDate > rangeFrom && c.spl_createdDate <= rangeTo)
                                         && (c.spl_subsidiary == "TH")
                                         && ((c.spl_transactionType == "UNSHIP") || (c.spl_transactionType == "HSFULFILL"))
                                         select new
                                         {
                                             id = c.spl_sp_id,
                                             tranType = c.spl_transactionType,
                                             businessChannel = c.spl_mb_businessChannel_internalID,
                                             postingDate = c.spl_postingDate,
                                             memo = c.spl_sDesc,
                                             location_name = c.spl_sLoc,//cpng
                                             location_id = c.spl_ml_location_internalID,
                                             rlocation_name = m.ml_location_name, //cpng
                                             rlocation_id = m.ml_location_internalID, //cpng
                                             subsidiaryName = c.spl_subsidiary,
                                             subsidiary = c.spl_subsidiary_internalID,
                                             salesType = c.spl_noOfInstallments,
                                             isFirstRun = (c.spl_netsuiteProgress == null || c.spl_netsuiteProgress == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var cpasSalesGroup = (from d in cpasSales
                                              where d.isFirstRun == "Y"
                                              select new
                                              {
                                                  d.id,
                                                  d.tranType,
                                                  d.businessChannel,
                                                  d.postingDate,
                                                  d.memo,
                                                  d.location_name,
                                                  d.location_id,
                                                  d.rlocation_name, //cpng
                                                  d.rlocation_id, //cpng
                                                  d.subsidiaryName,
                                                  d.subsidiary,
                                                  d.salesType
                                              }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASSalesUnship: " + cpasSalesGroup.Count() + " records to update.");

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

                                #region Main Information
                                switch (con.subsidiary)
                                {
                                    case "3"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY; //last time 143, change to 68 on 27/Mar/2015
                                        so.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                        so.entity = refEntity;
                                        break;
                                    case "5"://hard code
                                        refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG; //last time 138, change to 68 on 27/Mar/2015
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
                                so.tranDate = Convert.ToDateTime(con.postingDate).AddDays(-1);
                                so.tranDateSpecified = true;

                                so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                so.orderStatusSpecified = true;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = con.subsidiary;
                                so.subsidiary = refSubsidiary;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = con.businessChannel;
                                so.@class = refClass;

                                so.memo = con.memo + "; " + con.location_name + "; " + con.tranType;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                so.customFieldList = cfrList;

                                //7 = Indonesia, 6 = Thailand, 9 = India
                                //if (con.subsidiary == "6" || con.subsidiary == "7" || con.subsidiary == "9")
                                //{
                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = con.rlocation_id;//cpng
                                so.location = refLocationSO;
                                //}

                                refNo = con.id;
                                #endregion

                                #region Trx with Sales Amount
                                var conItem = (from i in entities.cpas_dataposting
                                               where i.spl_sp_id == refNo
                                               && i.spl_ml_location_internalID == con.location_id
                                               && (i.spl_createdDate > rangeFrom && i.spl_createdDate <= rangeTo)
                                               && i.spl_subsidiary == con.subsidiaryName
                                               && i.spl_transactionType == con.tranType
                                               select i).ToList();

                                var conItemGroup = from p in conItem
                                                   let k = new
                                                   {
                                                       item_name = p.spl_sPID,
                                                       item = p.spl_mi_item_internalID,
                                                       location = p.spl_ml_location_internalID,
                                                       taxCode = p.spl_taxCode
                                                   }
                                                   group p by k into g
                                                   select new
                                                   {
                                                       item_name = g.Key.item_name,
                                                       item = g.Key.item,
                                                       location = g.Key.location,
                                                       taxCode = g.Key.taxCode,
                                                       qty = g.Sum(p => p.spl_dQty),
                                                       nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                       gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                       UFC = g.Sum(p => p.spl_tolUFC),
                                                       deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                   };

                                SalesOrderItem[] soii = new SalesOrderItem[conItemGroup.Count() + 4];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                double tolUFC = 0;
                                double tolDeliveryCharges = 0;
                                double tolNonInvPrice = 0;
                                double tolNonInvGstAmt = 0;
                                double tolDownPayment = 0;
                                int nonInvVar = 4;

                                if (conItemGroup.Count() > 0)
                                {
                                    int itemCount = 0;
                                    foreach (var item in conItemGroup)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();
                                        double tolQty = Convert.ToDouble(item.qty);
                                        string unitPrice = string.Empty;
                                        double tolUnitPrice = Convert.ToDouble(item.nettPrice);
                                        double tolGstAmount = Convert.ToDouble(item.gstAmount);
                                        string taxCodeInternalID = string.Empty;

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refPriceLevel = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        if (item.item_name != "DOWNPAYMENT")
                                        {
                                            #region !DOWNPAYMENT
                                            if (tolQty > 0)
                                            {
                                                //Items
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.item;
                                                soi.item = refItem;

                                                /*
                                                 * form 143 got location in column
                                                */
                                                if (refForm.internalId == "143")
                                                {
                                                    RecordRef refLocation = new RecordRef();
                                                    refLocation.internalId = con.rlocation_id;//cpng
                                                    soi.location = refLocation;
                                                }

                                                //Price Level
                                                refPriceLevel.internalId = "-1";//Custom
                                                soi.price = refPriceLevel;

                                                //Qty
                                                soi.quantity = Convert.ToDouble(tolQty);
                                                soi.quantitySpecified = true;

                                                //Commit Status
                                                //soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                                //soi.commitInventorySpecified = true;

                                                //Unit Price/Rate
                                                unitPrice = Convert.ToString(Math.Round(tolUnitPrice / tolQty, 2));
                                                soi.rate = unitPrice;

                                                //Total Amount
                                                soi.amount = tolUnitPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //temp
                                                    //if (item.taxCode == "ZRL")
                                                    //{
                                                    //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //else
                                                    //    if (item.taxCode == "ZRE")
                                                    //    {
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        if (tolGstAmount > 0)
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        }
                                                    //        else
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        }
                                                    //    }


                                                    //mohan -21/06/2018 start//
                                                    if (item.taxCode == "VAT 7.0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else if (item.taxCode == "VAT 0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //else
                                                    //{
                                                    //    if (tolGstAmount > 0)
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //    }
                                                    //}
                                                    //mohan -21/06/2018 end//

                                                    //Gst Amount
                                                    soi.tax1Amt = tolGstAmount;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = taxCodeInternalID;
                                                    soi.taxCode = refTaxCode;
                                                }

                                                // ----------------
                                                //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

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

                                                //soi.customFieldList = cfrList2;
                                                // ----------------

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            else
                                            {
                                                tolNonInvPrice = tolNonInvPrice + tolUnitPrice;
                                                tolNonInvGstAmt = tolNonInvGstAmt + tolGstAmount;
                                            }
                                            #endregion
                                        }
                                        else
                                        {
                                            tolDownPayment = Convert.ToDouble(item.nettPrice);
                                        }

                                        tolUFC = tolUFC + Convert.ToDouble(item.UFC);
                                        tolDeliveryCharges = tolDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                    }

                                    //------------------------------------------------------------------------
                                    //Add UFC, Delivery Charges and Non-Inv 
                                    for (int nonInv = 1; nonInv <= nonInvVar; nonInv++)
                                    {
                                        SalesOrderItem soi = new SalesOrderItem();

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        /*
                                         * form 143 got location in column
                                        */
                                        if (refForm.internalId == "143")
                                        {
                                            RecordRef refLocation = new RecordRef();
                                            refLocation.internalId = con.rlocation_id;//cpng
                                            soi.location = refLocation;
                                        }

                                        //Price Level
                                        RecordRef refPriceLevel = new RecordRef();
                                        refPriceLevel.internalId = "-1";//Custom
                                        soi.price = refPriceLevel;

                                        if (nonInv == 1)//UFC
                                        {
                                            #region UFC
                                            if (con.salesType == "INST")//Only Inst sales have UFC
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolUFC);

                                                //Total Amount
                                                //soi.amount = (1*tolUFC);
                                                //soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;

                                                    //mohan -21/06/2018 end//

                                                    soi.taxCode = refTaxCode;
                                                }

                                                // ----------------
                                                //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

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

                                                //soi.customFieldList = cfrList2;
                                                // ----------------

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 2)//Delivery Charges
                                        {
                                            #region Delivery Charges
                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                            soi.item = refItem;

                                            //Qty
                                            soi.quantity = 1;
                                            soi.quantitySpecified = true;

                                            //Unit Price/Rate 
                                            soi.rate = Convert.ToString(tolDeliveryCharges);

                                            //Total Amount
                                            soi.amount = tolDeliveryCharges;
                                            soi.amountSpecified = true;

                                            if (con.subsidiaryName == "TH")
                                            {
                                                //Gst Amount
                                                soi.tax1Amt = 0;
                                                soi.tax1AmtSpecified = true;

                                                //Tax Code
                                            //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                //mohan -21/06/2018 start//
                                                refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                //mohan -21/06/2018 end//

                                                soi.taxCode = refTaxCode;
                                            }

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

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

                                            //soi.customFieldList = cfrList2;
                                            // ----------------

                                            soii[itemCount] = soi;
                                            itemCount++;
                                            #endregion
                                        }
                                        if (nonInv == 3)//Non-inventory
                                        {
                                            #region Non Inv
                                            if (tolNonInvPrice > 0)
                                            {
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_NONINV_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(tolNonInvPrice);

                                                //Total Amount
                                                soi.amount = tolNonInvPrice;
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = tolNonInvGstAmt;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolNonInvGstAmt > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}


                                                    if (tolNonInvGstAmt > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    soi.taxCode = refTaxCode;
                                                }

                                                // ----------------
                                                //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                                //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                                //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr2.value = DateTime.Now;
                                                //cfrList2[0] = scfr2;

                                                //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                                //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr3.value = DateTime.Now;
                                                //cfrList2[1] = scfr3;

                                                //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                                //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr4.value = "1";
                                                //cfrList2[2] = scfr4;

                                                //soi.customFieldList = cfrList2;
                                                // ----------------

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        if (nonInv == 4)//Down Payment
                                        {
                                            #region Down Payment
                                            if (tolDownPayment > 0)
                                            {
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DOWNPAYMENT_INTERNALID;//TEMP
                                                soi.item = refItem;

                                                //Qty
                                                soi.quantity = 1;
                                                soi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                soi.rate = Convert.ToString(-1 * tolDownPayment);

                                                //Total Amount
                                                soi.amount = (-1 * tolDownPayment);
                                                soi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    soi.tax1Amt = 0;
                                                    soi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                    soi.taxCode = refTaxCode;
                                                }

                                                // ----------------
                                                //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                                //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                                //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr2.value = DateTime.Now;
                                                //cfrList2[0] = scfr2;

                                                //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                                //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr3.value = DateTime.Now;
                                                //cfrList2[1] = scfr3;

                                                //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                                //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                                //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                                //scfr4.value = "1";
                                                //cfrList2[2] = scfr4;

                                                //soi.customFieldList = cfrList2;
                                                // ----------------

                                                soii[itemCount] = soi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                    }

                                    soil.item = soii;
                                    so.itemList = soil;
                                    soList[soCount] = so;
                                    rowCount = soCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + con.id + "'  AND spl_subsidiary_internalID = '" + con.subsidiary + "' " +
                                                      "AND spl_sDesc = '" + con.memo + "' " +
                                                      "AND spl_ml_location_internalID = '" + con.location_id + "'" +
                                                      "AND (spl_transactionType = '" + con.tranType + "') " +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-SALES UNSHIP', 'CPASDATAPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + '.' + con.tranType + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + insertTask2);
                                    entities.Database.ExecuteSqlCommand(insertTask2);

                                    var insSalesTrx = "insert into cpas_salestransaction (cst_refNo, cst_createdDate, cst_soSeqNo, cst_sp_id, cst_sDesc, cst_sLoc, " +
                                        "cst_ml_location_internalID, cst_subsidiary, cst_subsidiary_internalID, cst_postingDate, cst_soJobID, cst_salesType) " +
                                        "values ('CPASDATAPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', '" + con.id + "', " +
                                        "'" + con.memo + "','" + con.location_name + "','" + con.location_id + "','" + con.subsidiaryName + "','" + con.subsidiary + "','" + convertDateToString(Convert.ToDateTime(con.postingDate)) + "'," +
                                        "'" + gjob_id.ToString() + "','" + con.tranType + "-" + con.salesType + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    soCount++;
                                    status = true;
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASSalesUnship Exception: " + ex.ToString());
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
                                job = service.asyncAddList(soList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES UNSHIP' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var updSalesTrx = "update cpas_salestransaction set cst_soJobID = '" + jobID + "' where cst_soJobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updSalesTrx);
                                entities.Database.ExecuteSqlCommand(updSalesTrx);

                                var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updDataPost);
                                entities.Database.ExecuteSqlCommand(updDataPost);

                                scope1.Complete();
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-SALES UNSHIP' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASSalesUnship: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASSalesUnship: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }
        public Boolean CPASCashSales(DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASCashSales ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Debug("CPASCashSales: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 soCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var cpasSales = (from c in entities.cpas_dataposting
                                         ////cpng start
                                         //join b in entities.map_bin
                                         //on c.spl_ml_location_internalID equals b.mb_bin_internalID
                                         //join m in entities.map_location
                                         //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                         ////cpng end
                                         //cpng ori start
                                         join m in entities.map_location
                                         on c.spl_ml_location_internalID equals m.ml_location_internalID
                                         //cpng ori end
                                         where (c.spl_createdDate > rangeFrom && c.spl_createdDate <= rangeTo)
                                         && (c.spl_subsidiary == "TH")
                                         && (c.spl_transactionType == "SALES")
                                         && (c.spl_noOfInstallments == "COD" || c.spl_noOfInstallments == "HS")
                                         select new
                                         {
                                             id = c.spl_sp_id,
                                             tranType = c.spl_transactionType,
                                             businessChannel = c.spl_mb_businessChannel_internalID,
                                             postingDate = c.spl_postingDate,
                                             memo = c.spl_sDesc,
                                             location_name = c.spl_sLoc,//cpng
                                             location_id = c.spl_ml_location_internalID,
                                             rlocation_name = m.ml_location_name, //cpng
                                             rlocation_id = m.ml_location_internalID, //cpng
                                             subsidiaryName = c.spl_subsidiary,
                                             subsidiary = c.spl_subsidiary_internalID,
                                             salesType = c.spl_noOfInstallments,
                                             isFirstRun = (c.spl_netsuiteProgress == null || c.spl_netsuiteProgress == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var cpasSalesGroup = (from d in cpasSales
                                              where d.isFirstRun == "Y"
                                              select new
                                              {
                                                  d.id,
                                                  d.tranType,
                                                  d.businessChannel,
                                                  d.postingDate,
                                                  d.memo,
                                                  d.location_name,
                                                  d.location_id,
                                                  d.rlocation_name, //cpng
                                                  d.rlocation_id, //cpng
                                                  d.subsidiaryName,
                                                  d.subsidiary,
                                                  d.salesType
                                              }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASCashSales: " + cpasSalesGroup.Count() + " records to update.");

                        CashSale[] csList = new CashSale[cpasSalesGroup.Count()];

                        foreach (var con in cpasSalesGroup)
                        {
                            try
                            {
                                String refNo = null;
                                CashSale cs = new CashSale();

                                RecordRef refForm = new RecordRef();
                                RecordRef refEntity = new RecordRef();
                                #region Main Information
                                switch (con.subsidiaryName)
                                {
                                    case "TH"://hard code - MY
                                        //refForm.internalId = @Resource.CPAS_CASHSALES_CUSTOMFORM_MY; 
                                        //cs.customForm = refForm;

                                        refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_CASH;
                                        cs.entity = refEntity;
                                        break;
                                }

                                cs.tranDate = Convert.ToDateTime(con.postingDate).AddDays(-1);
                                cs.tranDateSpecified = true;

                                //so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                                //so.orderStatusSpecified = true;

                                RecordRef refSubsidiary = new RecordRef();
                                refSubsidiary.internalId = con.subsidiary;
                                cs.subsidiary = refSubsidiary;

                                RecordRef refClass = new RecordRef();
                                refClass.internalId = con.businessChannel;
                                cs.@class = refClass;

                                cs.memo = con.memo + "; " + con.location_name + "; " + con.salesType;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrList[0] = scfr;

                                cs.customFieldList = cfrList;

                                RecordRef refLocationSO = new RecordRef();
                                refLocationSO.internalId = con.rlocation_id;//cpng
                                cs.location = refLocationSO;

                                refNo = con.id;

                                //set account code
                                cs.undepFunds = false;
                                cs.undepFundsSpecified = true;

                                RecordRef refCashSalesAcct = new RecordRef();
                                refCashSalesAcct.internalId = @Resource.CPAS_TH_CASHSALES_ACCTCODE;//TEMP
                                cs.account = refCashSalesAcct;
                                #endregion

                                #region Cash Sales Trx
                                var conItem = (from i in entities.cpas_dataposting
                                               where i.spl_sp_id == refNo
                                               && i.spl_ml_location_internalID == con.location_id
                                               && (i.spl_createdDate > rangeFrom && i.spl_createdDate <= rangeTo)
                                               && i.spl_subsidiary == con.subsidiaryName
                                               && (i.spl_transactionType == con.tranType)
                                               && (i.spl_noOfInstallments == con.salesType)
                                               select i).ToList();

                                var conItemGroup = from p in conItem
                                                   let k = new
                                                   {
                                                       item_name = p.spl_sPID,
                                                       item = p.spl_mi_item_internalID,
                                                       location = p.spl_ml_location_internalID,
                                                       taxCode = p.spl_taxCode
                                                   }
                                                   group p by k into g
                                                   select new
                                                   {
                                                       item_name = g.Key.item_name,
                                                       item = g.Key.item,
                                                       location = g.Key.location,
                                                       taxCode = g.Key.taxCode,
                                                       qty = g.Sum(p => p.spl_dQty),
                                                       nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                       gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                       deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                   };

                                CashSaleItem[] csii = new CashSaleItem[conItemGroup.Count() + 2];
                                CashSaleItemList csil = new CashSaleItemList();
                                double tolDeliveryCharges = 0;
                                double tolNonInvPrice = 0;
                                double tolNonInvGstAmt = 0;
                                double tolDownPayment = 0;
                                //double tolVAT = 0;
                                int nonInvVar = 2;

                                if (conItemGroup.Count() > 0)
                                {
                                    int itemCount = 0;
                                    foreach (var item in conItemGroup)
                                    {
                                        CashSaleItem csi = new CashSaleItem();
                                        double tolQty = Convert.ToDouble(item.qty);
                                        string unitPrice = string.Empty;
                                        double tolUnitPrice = Convert.ToDouble(item.nettPrice);
                                        double tolGstAmount = Convert.ToDouble(item.gstAmount);
                                        string taxCodeInternalID = string.Empty;

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refPriceLevel = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        if (item.item_name != "DOWNPAYMENT" && item.item_name != "VAT")
                                        {
                                            if (tolQty > 0)
                                            {
                                                //Items
                                                refItem.type = RecordType.inventoryItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = item.item;
                                                csi.item = refItem;

                                                //Price Level
                                                refPriceLevel.internalId = "-1";//Custom
                                                csi.price = refPriceLevel;

                                                //Qty
                                                csi.quantity = Convert.ToDouble(tolQty);
                                                csi.quantitySpecified = true;

                                                //Unit Price/Rate
                                                unitPrice = Convert.ToString(Math.Round(tolUnitPrice / tolQty, 2));
                                                csi.rate = unitPrice;

                                                //Total Amount
                                                csi.amount = tolUnitPrice;
                                                csi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //temp
                                                    //if (item.taxCode == "ZRL")
                                                    //{
                                                    //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //else
                                                    //    if (item.taxCode == "ZRE")
                                                    //    {
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        if (tolGstAmount > 0)
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        }
                                                    //        else
                                                    //        {
                                                    //            taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        }
                                                    //    }

                                                    //mohan -21/06/2018 start//
                                                    if (item.taxCode == "VAT 7.0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else if (item.taxCode == "VAT 0%")
                                                    {
                                                        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //else
                                                    //{
                                                    //    if (tolGstAmount > 0)
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    //    }
                                                    //    else
                                                    //    {
                                                    //        // taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //    }
                                                    //}
                                                    //mohan -21/06/2018 end//
                                                    //Gst Amount
                                                    csi.tax1Amt = tolGstAmount;
                                                    csi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = taxCodeInternalID;
                                                    csi.taxCode = refTaxCode;
                                                }

                                                ////cpng start
                                                //InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                //InventoryAssignment IA = new InventoryAssignment();
                                                //InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                //InventoryDetail ID = new InventoryDetail();

                                                //IA.quantity = Convert.ToDouble(tolQty);
                                                //IA.quantitySpecified = true;
                                                //IA.binNumber = new RecordRef { internalId = item.location };
                                                //IAA[0] = IA;
                                                //IAL.inventoryAssignment = IAA;
                                                //ID.inventoryAssignmentList = IAL;

                                                //csi.inventoryDetail = ID;
                                                ////cpng end

                                                if (con.salesType != "HS")
                                                {
                                                    csii[itemCount] = csi;
                                                    itemCount++;
                                                }
                                                if (con.salesType == "HS")
                                                {
                                                    tolNonInvPrice = tolNonInvPrice + tolUnitPrice;
                                                    tolNonInvGstAmt = tolNonInvGstAmt + tolGstAmount;
                                                }
                                            }
                                            else
                                            {
                                                tolNonInvPrice = tolNonInvPrice + tolUnitPrice;
                                                tolNonInvGstAmt = tolNonInvGstAmt + tolGstAmount;
                                            }
                                        }
                                        //else if (item.item_name == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            tolDownPayment = Convert.ToDouble(item.nettPrice);
                                        }

                                        tolDeliveryCharges = tolDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                    }

                                    //Add DeliveryCharges and Non-Inv (not require UFC and Downpayment)
                                    for (int nonInv = 1; nonInv <= nonInvVar; nonInv++)
                                    {
                                        CashSaleItem csi = new CashSaleItem();

                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();

                                        //Price Level
                                        RecordRef refPriceLevel = new RecordRef();
                                        refPriceLevel.internalId = "-1";//Custom
                                        csi.price = refPriceLevel;

                                        if (nonInv == 1)//Delivery Charges
                                        {
                                            #region Delivery Charges
                                            refItem.type = RecordType.nonInventoryResaleItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                            csi.item = refItem;

                                            //Qty
                                            csi.quantity = 1;
                                            csi.quantitySpecified = true;

                                            //Unit Price/Rate 
                                            csi.rate = Convert.ToString(tolDeliveryCharges);

                                            //Total Amount
                                            csi.amount = tolDeliveryCharges;
                                            csi.amountSpecified = true;

                                            if (con.subsidiaryName == "TH")
                                            {
                                                //Gst Amount
                                                csi.tax1Amt = 0;
                                                csi.tax1AmtSpecified = true;

                                                //Tax Code
                                                //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                //mohan -21/06/2018 start//
                                                refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;

                                                //mohan -21/06/2018 end//

                                                csi.taxCode = refTaxCode;
                                            }
                                            csii[itemCount] = csi;
                                            itemCount++;
                                            #endregion
                                        }
                                        if (nonInv == 2)//Non-inventory
                                        {
                                            #region Non Inv
                                            if (tolNonInvPrice > 0)
                                            {
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_NONINV_INTERNALID;//TEMP
                                                csi.item = refItem;

                                                //Qty
                                                csi.quantity = 1;
                                                csi.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                csi.rate = Convert.ToString(tolNonInvPrice);

                                                //Total Amount
                                                csi.amount = tolNonInvPrice;
                                                csi.amountSpecified = true;

                                                if (con.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    csi.tax1Amt = tolNonInvGstAmt;
                                                    csi.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolNonInvGstAmt > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //mohan -21/06/2018 start//
                                                    if (tolNonInvGstAmt > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //mohan -21/06/2018 end//
                                                    csi.taxCode = refTaxCode;
                                                }
                                                csii[itemCount] = csi;
                                                itemCount++;
                                            }
                                            #endregion
                                        }
                                        //if (nonInv == 3)//VAT
                                        //{
                                        //    #region VAT
                                        //    if (tolVAT > 0)
                                        //    {
                                        //        refItem.type = RecordType.nonInventoryResaleItem;
                                        //        refItem.typeSpecified = true;
                                        //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                        //        csi.item = refItem;

                                        //        //Qty
                                        //        csi.quantity = 1;
                                        //        csi.quantitySpecified = true;

                                        //        //Unit Price/Rate 
                                        //        csi.rate = Convert.ToString(tolVAT);

                                        //        //Total Amount
                                        //        csi.amount = tolVAT;
                                        //        csi.amountSpecified = true;

                                        //        if (con.subsidiaryName == "TH")
                                        //        {
                                        //            //Gst Amount
                                        //            csi.tax1Amt = 0;
                                        //            csi.tax1AmtSpecified = true;

                                        //            //Tax Code
                                        //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                        //            csi.taxCode = refTaxCode;
                                        //        }
                                        //        csii[itemCount] = csi;
                                        //        itemCount++;
                                        //    }
                                        //    #endregion
                                        //}
                                    }

                                    csil.item = csii;
                                    cs.itemList = csil;
                                    csList[soCount] = cs;
                                    rowCount = soCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + con.id + "'  AND spl_subsidiary_internalID = '" + con.subsidiary + "' " +
                                                      "AND spl_sDesc = '" + con.memo + "' " +
                                                      "AND spl_ml_location_internalID = '" + con.location_id + "'" +
                                                      "AND (spl_transactionType = 'SALES') " +
                                                      "AND (spl_noOfInstallments = 'COD' OR spl_noOfInstallments = 'HS') " +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask2 = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-CASH SALES', 'CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASCashSales: " + insertTask2);
                                    entities.Database.ExecuteSqlCommand(insertTask2);

                                    var insSalesTrx = "insert into cpas_salestransaction (cst_refNo, cst_createdDate, cst_soSeqNo, cst_sp_id, cst_sDesc, cst_sLoc, " +
                                        "cst_ml_location_internalID,cst_subsidiary,cst_subsidiary_internalID,cst_postingDate,cst_soJobID,cst_salesType) " +
                                        "values ('CPASSTOCKPOSTING.SPL_SP_ID." + refNo + '.' + con.location_id + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', '" + con.id + "', " +
                                        "'" + con.memo + "','" + con.location_name + "','" + con.location_id + "','" + con.subsidiaryName + "','" + con.subsidiary + "','" + convertDateToString(Convert.ToDateTime(con.postingDate)) + "'," +
                                        "'" + gjob_id.ToString() + "', '" + con.tranType + "-" + con.salesType + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASCashSales: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    soCount++;
                                    status = true;
                                }
                                #endregion

                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASCashSales Exception: " + ex.ToString());
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
                                job = service.asyncAddList(csList);
                                String jobID = job.jobId;

                                var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASCashSales: " + updateTask);
                                entities.Database.ExecuteSqlCommand(updateTask);

                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-CASH SALES' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                this.DataFromNetsuiteLog.Debug("CPASCashSales: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                var updSalesTrx = "update cpas_salestransaction set cst_soJobID = '" + jobID + "' where cst_soJobID = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASCashSales: " + updSalesTrx);
                                entities.Database.ExecuteSqlCommand(updSalesTrx);

                                var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updDataPost);
                                entities.Database.ExecuteSqlCommand(updDataPost);

                                scope1.Complete();
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-CASH SALES' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASCashSales: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASCashSales: Login Netsuite failed.");
                }
            }//end of scope1
            logout();
            return status;
        }

        public Boolean CPASFulfillment(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASFulfillment ***************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {

                    this.DataFromNetsuiteLog.Debug("CPASFulfillment: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.cpas_salestransaction
                                         where q1.cst_soUpdatedDate > rangeFrom
                                         && q1.cst_soUpdatedDate <= rangeTo
                                         && q1.cst_subsidiary == "TH" //cpng
                                         && (q1.cst_salesType == "SALES-INST" || q1.cst_salesType == "SALES-CAD" || q1.cst_salesType == "UNSHIP-" || q1.cst_salesType == "HSFULFILL-HS")
                                         select new
                                         {
                                             q1.cst_recID,
                                             q1.cst_soInternalID,
                                             q1.cst_subsidiary_internalID,
                                             q1.cst_soJobID,
                                             q1.cst_sp_id,
                                             q1.cst_ml_location_internalID,
                                             q1.cst_postingDate,
                                             q1.cst_sDesc,
                                             q1.cst_sLoc,
                                             q1.cst_salesType,
                                             isFirstRun = (q1.cst_ifJobID == null || q1.cst_ifJobID == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var salesOrder = (from d in qListMono
                                          where d.isFirstRun == "Y"
                                          select new
                                          {
                                              d.cst_recID,
                                              d.cst_soInternalID,
                                              d.cst_subsidiary_internalID,
                                              d.cst_soJobID,
                                              d.cst_sp_id,
                                              d.cst_ml_location_internalID,
                                              d.cst_postingDate,
                                              d.cst_sDesc,
                                              d.cst_sLoc,
                                              d.cst_salesType
                                          }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASFulfillment: " + salesOrder.Count() + " records to update.");

                        //status = true;
                        ItemFulfillment[] iffList = new ItemFulfillment[salesOrder.Count()];
                        Int32 fulFillCount = 0;

                        foreach (var so in salesOrder)
                        {
                            try
                            {
                                var conItem = (from i in entities.cpas_dataposting
                                               where i.spl_sp_id == so.cst_sp_id
                                               && i.spl_ml_location_internalID == so.cst_ml_location_internalID
                                               && (((i.spl_transactionType + "-" + i.spl_noOfInstallments) == so.cst_salesType) || ((i.spl_transactionType + "-") == so.cst_salesType))
                                               select i).ToList();

                                var conItemGroup = from p in conItem
                                                   let k = new
                                                   {
                                                       item = p.spl_mi_item_internalID,
                                                       location = p.spl_ml_location_internalID,
                                                       taxCode = p.spl_taxCode
                                                   }
                                                   group p by k into g
                                                   select new
                                                   {
                                                       item = g.Key.item,
                                                       location = g.Key.location,
                                                       taxCode = g.Key.taxCode,
                                                       qty = g.Sum(p => p.spl_dQty),
                                                   };

                                Decimal? totalQtyinSDE = 0;
                                foreach (var item in conItemGroup)
                                {
                                    totalQtyinSDE = totalQtyinSDE + item.qty;
                                }

                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.salesOrder;
                                refSO.internalId = so.cst_soInternalID;
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

                                DateTime postingDate = Convert.ToDateTime(so.cst_postingDate).AddDays(-1); //iff1.tranDate;

                                //String[] refNoArr;
                                //String splSpID = string.Empty;
                                //String locationID = string.Empty;
                                //refNoArr = so.rnt_refNO.Split('.');
                                //if (refNoArr.Count() >= 4)
                                //{
                                //    splSpID = refNoArr[2].ToString();
                                //    locationID = refNoArr[3].ToString();

                                //    var qPostingDate = (from c in entities.cpas_stockposting
                                //                        where (c.spl_sp_id == splSpID && c.spl_ml_location_internalID == locationID)
                                //                        select c.spl_postingDate).Distinct().ToList().FirstOrDefault();

                                //    postingDate = Convert.ToDateTime(qPostingDate);
                                //}

                                iff2.tranDate = postingDate;
                                iff2.tranDateSpecified = true;

                                //iff2.postingPeriod = iff1.postingPeriod;
                                iff2.memo = so.cst_sDesc + ", " + so.cst_sLoc + ", " + so.cst_salesType;

                                ////Added for Advanced Inventory
                                iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                                iff2.shipStatusSpecified = true;

                                ItemFulfillmentItemList ifitemlist = iff1.itemList;

                                Double? totalCommitinNS = 0;
                                for (int i = 0; i < ifitemlist.item.Length; i++)
                                {
                                    totalCommitinNS = totalCommitinNS + ifitemlist.item[i].quantity;
                                }

                                RecordRef refCreatedFrom = new RecordRef();
                                refCreatedFrom.internalId = iff1.createdFrom.internalId;
                                iff2.createdFrom = refCreatedFrom;

                                ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];
                                int count1 = 0;

                                if ((totalCommitinNS - Double.Parse(totalQtyinSDE.ToString()) == 1) || (totalCommitinNS - Double.Parse(totalQtyinSDE.ToString()) == 0))
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

                                        ////cpng start
                                        //if (i < conItemGroup.Count() && conItemGroup.ElementAt(i).item != "DOWNPAYMENT" && conItemGroup.ElementAt(i).item != "VAT")
                                        //{
                                        //    InventoryAssignment[] IAA = new InventoryAssignment[1];
                                        //    InventoryAssignment IA = new InventoryAssignment();
                                        //    InventoryAssignmentList IAL = new InventoryAssignmentList();
                                        //    InventoryDetail ID = new InventoryDetail();

                                        //    IA.quantity = Convert.ToDouble(conItemGroup.ElementAt(i).qty);
                                        //    IA.quantitySpecified = true;
                                        //    IA.binNumber = new RecordRef { internalId = conItemGroup.ElementAt(i).location };
                                        //    IAA[0] = IA;
                                        //    IAL.inventoryAssignment = IAA;
                                        //    ID.inventoryAssignmentList = IAL;

                                        //    iffi.inventoryDetail = ID;
                                        //}
                                        ////cpng end

                                        ifitems[count1] = iffi;
                                        count1++;
                                    }

                                    if (count1 >= 1)
                                    {
                                        ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                        ifil1.item = ifitems;
                                        iff2.itemList = ifil1;

                                        iffList[fulFillCount] = iff2;
                                        rowCount = fulFillCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_nsInternalId, rnt_createdFromInternalID) values ('ADD', 'CPAS-FULFILLMENT', 'CPAS_SALESTRANSACTION.recID." + so.cst_recID.ToString() + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + so.cst_soInternalID + "','" + so.cst_soInternalID + "')";
                                        this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updSalesTrx = "update cpas_salestransaction set cst_ifSeqNo = '" + rowCount + "', cst_ifJobID = '" + gjob_id.ToString() + "' " +
                                                          "where cst_soJobID = '" + so.cst_soJobID + "' and cst_soInternalID = '" + so.cst_soInternalID + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        fulFillCount++;
                                        this.DataFromNetsuiteLog.Debug("CPASFulfillment: Sales order internalID_moNo: " + so.cst_soInternalID);
                                        status = true;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASFulfillment Exception: " + ex.ToString());
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

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-FULFILLMENT' " +
                                    "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    var updSalesTrx = "update cpas_salestransaction set cst_ifJobID = '" + jobID + "' where cst_ifJobID = '" + gjob_id.ToString() + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-FULFILLMENT' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASFulfillment: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }

                    }//end of sdeEntities
                    logout();
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASFulfillment: Login Netsuite failed.");
                }
                //}
            }//end of scope1

            return status;
        }
        public Boolean CPASInvoiceCreation(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            this.DataFromNetsuiteLog.Info("CPASInvoiceCreation *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();

                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASInvoiceCreation: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.cpas_salestransaction
                                         where (q1.cst_ifUpdatedDate > rangeFrom && q1.cst_ifUpdatedDate <= rangeTo)
                                         && q1.cst_ifInternalID != null
                                         && q1.cst_subsidiary == "TH" //cpng
                                         select new
                                         {
                                             q1.cst_soInternalID,
                                             q1.cst_subsidiary_internalID,
                                             q1.cst_postingDate,
                                             q1.cst_sDesc,
                                             q1.cst_sLoc,
                                             q1.cst_salesType,
                                             isFirstRun = q1.cst_invProgressStatus == null ? "Y" : "N"
                                         }).Distinct().ToList();

                        var qFilterMono = (from d in qListMono
                                           where d.isFirstRun == "Y"
                                           select new
                                           {
                                               d.cst_soInternalID,
                                               d.cst_subsidiary_internalID,
                                               d.cst_postingDate,
                                               d.cst_sDesc,
                                               d.cst_sLoc,
                                               d.cst_salesType,
                                           }).Distinct().ToList();

                        this.DataFromNetsuiteLog.Info("CPASInvoiceCreation: " + qFilterMono.Count() + " records to update.");

                        Invoice[] invList = new Invoice[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.salesOrder;
                                refSO.internalId = i.cst_soInternalID;
                                refSO.typeSpecified = true;

                                InitializeRecord recSO = new InitializeRecord();
                                recSO.type = InitializeType.invoice;
                                recSO.reference = refSO;

                                ReadResponse rrSO = service.initialize(recSO);
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
                                    refForm.internalId = @Resource.CPAS_INVOICE_CUSTOMFORM_GMY;
                                    inv.customForm = refForm;

                                    inv.tranDate = Convert.ToDateTime(i.cst_postingDate).AddDays(-1);
                                    inv.tranDateSpecified = true;

                                    inv.memo = i.cst_sDesc + ", " + i.cst_sLoc + ", " + i.cst_salesType;
                                    #endregion

                                    if (inv2.itemList != null)
                                    {
                                        invList[invCount] = inv;
                                        rowCount = invCount + 1;

                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-INVOICE', 'CPASINVOICECREATION.SOINTERNALID." + i.cst_soInternalID + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + i.cst_soInternalID + "')";
                                        this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updSalesTrx = "UPDATE cpas_salestransaction SET cst_invProgressStatus = '" + gjob_id.ToString() + "',cst_invSeqNo = '" + rowCount + "', " +
                                                          "cst_invJobID = '" + gjob_id.ToString() + "' " +
                                                          "WHERE cst_invProgressStatus IS NULL AND cst_soInternalID = '" + i.cst_soInternalID + "' " +
                                                          "AND cst_ifUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                          "AND cst_ifUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        invCount++;
                                        status = true;
                                    }
                                }
                                else
                                {
                                    var updSalesTrx = "UPDATE cpas_salestransaction SET cst_invProgressStatus = 'NO RECORD FOUND' " +
                                                      "WHERE cst_invProgressStatus IS NULL AND cst_soInternalID '" + i.cst_soInternalID + "' " +
                                                      "AND cst_ifUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                                      "AND cst_ifUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);
                                }

                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASInvoiceCreation Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updSalesTrx = "UPDATE cpas_salestransaction SET cst_invJobID = '" + jobID + "' WHERE cst_invJobID = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("CPASInvoiceCreation: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            //to capture those timeout issue - prevent duplicate happen
                            this.DataFromNetsuiteLog.Error("CPASInvoiceCreation Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASInvoiceCreation: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }

        public Boolean CPASReturnAuthorizeInstRetn(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRetn *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRetn: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "INST")
                                          && (q1.spl_cancelType == "RETN")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null) ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where (DateTime.Parse(q2.suspendDate) <= DateTime.Parse("1977-06-01"))
                                         && q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-INST";
                                string refInternalID = string.Empty;

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == "SALES-INST"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }

                                //if (q1.salesPostingCat.Contains("SALES 19")
                                //    || q1.salesPostingCat.Contains("SALES 2000")
                                //    || q1.salesPostingCat.Contains("SALES 2001")
                                //    || q1.salesPostingCat.Contains("SALES 2002")
                                //    || q1.salesPostingCat.Contains("SALES 2003")
                                //    || q1.salesPostingCat.Contains("SALES 2004")
                                //    || q1.salesPostingCat.Contains("SALES 2005")
                                //    || q1.salesPostingCat.Contains("SALES 2006")
                                //    || q1.salesPostingCat.Contains("SALES 2007")
                                //    || q1.salesPostingCat.Contains("SALES 2008")
                                //    || q1.salesPostingCat.Contains("SALES 2009")
                                //    || q1.salesPostingCat.Contains("SALES 2010")
                                //    || q1.salesPostingCat.Contains("SALES 2011")
                                //    || q1.salesPostingCat.Contains("SALES 2012")
                                //    || q1.salesPostingCat.Contains("SALES 2013")
                                //    || q1.salesPostingCat.Contains("SALES 2014")
                                //    || q1.salesPostingCat.Contains("SALES 20151")
                                //    || q1.salesPostingCat.Contains("SALES 20152")
                                //    //|| q1.salesPostingCat.Contains("SALES 20153")
                                //    //|| q1.salesPostingCat.Contains("SALES 20154")
                                //    )
                                //{
                                //    refInternalID = @Resource.CPAS_INV_BEFORECUTOFFII_INTERNALID; 
                                //}
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                refInv.type = InitializeRefType.invoice;
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                //if (raInv1 != null)
                                //{
                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    //ReturnAuthorizationItemList raInvItemlist = raInv1.itemList;
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType + "; RETN; " + q1.location_name;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && (o.spl_noOfInstallments == "INST")
                                                 && (o.spl_cancelType == "RETN")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where (DateTime.Parse(p.spl_suspendDate) <= DateTime.Parse("1977-06-01"))
                                                && p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 5;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;
                                    double tolRETNNettPrice = 0;
                                    double tolRETNGstAmount = 0;
                                    double tolRETNRevenueFinCharges = 0;//UFC
                                    double tolRETNDeliveryCharges = 0;
                                    double tolRETNUnearnedInt = 0;
                                    double osBalanceRETN = 0;
                                    double revenueOtherRETN = 0;

                                    double tolRNCONettPrice = 0;
                                    double tolRNCOGstAmount = 0;
                                    double tolRNCORevenueFinCharges = 0;//UFC
                                    double tolRNCODeliveryCharges = 0;
                                    double tolRNCOUnearnedInt = 0;
                                    double osBalanceRNCO = 0;
                                    double revenueOtherRNCO = 0;

                                    double osBalanceSuspend = 0;
                                    //double tolVAT = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.itemName != "OSBALANCE") && (item.item != "OSBALANCE") && (item.itemName != "VAT") && (item.item != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                //mohan -21/06/2018 end//


                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }

                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            //    tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------

                                            raInvitems[itemCntVar] = raiInv;
                                            itemCntVar++;
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRETN = osBalanceRETN + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN - osBalanceSuspend;
                                    revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;
                                        double tolPrice = 0;
                                        switch (i)
                                        {
                                            case 1:
                                                #region 4090001 Returns: Returns-Actual & 2185041 MY FST on Sales
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RETNNONINV_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice + tolRNCONettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = tolRETNGstAmount;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolRETNGstAmount > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}

                                                    if (tolRETNGstAmount > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }


                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //if (tolPrice > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060011 Other Revenue : Revenue-Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUE_FIN_CHARGE; //TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolRevFinCharges);

                                                //Total Amount
                                                raiInv.amount = tolRevFinCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                  //  refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 3:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 4:
                                                #region 1110070 AR-Gross:AR-Unearned Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolUnearnedInt);

                                                //Total Amount
                                                //raiInv.amount = tolUnearnedInt;
                                                //raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 5:
                                                #region 4060031 Other Revenus: Revenue - Other
                                                double tolRevOther = 0;
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUEOTHER_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;


                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN + revenueOtherRNCO;
                                                    raiInv.rate = Convert.ToString(-tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = -tolRevOther;
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN;
                                                    raiInv.rate = Convert.ToString(-tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = -tolRevOther;
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;//temp
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //if (tolRevOther > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            //case 6:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND spl_noOfInstallments = 'INST' " +
                                                      "AND spl_cancelType = 'RETN' " +
                                                      "AND ((spl_suspendDate is NULL) or (spl_suspendDate <= '1977-06-01'))" +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION INST RETN', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType, cot_postingDate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                //}
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRetn Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASSalesWithPriceInst: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRetn Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeInstRetn: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeInstRetnSuspend(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRetnSuspend *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRetnSuspend: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "INST")
                                          && (q1.spl_cancelType == "RETN")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null) ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where (DateTime.Parse(q2.suspendDate) > DateTime.Parse("1977-06-01"))
                                         && q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-INST";
                                string refInternalID = string.Empty;

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == "SALES-INST"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                refInv.type = InitializeRefType.invoice;
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
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
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType + "; RETN; SUSPEND; " + q1.location_name;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && (o.spl_noOfInstallments == "INST")
                                                 && (o.spl_cancelType == "RETN")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where (DateTime.Parse(p.spl_suspendDate) > DateTime.Parse("1977-06-01"))
                                                && p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 6;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;
                                    double tolRETNNettPrice = 0;
                                    double tolRETNGstAmount = 0;
                                    double tolRETNRevenueFinCharges = 0;//UFC
                                    double tolRETNDeliveryCharges = 0;
                                    double tolRETNUnearnedInt = 0;
                                    double osBalanceRETN = 0;
                                    double revenueOtherRETN = 0;

                                    //double tolRNCONettPrice = 0;
                                    //double tolRNCOGstAmount = 0;
                                    //double tolRNCORevenueFinCharges = 0;//UFC
                                    //double tolRNCODeliveryCharges = 0;
                                    //double tolRNCOUnearnedInt = 0;
                                    //double osBalanceRNCO = 0;
                                    //double revenueOtherRNCO = 0;

                                    double osBalanceSuspend = 0;
                                    //double tolVAT = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.item != "OSBALANCE") && (item.itemName != "OSBALANCE") && (item.item != "VAT") && (item.itemName != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;

                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                //mohan -21/06/2018 end//


                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            //    tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------

                                            raInvitems[itemCntVar] = raiInv;
                                            itemCntVar++;
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRETN = osBalanceRETN + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN - osBalanceSuspend;
                                    //revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        double tolRevFinCharges = tolRETNRevenueFinCharges;
                                        double tolDeliveryCharges = tolRETNDeliveryCharges;
                                        double tolUnearnedInt = tolRETNUnearnedInt;
                                        double tolPrice = 0;
                                        //double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        //double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        //double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;

                                        switch (i)
                                        {
                                            case 1:
                                                #region 4090001 Returns: Returns-Actual & 2185041 MY FST on Sales
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RETNNONINV_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice; //+ tolRNCONettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = tolRETNGstAmount;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolRETNGstAmount > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}

                                                    if (tolRETNGstAmount > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //if (tolPrice > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060011 Other Revenue : Revenue-Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUE_FIN_CHARGE; //TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolRevFinCharges);

                                                //Total Amount
                                                raiInv.amount = tolRevFinCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 3:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 4:
                                                #region 1110070 AR-Gross:AR-Unearned Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolUnearnedInt);

                                                //Total Amount
                                                //raiInv.amount = tolUnearnedInt;
                                                //raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp


                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 5:
                                                #region 4060031 Other Revenus: Revenue - Other
                                                double tolRevOther = 0;
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUEOTHER_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;


                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN; //+ revenueOtherRNCO;
                                                    raiInv.rate = Convert.ToString(-1 * tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = (-1 * tolRevOther);
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN;
                                                    raiInv.rate = Convert.ToString(-1 * tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = (-1 * tolRevOther);
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;//temp
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //if (tolRevOther > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 6:
                                                #region 6810010 Bad Debt Expenses : Bad Debt Actual (suspend account)
                                                //if (osBalanceSuspend > 0)
                                                //{
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_SUSPENDBadDebt_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(-1 * osBalanceSuspend);

                                                //Total Amount
                                                raiInv.amount = (-1 * osBalanceSuspend);
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;//temp
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            //case 7:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND spl_noOfInstallments = 'INST' " +
                                                      "AND spl_cancelType = 'RETN' " +
                                                      "AND ((spl_suspendDate is not NULL) AND (spl_suspendDate > '1977-06-01'))" +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION INST RETN SUSPEND', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType,cot_postingdate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION INST RETN SUSPEND', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetn: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRetnSuspend Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRetnSuspend: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRetnSuspend Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeInstRetnSuspend: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeInstRnco(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRnco *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRnco: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "INST")
                                          && (q1.spl_cancelType == "RNCO")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null) ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where (DateTime.Parse(q2.suspendDate) <= DateTime.Parse("1977-06-01"))
                                         && q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-INST";
                                string refInternalID = string.Empty;

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == "SALES-INST"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                //if(trxTypeVal.Contains("CASH SALES"))
                                //{
                                //    refInv.type = InitializeRefType.cashSale;
                                //}
                                //else
                                //{
                                refInv.type = InitializeRefType.invoice;
                                //}
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                //if (raInv1 != null)
                                //{
                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    //ReturnAuthorizationItemList raInvItemlist = raInv1.itemList;
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType + "; RNCO, " + q1.location_name;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && (o.spl_noOfInstallments == "INST")
                                                 && (o.spl_cancelType == "RNCO")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where (DateTime.Parse(p.spl_suspendDate) <= DateTime.Parse("1977-06-01"))
                                                && p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 5;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;

                                    //double tolRETNNettPrice = 0;
                                    //double tolRETNGstAmount = 0;
                                    //double tolRETNRevenueFinCharges = 0;//UFC
                                    //double tolRETNDeliveryCharges = 0;
                                    //double tolRETNUnearnedInt = 0;
                                    //double osBalanceRETN = 0;
                                    //double revenueOtherRETN = 0;

                                    double tolRNCONettPrice = 0;
                                    double tolRNCOGstAmount = 0;
                                    double tolRNCORevenueFinCharges = 0;//UFC
                                    double tolRNCODeliveryCharges = 0;
                                    double tolRNCOUnearnedInt = 0;
                                    double osBalanceRNCO = 0;
                                    double revenueOtherRNCO = 0;
                                    double osBalanceSuspend = 0;
                                    //double tolVAT = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.item != "OSBALANCE") && (item.itemName != "OSBALANCE") && (item.item != "VAT") && (item.itemName != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;

                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                //mohan -21/06/2018 end//
                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            //    tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------

                                            raInvitems[itemCntVar] = raiInv;
                                            itemCntVar++;
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRNCO = osBalanceRNCO + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    //revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN;
                                    revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        //double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        //double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        //double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;
                                        double tolRevFinCharges = tolRNCORevenueFinCharges;
                                        double tolDeliveryCharges = tolRNCODeliveryCharges;
                                        double tolUnearnedInt = tolRNCOUnearnedInt;

                                        switch (i)
                                        {
                                            case 1:
                                                #region 6810040 Bad Debt Expenses:Bad Debt-Ret Mdse C/Offs & 2185041 MY FST on Sales
                                                if (tolRNCONettPrice > 0)
                                                {
                                                    refItem.type = RecordType.nonInventoryResaleItem;
                                                    refItem.typeSpecified = true;
                                                    refItem.internalId = @Resource.CPAS_TH_RNCONONINV_INTERNALID;//TEMP
                                                    raiInv.item = refItem;

                                                    //Qty
                                                    raiInv.quantity = 1;
                                                    raiInv.quantitySpecified = true;

                                                    //Unit Price/Rate 
                                                    raiInv.rate = Convert.ToString(tolRNCONettPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolRNCONettPrice;
                                                    raiInv.amountSpecified = true;

                                                    if (q1.subsidiaryName == "TH")
                                                    {
                                                        //Gst Amount
                                                        raiInv.tax1Amt = tolRNCOGstAmount;
                                                        raiInv.tax1AmtSpecified = true;

                                                        //Tax Code
                                                        //if (tolRNCOGstAmount > 0)
                                                        //{
                                                        //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                        //}
                                                        //else
                                                        //{
                                                        //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                        //}
                                                        //mohan -21/06/2018 start//
                                                        if (tolRNCOGstAmount > 0)
                                                        {
                                                            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                        }
                                                        else
                                                        {
                                                            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                        }

                                                        //mohan -21/06/2018 end//


                                                        raiInv.taxCode = refTaxCode;
                                                    }
                                                    //raInvitems[itemCntVar] = raiInv;
                                                    //itemCntVar++;
                                                }
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060011 Other Revenue : Revenue-Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUE_FIN_CHARGE; //TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolRevFinCharges);

                                                //Total Amount
                                                raiInv.amount = tolRevFinCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 3:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 4:
                                                #region 1110070 AR-Gross:AR-Unearned Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolUnearnedInt);

                                                //Total Amount
                                                //raiInv.amount = tolUnearnedInt;
                                                //raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 5:
                                                #region 6810060 Bad Debt Expenses:Bad Debt-Cancel Cr/Mdse C/Offs
                                                if (revenueOtherRNCO > 0)
                                                {
                                                    refItem.type = RecordType.nonInventoryResaleItem;
                                                    refItem.typeSpecified = true;
                                                    refItem.internalId = @Resource.CPAS_TH_RNCOBadDebt_INTERNALID;//TEMP
                                                    raiInv.item = refItem;

                                                    //Qty
                                                    raiInv.quantity = 1;
                                                    raiInv.quantitySpecified = true;

                                                    //Unit Price/Rate 
                                                    raiInv.rate = Convert.ToString(-revenueOtherRNCO);

                                                    //Total Amount
                                                    raiInv.amount = -revenueOtherRNCO;
                                                    raiInv.amountSpecified = true;

                                                    if (q1.subsidiaryName == "TH")
                                                    {
                                                        //Gst Amount
                                                        raiInv.tax1Amt = 0;
                                                        raiInv.tax1AmtSpecified = true;

                                                        //Tax Code
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                        raiInv.taxCode = refTaxCode;
                                                    }
                                                    //raInvitems[itemCntVar] = raiInv;
                                                    //itemCntVar++;
                                                }
                                                #endregion
                                                break;
                                            //case 6:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND spl_noOfInstallments = 'INST' " +
                                                      "AND spl_cancelType = 'RNCO' " +
                                                      "AND ((spl_suspendDate is NULL) or (spl_suspendDate <= '1977-06-01'))" +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION INST RNCO', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType,cot_postingdate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION INST RNCO', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                //}
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRnco Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRnco: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRnco Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeInstRnco: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeInstRncoSuspend(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRncoSuspend *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeInstRncoSuspend: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "INST")
                                          && (q1.spl_cancelType == "RNCO")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null || q1.spl_suspendDate == "") ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where (DateTime.Parse(q2.suspendDate) > DateTime.Parse("1977-06-01"))
                                         && q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-INST";
                                string refInternalID = string.Empty;

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == "SALES-INST"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                //if(trxTypeVal.Contains("CASH SALES"))
                                //{
                                //    refInv.type = InitializeRefType.cashSale;
                                //}
                                //else
                                //{
                                refInv.type = InitializeRefType.invoice;
                                //}
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                //if (raInv1 != null)
                                //{
                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    //ReturnAuthorizationItemList raInvItemlist = raInv1.itemList;
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType + "; RNCO; SUSPEND; " + q1.location_name;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && (o.spl_noOfInstallments == "INST")
                                                 && (o.spl_cancelType == "RNCO")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where (DateTime.Parse(p.spl_suspendDate) > DateTime.Parse("1977-06-01"))
                                                && p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 6;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;

                                    //double tolRETNNettPrice = 0;
                                    //double tolRETNGstAmount = 0;
                                    //double tolRETNRevenueFinCharges = 0;//UFC
                                    //double tolRETNDeliveryCharges = 0;
                                    //double tolRETNUnearnedInt = 0;
                                    //double osBalanceRETN = 0;
                                    //double revenueOtherRETN = 0;

                                    double tolRNCONettPrice = 0;
                                    double tolRNCOGstAmount = 0;
                                    double tolRNCORevenueFinCharges = 0;//UFC
                                    double tolRNCODeliveryCharges = 0;
                                    double tolRNCOUnearnedInt = 0;
                                    double osBalanceRNCO = 0;
                                    double revenueOtherRNCO = 0;
                                    double osBalanceSuspend = 0;
                                    //double tolVAT = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.item != "OSBALANCE") && (item.itemName != "OSBALANCE") && (item.item != "VAT") && (item.itemName != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;

                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                //mohan -21/06/2018 end//
                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            //    tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------

                                            raInvitems[itemCntVar] = raiInv;
                                            itemCntVar++;
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRNCO = osBalanceRNCO + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    //revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN;
                                    revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        //double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        //double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        //double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;
                                        double tolRevFinCharges = tolRNCORevenueFinCharges;
                                        double tolDeliveryCharges = tolRNCODeliveryCharges;
                                        double tolUnearnedInt = tolRNCOUnearnedInt;

                                        double tolPrice = 0;
                                        switch (i)
                                        {
                                            case 1:
                                                #region 6810040 Bad Debt Expenses:Bad Debt-Ret Mdse C/Offs & 2185041 MY FST on Sales
                                                //if (tolRNCONettPrice > 0)
                                                //{
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RNCONONINV_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolRNCONettPrice);

                                                //Total Amount
                                                raiInv.amount = tolRNCONettPrice;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = tolRNCOGstAmount;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolRNCOGstAmount > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}


                                                    //mohan -21/06/2018 start//
                                                    if (tolRNCOGstAmount > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060011 Other Revenue : Revenue-Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUE_FIN_CHARGE; //TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolRevFinCharges);

                                                //Total Amount
                                                raiInv.amount = tolRevFinCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 3:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 4:
                                                #region 1110070 AR-Gross:AR-Unearned Finance Charges (RETN & RNCO)
                                                refItem.type = RecordType.paymentItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_UFC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolUnearnedInt);

                                                //Total Amount
                                                //raiInv.amount = tolUnearnedInt;
                                                //raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 5:
                                                #region 6810060 Bad Debt Expenses:Bad Debt-Cancel Cr/Mdse C/Offs
                                                //if (revenueOtherRNCO > 0)
                                                //{
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RNCOBadDebt_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(-revenueOtherRNCO);

                                                //Total Amount
                                                raiInv.amount = -revenueOtherRNCO;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;//temp
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 6:
                                                #region 6810010 Bad Debt Expenses : Bad Debt Actual (suspend account)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_SUSPENDBadDebt_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(-osBalanceSuspend);

                                                //Total Amount
                                                raiInv.amount = -osBalanceSuspend;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            //case 7:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND spl_noOfInstallments = 'INST' " +
                                                      "AND spl_cancelType = 'RNCO' " +
                                                      "AND ((spl_suspendDate is NOT NULL) AND (spl_suspendDate > '1977-06-01'))" +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION INST RNCO SUSPEND', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType,cot_postingdate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION INST RNCO SUSPEND', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                //}
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RNCO SUSPEND', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRncoSuspend Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeInstRncoSuspend: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeInstRncoSuspend Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeInstRncoSuspend: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeCad(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCad *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCad: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "CAD")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null || q1.spl_suspendDate == "") ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-CAD";
                                string refInternalID = string.Empty;

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == "SALES-CAD"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                //if(trxTypeVal.Contains("CASH SALES"))
                                //{
                                //    refInv.type = InitializeRefType.cashSale;
                                //}
                                //else
                                //{
                                refInv.type = InitializeRefType.invoice;
                                //}
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                //if (raInv1 != null)
                                //{
                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    //ReturnAuthorizationItemList raInvItemlist = raInv1.itemList;
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY_TRADE;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_TRADE;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && (o.spl_noOfInstallments == "CAD")
                                                 //&& (o.spl_cancelType == "RETN")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 3;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;
                                    double tolRETNNettPrice = 0;
                                    double tolRETNGstAmount = 0;
                                    double tolRETNRevenueFinCharges = 0;//UFC
                                    double tolRETNDeliveryCharges = 0;
                                    double tolRETNUnearnedInt = 0;
                                    double osBalanceRETN = 0;
                                    double revenueOtherRETN = 0;
                                    //double tolVAT = 0;

                                    //double tolRNCONettPrice = 0;
                                    //double tolRNCOGstAmount = 0;
                                    //double tolRNCORevenueFinCharges = 0;//UFC
                                    //double tolRNCODeliveryCharges = 0;
                                    //double tolRNCOUnearnedInt = 0;
                                    //double osBalanceRNCO = 0;
                                    //double revenueOtherRNCO = 0;

                                    double osBalanceSuspend = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.item != "OSBALANCE") && (item.itemName != "OSBALANCE") && (item.item != "VAT") && (item.itemName != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                //mohan -21/06/2018 end//
                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            //    tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------

                                            raInvitems[itemCntVar] = raiInv;
                                            itemCntVar++;
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRETN = osBalanceRETN + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN - osBalanceSuspend;
                                    //revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        double tolRevFinCharges = tolRETNRevenueFinCharges;
                                        double tolDeliveryCharges = tolRETNDeliveryCharges;
                                        double tolUnearnedInt = tolRETNUnearnedInt;
                                        //double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        //double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        //double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;
                                        double tolPrice = 0;
                                        switch (i)
                                        {
                                            case 1:
                                                #region 4090001 Returns: Returns-Actual & 2185041 MY FST on Sales
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RETNNONINV_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice; // +tolRNCONettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolPrice = tolRETNNettPrice;
                                                    raiInv.rate = Convert.ToString(tolPrice);

                                                    //Total Amount
                                                    raiInv.amount = tolPrice;
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = tolRETNGstAmount;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolRETNGstAmount > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //mohan -21/06/2018 start//
                                                    if (tolRETNGstAmount > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //if (tolPrice > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp

                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            case 3:
                                                #region 4060031 Other Revenus: Revenue - Other
                                                double tolRevOther = 0;
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_REVENUEOTHER_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;


                                                if (trxTypeVal.Contains("CASH SALES"))
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN; // +revenueOtherRNCO;
                                                    raiInv.rate = Convert.ToString(-1 * tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = (-1 * tolRevOther);
                                                    raiInv.amountSpecified = true;
                                                }
                                                else
                                                {
                                                    //Unit Price/Rate 
                                                    tolRevOther = revenueOtherRETN;
                                                    raiInv.rate = Convert.ToString(-1 * tolRevOther);

                                                    //Total Amount
                                                    raiInv.amount = (-1 * tolRevOther);
                                                    raiInv.amountSpecified = true;
                                                }

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_OS_INTERNALID;
                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //if (tolRevOther > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            //case 4:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND spl_noOfInstallments = 'CAD' " +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION CAD', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType,cot_postingdate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION CAD', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                //}
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCad Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCad: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCad Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeCad: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeCashSales(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCashSales *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCashSales: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 ordCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var ReturnData = (from q1 in entities.cpas_dataposting
                                          ////cpng start
                                          //join b in entities.map_bin
                                          //on q1.spl_ml_location_internalID equals b.mb_bin_internalID
                                          //join m in entities.map_location
                                          //on b.mb_bin_location_internalID equals m.ml_location_internalID
                                          ////cpng end
                                          //cpng ori start
                                          join m in entities.map_location
                                          on q1.spl_ml_location_internalID equals m.ml_location_internalID
                                          //cpng ori end
                                          where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
                                          && q1.spl_subsidiary == "TH"
                                          && (q1.spl_transactionType == "CANCEL")
                                          && (q1.spl_noOfInstallments == "COD" || q1.spl_noOfInstallments == "HS")
                                          select new
                                          {
                                              id = q1.spl_sp_id,
                                              tranType = q1.spl_transactionType,
                                              subsidiaryInternalID = q1.spl_subsidiary_internalID,
                                              businessChannel = q1.spl_mb_businessChannel_internalID,
                                              spl_postingDate = q1.spl_postingDate,
                                              location_id = q1.spl_ml_location_internalID,
                                              location_name = q1.spl_sLoc,//cpng
                                              rlocation_name = m.ml_location_name, //cpng
                                              rlocation_id = m.ml_location_internalID, //cpng
                                              subsidiaryName = q1.spl_subsidiary,
                                              salesPostingCat = q1.spl_salespostingcategory,
                                              memo = q1.spl_sDesc,
                                              salesType = q1.spl_noOfInstallments,
                                              suspendDate = (q1.spl_suspendDate == null || q1.spl_suspendDate == "") ? "1977-06-01" : q1.spl_suspendDate,
                                              isFirstRun = (q1.spl_netsuiteProgress == null || q1.spl_netsuiteProgress == "") ? "Y" : "N"
                                          }).Distinct().ToList();

                        var grpReturn = (from q2 in ReturnData
                                         where q2.isFirstRun == "Y"
                                         select new
                                         {
                                             id = q2.id,
                                             tranType = q2.tranType,
                                             subsidiaryInternalID = q2.subsidiaryInternalID,
                                             businessChannel = q2.businessChannel,
                                             spl_postingDate = q2.spl_postingDate,
                                             location_id = q2.location_id,
                                             location_name = q2.location_name,
                                             rlocation_id = q2.rlocation_id, //cpng
                                             rlocation_name = q2.rlocation_name, //cpng
                                             subsidiaryName = q2.subsidiaryName,
                                             salesPostingCat = q2.salesPostingCat,
                                             memo = q2.memo,
                                             salesType = q2.salesType,
                                         }).Distinct().ToList();

                        ReturnAuthorization[] raList = new ReturnAuthorization[grpReturn.Count];

                        foreach (var q1 in grpReturn)
                        {
                            try
                            {
                                string trxTypeVal = "SALES-" + q1.salesType;
                                string refInternalID = string.Empty;

                                string searchSalesType = "";
                                if (q1.salesType == "COD")
                                {
                                    searchSalesType = "SALES-COD";
                                }
                                if (q1.salesType == "HS")
                                {
                                    searchSalesType = "SALES-HS";
                                }

                                #region FIND INVOICE NO
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == q1.subsidiaryInternalID
                                               && qTrx.cst_sDesc == q1.salesPostingCat
                                               && qTrx.cst_ml_location_internalID == q1.location_id
                                               && qTrx.cst_salesType == searchSalesType
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_soInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }
                                #endregion

                                #region Return Authorization
                                InitializeRef refInv = new InitializeRef();
                                //if(trxTypeVal.Contains("CASH SALES"))
                                //{
                                refInv.type = InitializeRefType.cashSale;
                                //}
                                //else
                                //{
                                //refInv.type = InitializeRefType.invoice;
                                //}
                                refInv.internalId = refInternalID;
                                refInv.typeSpecified = true;

                                InitializeRecord recInv = new InitializeRecord();
                                recInv.type = InitializeType.returnAuthorization;
                                recInv.reference = refInv;

                                ReadResponse rrInv = service.initialize(recInv);
                                Record rInv = rrInv.record;

                                ReturnAuthorization raInv1 = (ReturnAuthorization)rInv;
                                ReturnAuthorization raInv2 = new ReturnAuthorization();

                                //if (raInv1 != null)
                                //{
                                #region Main Information
                                RecordRef refForm = new RecordRef();
                                refForm.internalId = @Resource.CPAS_RETURN_CUSTOMFORM_ID;
                                raInv2.customForm = refForm;

                                if (raInv1 != null)
                                {
                                    //ReturnAuthorizationItemList raInvItemlist = raInv1.itemList;
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = raInv1.createdFrom.internalId;
                                    raInv2.createdFrom = refCreatedFrom;
                                }
                                else
                                {
                                    RecordRef refEntity = new RecordRef();
                                    switch (q1.subsidiaryInternalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY_CASH;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_CASH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    raInv2.entity = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = q1.businessChannel;
                                    raInv2.@class = refClass;

                                    RecordRef refLocationSO = new RecordRef();
                                    refLocationSO.internalId = q1.rlocation_id; //cpng
                                    raInv2.location = refLocationSO;
                                }

                                raInv2.memo = q1.memo + "; " + q1.salesPostingCat + "; " + q1.salesType;

                                raInv2.tranDate = Convert.ToDateTime(q1.spl_postingDate).AddDays(-1);
                                raInv2.tranDateSpecified = true;

                                CustomFieldRef[] cfrInvList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                scfr.value = "2";
                                cfrInvList[0] = scfr;
                                raInv2.customFieldList = cfrInvList;
                                #endregion

                                #region Item Information
                                //if (raInv1.itemList != null)
                                //{
                                var raInvMain = (from o in entities.cpas_dataposting
                                                 where (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
                                                 && o.spl_subsidiary_internalID == q1.subsidiaryInternalID
                                                 && o.spl_transactionType == q1.tranType
                                                 && o.spl_ml_location_internalID == q1.location_id
                                                 && o.spl_sp_id == q1.id
                                                 && o.spl_salespostingcategory == q1.salesPostingCat
                                                 && o.spl_noOfInstallments == q1.salesType
                                                 //&& (o.spl_noOfInstallments == "COD" || o.spl_noOfInstallments == "HS")
                                                 select new
                                                 {
                                                     spl_sp_id = o.spl_sp_id,
                                                     spl_transactionType = o.spl_transactionType,
                                                     spl_subsidiary_internalID = o.spl_subsidiary_internalID,
                                                     spl_mb_businessChannel_internalID = o.spl_mb_businessChannel_internalID,
                                                     spl_postingDate = o.spl_postingDate,
                                                     spl_ml_location_internalID = o.spl_ml_location_internalID,
                                                     spl_subsidiary = o.spl_subsidiary,
                                                     spl_salespostingcategory = o.spl_salespostingcategory,
                                                     spl_sDesc = o.spl_sDesc,
                                                     spl_noOfInstallments = o.spl_noOfInstallments,
                                                     spl_sPID = o.spl_sPID,
                                                     spl_mi_item_internalID = o.spl_mi_item_internalID,
                                                     spl_taxCode = o.spl_taxCode,
                                                     spl_cancelType = o.spl_cancelType,
                                                     spl_dQty = o.spl_dQty,
                                                     spl_tolNettPrice = o.spl_tolNettPrice,
                                                     spl_tolGstAmount = o.spl_tolGstAmount,
                                                     spl_tolUFC = o.spl_tolUFC,
                                                     spl_tolDeliveryCharges = o.spl_tolDeliveryCharges,
                                                     spl_tolUnearnedInt = o.spl_tolUnearnedInt,
                                                     spl_tolRevenueFinCharges = o.spl_tolRevenueFinCharges,
                                                     spl_suspendDate = (o.spl_suspendDate == null || o.spl_suspendDate == "") ? "1977-06-01" : o.spl_suspendDate,
                                                     isFirstRun = (o.spl_netsuiteProgress == null || o.spl_netsuiteProgress == "") ? "Y" : "N"
                                                 }).ToList();

                                var raInvItem = from p in raInvMain
                                                where p.isFirstRun == "Y"
                                                let k = new
                                                {
                                                    itemName = p.spl_sPID,
                                                    item = p.spl_mi_item_internalID,
                                                    location = p.spl_ml_location_internalID,
                                                    taxCode = p.spl_taxCode,
                                                    cancelType = p.spl_cancelType,
                                                    suspendDate = p.spl_suspendDate
                                                }
                                                group p by k into g
                                                select new
                                                {
                                                    itemName = g.Key.itemName,
                                                    item = g.Key.item,
                                                    location = g.Key.location,
                                                    taxCode = g.Key.taxCode,
                                                    cancelType = g.Key.cancelType,
                                                    suspendDate = g.Key.suspendDate,
                                                    qty = g.Sum(p => p.spl_dQty),
                                                    nettPrice = g.Sum(p => p.spl_tolNettPrice),
                                                    gstAmount = g.Sum(p => p.spl_tolGstAmount),
                                                    UFC = g.Sum(p => p.spl_tolUFC),
                                                    deliveryCharges = g.Sum(p => p.spl_tolDeliveryCharges),
                                                    UnearnedInt = g.Sum(p => p.spl_tolUnearnedInt),
                                                    revenueFinCharges = g.Sum(p => p.spl_tolRevenueFinCharges),
                                                };

                                int nonInvVar = 2;
                                ReturnAuthorizationItem[] raInvitems = new ReturnAuthorizationItem[raInvItem.Count() + nonInvVar];
                                if (raInvItem.Count() > 0)
                                {
                                    int itemCntVar = 0;
                                    double tolRETNNettPrice = 0;
                                    double tolRETNGstAmount = 0;
                                    double tolRETNRevenueFinCharges = 0;//UFC
                                    double tolRETNDeliveryCharges = 0;
                                    double tolRETNUnearnedInt = 0;
                                    double osBalanceRETN = 0;
                                    double revenueOtherRETN = 0;
                                    //double tolVAT = 0;

                                    //double tolRNCONettPrice = 0;
                                    //double tolRNCOGstAmount = 0;
                                    //double tolRNCORevenueFinCharges = 0;//UFC
                                    //double tolRNCODeliveryCharges = 0;
                                    //double tolRNCOUnearnedInt = 0;
                                    //double osBalanceRNCO = 0;
                                    //double revenueOtherRNCO = 0;

                                    double osBalanceSuspend = 0;

                                    #region Inventory Items Looping
                                    foreach (var item in raInvItem)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        if ((item.item != "OSBALANCE") && (item.itemName != "OSBALANCE") && (item.item != "VAT") && (item.itemName != "VAT"))
                                        {
                                            double tolQty = Convert.ToDouble(item.qty);
                                            string taxCodeInternalID = string.Empty;

                                            //Item
                                            RecordRef refItem = new RecordRef();
                                            refItem.type = RecordType.inventoryItem;
                                            refItem.typeSpecified = true;
                                            refItem.internalId = item.item;
                                            raiInv.item = refItem;

                                            //Qty
                                            raiInv.quantity = tolQty;
                                            raiInv.quantitySpecified = true;

                                            //Set Zero Price Level
                                            RecordRef refPriceLevelInternalID = new RecordRef();
                                            refPriceLevelInternalID.internalId = "9";
                                            refPriceLevelInternalID.typeSpecified = true;
                                            raiInv.price = refPriceLevelInternalID;

                                            if (q1.subsidiaryName == "TH")
                                            {
                                                //Tax Amount
                                                raiInv.tax1Amt = 0;
                                                raiInv.tax1AmtSpecified = true;

                                                //Tax Code
                                                //if (item.taxCode == "ZRL")
                                                //{
                                                //    taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                //}
                                                //else
                                                //    if (item.taxCode == "ZRE")
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRE_INTERNALID;
                                                //    }
                                                //    else
                                                //    {
                                                //        taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                //    }
                                                //taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;

                                                //mohan -21/06/2018 start//
                                                taxCodeInternalID = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                //mohan -21/06/2018 end//

                                                RecordRef refTaxCode = new RecordRef();
                                                refTaxCode.internalId = taxCodeInternalID;
                                                raiInv.taxCode = refTaxCode;
                                            }

                                            //if (item.cancelType == "RNCO")
                                            //{
                                            //    tolRNCONettPrice = tolRNCONettPrice + Convert.ToDouble(item.nettPrice);
                                            //    tolRNCOGstAmount = tolRNCOGstAmount + Convert.ToDouble(item.gstAmount);
                                            //    tolRNCODeliveryCharges = tolRNCODeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            //    tolRNCOUnearnedInt = tolRNCOUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            //    tolRNCORevenueFinCharges = tolRNCORevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}
                                            //else
                                            //{
                                            tolRETNNettPrice = tolRETNNettPrice + Convert.ToDouble(item.nettPrice);
                                            tolRETNGstAmount = tolRETNGstAmount + Convert.ToDouble(item.gstAmount);
                                            tolRETNDeliveryCharges = tolRETNDeliveryCharges + Convert.ToDouble(item.deliveryCharges);
                                            tolRETNUnearnedInt = tolRETNUnearnedInt + Convert.ToDouble(item.UnearnedInt);
                                            tolRETNRevenueFinCharges = tolRETNRevenueFinCharges + Convert.ToDouble(item.revenueFinCharges);
                                            //}

                                            // ----------------
                                            //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                            //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                            //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr2.value = DateTime.Now;
                                            //cfrList2[0] = scfr2;

                                            //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                            //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr3.value = DateTime.Now;
                                            //cfrList2[1] = scfr3;

                                            //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                            //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                            //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                            //scfr4.value = "1";
                                            //cfrList2[2] = scfr4;

                                            //raiInv.customFieldList = cfrList2;
                                            // ----------------
                                            if (q1.salesType != "HS")
                                            {
                                                raInvitems[itemCntVar] = raiInv;
                                                itemCntVar++;
                                            }
                                        }
                                        //else if (item.itemName == "VAT")
                                        //{
                                        //    tolVAT = Convert.ToDouble(item.nettPrice);
                                        //}
                                        else
                                        {
                                            if (DateTime.Parse(item.suspendDate) > DateTime.Parse("1977-06-01"))
                                            {
                                                osBalanceSuspend = osBalanceSuspend + Convert.ToDouble(item.nettPrice);
                                            }
                                            else
                                            {
                                                osBalanceRETN = osBalanceRETN + Convert.ToDouble(item.nettPrice);
                                            }
                                        }
                                    }
                                    #endregion

                                    #region Non-Inventory Items Looping
                                    revenueOtherRETN = (tolRETNNettPrice + tolRETNGstAmount + tolRETNDeliveryCharges + tolRETNUnearnedInt + tolRETNRevenueFinCharges) - osBalanceRETN - osBalanceSuspend;
                                    //revenueOtherRNCO = (tolRNCONettPrice + tolRNCOGstAmount + tolRNCODeliveryCharges + tolRNCOUnearnedInt + tolRNCORevenueFinCharges) - osBalanceRNCO - osBalanceSuspend;

                                    for (int i = 1; i <= nonInvVar; i++)
                                    {
                                        ReturnAuthorizationItem raiInv = new ReturnAuthorizationItem();
                                        RecordRef refItem = new RecordRef();
                                        RecordRef refTaxCode = new RecordRef();
                                        double tolRevFinCharges = tolRETNRevenueFinCharges;
                                        double tolDeliveryCharges = tolRETNDeliveryCharges;
                                        double tolUnearnedInt = tolRETNUnearnedInt;
                                        //double tolRevFinCharges = tolRETNRevenueFinCharges + tolRNCORevenueFinCharges;
                                        //double tolDeliveryCharges = tolRETNDeliveryCharges + tolRNCODeliveryCharges;
                                        //double tolUnearnedInt = tolRETNUnearnedInt + tolRNCOUnearnedInt;
                                        double tolPrice = 0;
                                        switch (i)
                                        {
                                            case 1:
                                                #region 4090001 Returns: Returns-Actual & 2185041 MY FST on Sales
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_RETNNONINV_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //if (trxTypeVal.Contains("CASH SALES"))
                                                //{
                                                //Unit Price/Rate 
                                                tolPrice = tolRETNNettPrice; // +tolRNCONettPrice;
                                                raiInv.rate = Convert.ToString(tolPrice);

                                                //Total Amount
                                                raiInv.amount = tolPrice;
                                                raiInv.amountSpecified = true;
                                                //}
                                                //else
                                                //{
                                                //    //Unit Price/Rate 
                                                //    tolPrice = tolRETNNettPrice;
                                                //    raiInv.rate = Convert.ToString(tolPrice);

                                                //    //Total Amount
                                                //    raiInv.amount = tolPrice;
                                                //    raiInv.amountSpecified = true;
                                                //}

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = tolRETNGstAmount; // +tolRNCOGstAmount;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //if (tolRETNGstAmount > 0)
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_SR_INTERNALID;
                                                    //}
                                                    //else
                                                    //{
                                                    //    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;
                                                    //}
                                                    //mohan -21/06/2018 start//
                                                    if (tolRETNGstAmount > 0)
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT7_INTERNALID;
                                                    }
                                                    else
                                                    {
                                                        refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;
                                                    }
                                                    //mohan -21/06/2018 start//
                                                    raiInv.taxCode = refTaxCode;
                                                }

                                                //if (tolPrice > 0)
                                                //{
                                                //    raInvitems[itemCntVar] = raiInv;
                                                //    itemCntVar++;
                                                //}
                                                #endregion
                                                break;
                                            case 2:
                                                #region 4060001 Other Revenue : Revenue-Postage,Shipping and Handling (RETN & RNCO)
                                                refItem.type = RecordType.nonInventoryResaleItem;
                                                refItem.typeSpecified = true;
                                                refItem.internalId = @Resource.CPAS_TH_DC_INTERNALID;//TEMP
                                                raiInv.item = refItem;

                                                //Qty
                                                raiInv.quantity = 1;
                                                raiInv.quantitySpecified = true;

                                                //Unit Price/Rate 
                                                raiInv.rate = Convert.ToString(tolDeliveryCharges);

                                                //Total Amount
                                                raiInv.amount = tolDeliveryCharges;
                                                raiInv.amountSpecified = true;

                                                if (q1.subsidiaryName == "TH")
                                                {
                                                    //Gst Amount
                                                    raiInv.tax1Amt = 0;
                                                    raiInv.tax1AmtSpecified = true;

                                                    //Tax Code
                                                    //refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp


                                                    //mohan -21/06/2018 start//
                                                    refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_VAT0_INTERNALID;//temp

                                                    //mohan -21/06/2018 end//

                                                    raiInv.taxCode = refTaxCode;
                                                }
                                                //raInvitems[itemCntVar] = raiInv;
                                                //itemCntVar++;
                                                #endregion
                                                break;
                                            //case 3:
                                            //    #region VAT
                                            //    if (tolVAT > 0)
                                            //    {
                                            //        refItem.type = RecordType.nonInventoryResaleItem;
                                            //        refItem.typeSpecified = true;
                                            //        refItem.internalId = @Resource.CPAS_TH_VAT_INTERNALID;//TEMP
                                            //        raiInv.item = refItem;

                                            //        //Qty
                                            //        raiInv.quantity = 1;
                                            //        raiInv.quantitySpecified = true;

                                            //        //Unit Price/Rate 
                                            //        raiInv.rate = Convert.ToString(tolVAT);

                                            //        //Total Amount
                                            //        raiInv.amount = tolVAT;
                                            //        raiInv.amountSpecified = true;

                                            //        if (q1.subsidiaryName == "TH")
                                            //        {
                                            //            //Gst Amount
                                            //            raiInv.tax1Amt = 0;
                                            //            raiInv.tax1AmtSpecified = true;

                                            //            //Tax Code
                                            //            refTaxCode.internalId = @Resource.CPAS_TH_TAXCODE_ZRL_INTERNALID;//temp
                                            //            raiInv.taxCode = refTaxCode;
                                            //        }
                                            //    }
                                            //    #endregion
                                            //    break;
                                        }

                                        //CustomFieldRef[] cfrList2 = new CustomFieldRef[3];

                                        //DateCustomFieldRef scfr2 = new DateCustomFieldRef();
                                        //scfr2.scriptId = "custcol_sto_end_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr2.internalId = "2759"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr2.value = DateTime.Now;
                                        //cfrList2[0] = scfr2;

                                        //DateCustomFieldRef scfr3 = new DateCustomFieldRef();
                                        //scfr3.scriptId = "custcol_sto_start_date"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr3.internalId = "2760"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr3.value = DateTime.Now;
                                        //cfrList2[1] = scfr3;

                                        //StringCustomFieldRef scfr4 = new StringCustomFieldRef();
                                        //scfr4.scriptId = "custcol_order_type"; //@Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                                        //scfr4.internalId = "2804"; //@Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                                        //scfr4.value = "1";
                                        //cfrList2[2] = scfr4;

                                        //raiInv.customFieldList = cfrList2;

                                        raInvitems[itemCntVar] = raiInv;
                                        itemCntVar++;

                                    }
                                    #endregion

                                    ReturnAuthorizationItemList railInv = new ReturnAuthorizationItemList();
                                    railInv.item = raInvitems;
                                    railInv.replaceAll = true;
                                    raInv2.itemList = railInv;

                                    raList[ordCount] = raInv2;
                                    rowCount = ordCount + 1;

                                    var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
                                                      "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiaryInternalID + "' " +
                                                      "AND spl_sDesc = '" + q1.memo + "' " +
                                                      "AND spl_salespostingcategory = '" + q1.salesPostingCat + "' " +
                                                      "AND spl_ml_location_internalID = '" + q1.location_id + "'" +
                                                      "AND (spl_transactionType = 'CANCEL') " +
                                                      "AND (spl_noOfInstallments = 'COD' OR spl_noOfInstallments = 'HS')" +
                                                      "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZATION CASH SALES', 'CPASSTOCKPOSTING.SPL_SP_ID." + q1.id + '.' + q1.location_id + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    DateTime _postDate = Convert.ToDateTime(q1.spl_postingDate);
                                    var insSalesTrx = "insert into cpas_otherstransaction (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType,cot_postingdate) " +
                                        "values ('CPASDATAPOSTING." + q1.memo + '.' + q1.salesPostingCat + '.' + q1.location_name + "', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "', 'CPAS-RETURN AUTHORIZATION CASH SALES', " +
                                        "'" + refInternalID + "','" + gjob_id.ToString() + "','" + q1.subsidiaryName + "','" + q1.subsidiaryInternalID + "','" + trxTypeVal + "','" + convertDateToString(_postDate) + "')";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + insSalesTrx);
                                    entities.Database.ExecuteSqlCommand(insSalesTrx);

                                    ordCount++;
                                    status = true;
                                }
                                //}
                                #endregion
                                //}
                                //else
                                //{
                                //    var insSalesTrx = "insert into cpas_otherstransaction_error (cot_refNo, cot_invDate, cot_seqNo, cot_trxType, cot_invInternalID, " +
                                //        "cot_trxProgressStatus,cot_subsidiary,cot_subsidiaryInternalID,cot_salesType) " +
                                //        "values ('CPASSTOCKPOSTING.SALESPOSTINGCAT." + q1.salesPostingCat + '.' + q1.location_id + "', '" + convertDateToString(DateTime.Now) + "', '', 'CPAS-RETURN AUTHORIZATION INST RETN', " +
                                //        "'" + refInternalID + "', '', '" + q1.subsidiaryName + "', '" + q1.subsidiaryInternalID + "', '" + trxTypeVal + "')";
                                //    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + insSalesTrx);
                                //    entities.Database.ExecuteSqlCommand(insSalesTrx);
                                //}
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCashSales Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(raList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updTrx = "update cpas_otherstransaction set cot_trxProgressStatus = '" + jobID + "' where cot_trxProgressStatus = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updTrx);
                                        entities.Database.ExecuteSqlCommand(updTrx);

                                        var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updDataPost);
                                        entities.Database.ExecuteSqlCommand(updDataPost);

                                        scope1.Complete();
                                    }
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "'";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCashSales: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCashSales Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeCashSales: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }

        public Boolean CPASReturnAuthorizeItemsReceipt(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeItemsReceipt ***************");
            Boolean status = false;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 raCount = 0;
                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from ra in entities.cpas_otherstransaction
                                         where ra.cot_trxUpdatedDate > rangeFrom
                                         && ra.cot_trxUpdatedDate <= rangeTo
                                         && ra.cot_subsidiary == "TH" //cpng
                                         select new
                                         {
                                             ra.cot_cnInternalID,
                                             ra.cot_cnProgressStatus,
                                             ra.cot_cnUpdatedDate,
                                             ra.cot_trxInternalID,
                                             ra.cot_refNo,
                                             ra.cot_trxType,
                                             ra.cot_trxUpdatedDate,
                                             ra.cot_postingDate,
                                             isFirstRun = (ra.cot_irProgressStatus == null || ra.cot_irProgressStatus == "") ? "Y" : "N"
                                         }).ToList();

                        var returnAuthorization = (from ra2 in qListMono
                                                   where ra2.isFirstRun == "Y"
                                                   orderby ra2.cot_trxInternalID ascending
                                                   select ra2).ToList();

                        //var returnAuthorization = (from ra in entities.cpas_otherstransaction
                        //                           where ra.cot_trxUpdatedDate > rangeFrom 
                        //                           && ra.cot_trxUpdatedDate <= rangeTo
                        //                           && (ra.cot_irProgressStatus == null || ra.cot_irProgressStatus == "")
                        //                           && ra.cot_subsidiary == "TH" 
                        //                           select ra).ToList();

                        this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeItemsReceipt: " + returnAuthorization.Count() + " records to update.");
                        ItemReceipt[] irList = new ItemReceipt[returnAuthorization.Count()];

                        foreach (var r in returnAuthorization)
                        {
                            try
                            {
                                InitializeRef refRA = new InitializeRef();
                                refRA.type = InitializeRefType.returnAuthorization;
                                refRA.internalId = r.cot_trxInternalID;
                                refRA.typeSpecified = true;

                                InitializeRecord recRA = new InitializeRecord();
                                recRA.type = InitializeType.itemReceipt;
                                recRA.reference = refRA;

                                ReadResponse rrRA = service.initialize(recRA);
                                Record rRA = rrRA.record;

                                ItemReceipt ir1 = (ItemReceipt)rRA;
                                ItemReceipt ir2 = new ItemReceipt();

                                if (ir1 != null)
                                {
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = ir1.createdFrom.internalId;
                                    ir2.createdFrom = refCreatedFrom;

                                    ir2.tranDate = Convert.ToDateTime(r.cot_postingDate).AddDays(-1);  // ir1.tranDate; 
                                    ir2.tranDateSpecified = true;

                                    //ir2.postingPeriod = ir1.postingPeriod;
                                    ir2.memo = r.cot_refNo.Replace("CPASDATAPOSTING.", "") + ", " + r.cot_trxType.Replace("CPAS-RETURN AUTHORIZATION ", "");

                                    ItemReceiptItemList iril1 = new ItemReceiptItemList();
                                    iril1 = ir1.itemList;

                                    //var returnAuthorizationItem = (from rai in entities.netsuite_returnitem
                                    //                               where rai.nsri_nsr_rr_ID == r.nsr_rr_ID
                                    //                               select rai).ToList();

                                    if (iril1.item.Count() > 0)
                                    {
                                        //Int32[] receiveQty = new Int32[returnAuthorizationItem.Count()];
                                        //String[] itemID = new String[returnAuthorizationItem.Count()];
                                        //for (int i = 0; i < iril1.item.Count(); i++)
                                        //{
                                        //    if (iril1.item[i].item.internalId.Equals(returnAuthorizationItem[i].nsri_item_internalID))
                                        //    {
                                        //        iril1.item[i].quantity = Convert.ToInt32(returnAuthorizationItem[i].nsri_rritem_receive_qty);
                                        //        iril1.item[i].itemReceive = true;
                                        //        iril1.item[i].itemReceiveSpecified = true;
                                        //    }
                                        //    else
                                        //    {
                                        //        iril1.item[i].quantity = 0;
                                        //        iril1.item[i].itemReceive = false;
                                        //        iril1.item[i].itemReceiveSpecified = true;
                                        //    }
                                        //}

                                        ////cpng start
                                        //ItemReceiptItem[] ifitems = new ItemReceiptItem[iril1.item.Length];
                                        //int count1 = 0;
                                        //for (int i = 0; i < iril1.item.Length; i++)
                                        //{
                                        //    ItemReceiptItem iffi = new ItemReceiptItem();
                                        //    RecordRef refItem = new RecordRef();
                                        //    iffi.item = iril1.item[i].item;

                                        //    iffi.orderLine = iril1.item[i].orderLine;
                                        //    iffi.orderLineSpecified = true;

                                        //    RecordRef refLocation = new RecordRef();
                                        //    iffi.location = iril1.item[i].location;

                                        //    iffi.quantity = iril1.item[i].quantity;
                                        //    iffi.quantitySpecified = true;

                                        //    iffi.itemReceive = true;
                                        //    iffi.itemReceiveSpecified = true;

                                        //    //cpng start
                                        //    if (iril1.item[i].item.internalId != @Resource.CPAS_TH_RETNNONINV_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_REVENUE_FIN_CHARGE
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_DC_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_UFC_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_REVENUEOTHER_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_SUSPENDBadDebt_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_RNCONONINV_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_RNCOBadDebt_INTERNALID
                                        //        && iril1.item[i].item.internalId != @Resource.CPAS_TH_VAT_INTERNALID) // item is not non-inventory
                                        //    {
                                        //        InventoryAssignment[] IAA = new InventoryAssignment[1];
                                        //        InventoryAssignment IA = new InventoryAssignment();
                                        //        InventoryAssignmentList IAL = new InventoryAssignmentList();
                                        //        InventoryDetail ID = new InventoryDetail();

                                        //        IA.quantity = iril1.item[i].quantity;
                                        //        IA.quantitySpecified = true;
                                        //        IA.binNumber = new RecordRef { internalId = @Resource.CPAS_TH_DEFAULT_BIN };
                                        //        IAA[0] = IA;
                                        //        IAL.inventoryAssignment = IAA;
                                        //        ID.inventoryAssignmentList = IAL;

                                        //        iffi.inventoryDetail = ID;
                                        //    }
                                        //    //cpng end
                                        //    ifitems[count1] = iffi;
                                        //    count1++;
                                        //}
                                        //ItemReceiptItemList ifil1 = new ItemReceiptItemList();
                                        //ifil1.item = ifitems;
                                        //ir2.itemList = ifil1;
                                        ////cpng end
                                        ir2.itemList = iril1;
                                        irList[raCount] = ir2;
                                        rowCount = raCount + 1;

                                        String refNo = "NETSUITE_RETURN.COT_REFNO." + r.cot_refNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZE ITEMS RECEIPT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + r.cot_trxInternalID + "')";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        var updateCpasOthersTransaction = "update cpas_otherstransaction set cot_irProgressStatus='" + gjob_id.ToString() + "', cot_irSeqNo='" + rowCount + "'" +
                                            " where (cot_irProgressStatus is null or cot_irProgressStatus='') AND cot_trxInternalID = '" + r.cot_trxInternalID + "' " +
                                            " and cot_trxUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                            " and cot_trxUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + updateCpasOthersTransaction);
                                        entities.Database.ExecuteSqlCommand(updateCpasOthersTransaction);

                                        raCount++;
                                        status = true;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeItemsReceipt Exception: (" + r.cot_trxInternalID + "," + r.cot_trxUpdatedDate + ")" + ex.ToString());
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
                            {
                                job = service.asyncAddList(irList);
                                String jobID = job.jobId;

                                if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                {
                                    var updateCpasOthersTransaction = "update cpas_otherstransaction set cot_irProgressStatus='" + jobID + "'" +
                                        " where cot_irProgressStatus= '" + gjob_id.ToString() + "'" +
                                        " and cot_trxUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                        " and cot_trxUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + updateCpasOthersTransaction);
                                    entities.Database.ExecuteSqlCommand(updateCpasOthersTransaction);

                                    var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + updateTask);
                                    entities.Database.ExecuteSqlCommand(updateTask);

                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-RETURN AUTHORIZE ITEMS RECEIPT' " +
                                        "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                        }
                        else if (rowCount == 0)
                        {
                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA', rn_status='NO-DATA'," +
                                "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-RETURN AUTHORIZE ITEMS RECEIPT' " +
                                "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
                            this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeItemsReceipt: " + updateRequestNetsuite);
                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                            scope1.Complete();
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeItemsReceipt: Login Netsuite failed.");
                }
            } //end of scope1
            logout();
            return status;
        }
        public Boolean CPASReturnAuthorizeCreditMemo(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #825 */
            this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCreditMemo *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCreditMemo: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.cpas_otherstransaction
                                         where q1.cot_irUpdateDate > rangeFrom
                                         && q1.cot_irUpdateDate <= rangeTo
                                         && q1.cot_subsidiary == "TH" //cpng
                                         //&& (q1.cot_salesType == "SALES-INST" || q1.cot_salesType == "SALES-CAD")
                                         select new
                                         {
                                             q1.cot_trxInternalID,
                                             q1.cot_invInternalID,
                                             q1.cot_subsidiaryInternalID,
                                             q1.cot_postingDate,
                                             q1.cot_refNo,
                                             q1.cot_trxType,
                                             q1.cot_recID,
                                             isFirstRun = (q1.cot_cnProgressStatus == null || q1.cot_cnProgressStatus == "") ? "Y" : "N"
                                         }).Distinct().ToList();

                        var qFilterMono = (from d in qListMono
                                           where d.isFirstRun == "Y"
                                           orderby d.cot_trxInternalID ascending
                                           select new
                                           {
                                               d.cot_trxInternalID,
                                               d.cot_invInternalID,
                                               d.cot_subsidiaryInternalID,
                                               d.cot_postingDate,
                                               d.cot_refNo,
                                               d.cot_trxType,
                                               d.cot_recID,
                                           }).Distinct().ToList();

                        //this.DataFromNetsuiteLog.Info("CPASReturnAuthorizeCreditMemo:" + qFilterMono.Count() + " records to update.");

                        //CreditMemo[] invList = new CreditMemo[1];
                        CreditMemo[] invList = new CreditMemo[qFilterMono.Count()];
                        #region Create new memo - Sample
                        //try
                        //{
                        //    InitializeRef refSO = new InitializeRef();
                        //    refSO.type = InitializeRefType.returnAuthorization;
                        //    refSO.internalId = qFilterMono.cot_trxInternalID;
                        //    refSO.typeSpecified = true;

                        //    InitializeRecord recSO = new InitializeRecord();
                        //    recSO.type = InitializeType.creditMemo;
                        //    recSO.reference = refSO;

                        //    ReadResponse rrSO = service.initialize(recSO);
                        //    Record rSO = rrSO.record;

                        //    CreditMemo inv2 = (CreditMemo)rSO;
                        //    CreditMemo inv = new CreditMemo();

                        //    if (inv2 != null)
                        //    {
                        //        inv.entity = inv2.entity;
                        //        inv.location = inv2.location;
                        //        #region Main Information
                        //        //createdfrom 
                        //        RecordRef refCreatedFrom = new RecordRef();
                        //        refCreatedFrom.internalId = inv2.createdFrom.internalId;
                        //        inv.createdFrom = refCreatedFrom;

                        //        inv.tranDate = Convert.ToDateTime(qFilterMono.cot_postingDate).AddDays(-1);  // inv2.tranDate; 
                        //        inv.tranDateSpecified = true;

                        //        //inv.postingPeriod = inv2.postingPeriod;
                        //        inv.memo = qFilterMono.cot_refNo.Replace("CPASDATAPOSTING.", "") + ", " + qFilterMono.cot_trxType.Replace("CPAS-RETURN AUTHORIZATION ", "");

                        //        if (inv2.itemList != null)
                        //        {
                        //            CreditMemoItemList itemlst = inv2.itemList;
                        //            CreditMemoItem[] itm = new CreditMemoItem[itemlst.item.Count()];
                        //            int itmCount = 0;
                        //            for (int ii = 0; ii < itemlst.item.Length; ii++)
                        //            {
                        //                itm[itmCount] = new CreditMemoItem();
                        //                itm[itmCount].item = itemlst.item[ii].item;
                        //                itm[itmCount].quantity = itemlst.item[ii].quantity;
                        //                itm[itmCount].quantitySpecified = itemlst.item[ii].quantitySpecified;
                        //                itm[itmCount].amount = itemlst.item[ii].amount;
                        //                itm[itmCount].amountSpecified = itemlst.item[ii].amountSpecified;
                        //                itmCount++;
                        //            }
                        //            CreditMemoItemList itmfill = new CreditMemoItemList();
                        //            itmfill.item = itm;

                        //            inv.itemList = itmfill;
                        //        }

                        //        #endregion

                        //        if (inv2.itemList != null)
                        //        {
                        //            invList[invCount] = inv;
                        //            rowCount = invCount + 1;

                        //            ////////////////////////////////////////////////////////////
                        //            String refNo = "NETSUITE_CREDITMEMO.RETURNAUTHORIZE." + qFilterMono.cot_trxInternalID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                        //            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                        //                "rnt_seqNO, rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZE CREDIT MEMO', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                        //                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + qFilterMono.cot_trxInternalID + "')";

                        //            this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + insertTask);
                        //            entities.Database.ExecuteSqlCommand(insertTask);

                        //            ////////////////////////////////////////////////////////////
                        //            var updSalesTrx = "UPDATE cpas_otherstransaction SET cot_cnProgressStatus = '" + gjob_id.ToString() + "', cot_cnSeqNo = '" + rowCount + "' " +
                        //                              "WHERE (cot_cnProgressStatus is NULL or cot_cnProgressStatus='') AND cot_recID = '" + qFilterMono.cot_recID + "' ";

                        //            this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updSalesTrx);
                        //            entities.Database.ExecuteSqlCommand(updSalesTrx);

                        //            ////////////////////////////////////////////////////////////

                        //            invCount++;
                        //            status = true;
                        //        }
                        //    }
                        //    else
                        //    {

                        //    }

                        //}
                        //catch (Exception ex)
                        //{
                        //    this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCreditMemo Exception: " + ex.ToString());
                        //    status = false;
                        //    if (rowCount == 0)
                        //    {
                        //        rowCount++;
                        //    }
                        //    //break;
                        //}
                        #endregion


                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.returnAuthorization;
                                refSO.internalId = i.cot_trxInternalID;
                                refSO.typeSpecified = true;

                                InitializeRecord recSO = new InitializeRecord();
                                recSO.type = InitializeType.creditMemo;
                                recSO.reference = refSO;

                                ReadResponse rrSO = service.initialize(recSO);
                                Record rSO = rrSO.record;

                                CreditMemo inv2 = (CreditMemo)rSO;
                                CreditMemo inv = new CreditMemo();

                                if (inv2 != null)
                                {
                                    //inv.entity = inv2.entity;
                                    //inv.location = inv2.location;

                                    #region Main Information
                                    //createdfrom 
                                    RecordRef refCreatedFrom = new RecordRef();
                                    refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                    inv.createdFrom = refCreatedFrom;

                                    inv.tranDate = Convert.ToDateTime(i.cot_postingDate).AddDays(-1);  // inv2.tranDate; 
                                    inv.tranDateSpecified = true;

                                    //inv.postingPeriod = inv2.postingPeriod;
                                    inv.memo = i.cot_refNo.Replace("CPASDATAPOSTING.", "") + ", " + i.cot_trxType.Replace("CPAS-RETURN AUTHORIZATION ", "");

                                    //inv.autoApply = true;
                                    //inv.autoApplySpecified = true;

                                    //CreditMemoApply[] ifitems = new CreditMemoApply[1];
                                    //CreditMemoApplyList ifil1 = new CreditMemoApplyList();
                                    //ifil1.apply = ifitems;
                                    //ifil1.replaceAll = false;

                                    //inv.applyList = ifil1;


                                    //if (inv2.applyList != null)
                                    //{
                                    //    //inv.applyList = inv2.applyList;

                                    //    CreditMemoApplyList ifitemlist = inv2.applyList;
                                    //    CreditMemoApply[] ifitems = new CreditMemoApply[1];
                                    //    int count1 = 0;
                                    //    String invOffset = (i.cot_invInternalID == null || i.cot_invInternalID == "") ? "000000" : i.cot_invInternalID;
                                    //    for (int ii = 0; ii < ifitemlist.apply.Length; ii++)
                                    //    {

                                    //        if (ifitemlist.apply[ii].type == "Invoice" || ifitemlist.apply[ii].type == "Journal")
                                    //        {
                                    //            if (invOffset == "000000")
                                    //            {
                                    //                if (ifitemlist.apply[ii].type == "Journal")
                                    //                {
                                    //                    ifitemlist.apply[ii].apply = true;
                                    //                    ifitemlist.apply[ii].applySpecified = true;
                                    //                }
                                    //                else
                                    //                {
                                    //                    ifitemlist.apply[ii].apply = false;
                                    //                    ifitemlist.apply[ii].applySpecified = false;
                                    //                }
                                    //            }
                                    //            else
                                    //            {
                                    //                if (ifitemlist.apply[ii].doc == Convert.ToInt64(invOffset))
                                    //                {
                                    //                    ifitemlist.apply[ii].apply = true;
                                    //                    ifitemlist.apply[ii].applySpecified = true;
                                    //                }
                                    //                else
                                    //                {
                                    //                    ifitemlist.apply[ii].apply = false;
                                    //                    ifitemlist.apply[ii].applySpecified = false;
                                    //                }
                                    //            }


                                    //            ifitems[count1] = ifitemlist.apply[ii];
                                    //            count1++;

                                    //            Array.Resize(ref ifitems, count1 + 1);

                                    //        }

                                    //    }

                                    //    CreditMemoApplyList ifil1 = new CreditMemoApplyList();

                                    //    ifil1.apply = ifitems;
                                    //    ifil1.replaceAll = true;
                                    //    inv.applyList = ifil1;
                                    //}



                                    if (inv2.applyList != null)
                                    {
                                        double invAmount = inv2.total;
                                        CreditMemoApplyList ifitemlist = inv2.applyList;
                                        CreditMemoApply[] ifitems = new CreditMemoApply[1];
                                        CreditMemoApply[] journalitems = new CreditMemoApply[1];

                                        String invOffset = (i.cot_invInternalID == null || i.cot_invInternalID == "") ? "000000" : i.cot_invInternalID;
                                        for (int ii = 0; ii < ifitemlist.apply.Length; ii++)
                                        {
                                            if (ifitemlist.apply[ii].type == "Journal")
                                            {
                                                ifitemlist.apply[ii].apply = true;
                                                ifitemlist.apply[ii].applySpecified = true;
                                                journalitems[0] = ifitemlist.apply[ii];
                                                break;
                                            }
                                        }
                                        if (invOffset == "000000")
                                        {
                                            ifitems[0] = journalitems[0];
                                        }
                                        else
                                        {
                                            for (int ii = 0; ii < ifitemlist.apply.Length; ii++)
                                            {
                                                if (ifitemlist.apply[ii].type == "Invoice")
                                                {
                                                    if (ifitemlist.apply[ii].doc == Convert.ToInt64(invOffset))
                                                    {
                                                        if (ifitemlist.apply[ii].due >= invAmount)
                                                        {
                                                            ifitemlist.apply[ii].apply = true;
                                                            ifitemlist.apply[ii].applySpecified = true;
                                                            ifitems[0] = ifitemlist.apply[ii];
                                                        }
                                                        break;
                                                    }
                                                }
                                            }
                                            if (ifitems.Length == 0)
                                            {
                                                ifitems[0] = journalitems[0];
                                            }
                                        }

                                        CreditMemoApplyList ifil1 = new CreditMemoApplyList();

                                        ifil1.apply = ifitems;
                                        ifil1.replaceAll = false;
                                        inv.applyList = ifil1;
                                    }



                                    //Form 
                                    //RecordRef refForm = new RecordRef();
                                    //refForm.internalId = @Resource.TRADE_INVOICE_CUSTOMFORM_GMY;
                                    //inv.customForm = refForm;
                                    #endregion

                                    if (inv2.itemList != null)
                                    {
                                        invList[invCount] = inv;
                                        rowCount = invCount + 1;

                                        ////////////////////////////////////////////////////////////
                                        String refNo = "NETSUITE_CREDITMEMO.RETURNAUTHORIZE." + i.cot_trxInternalID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO, rnt_createdFromInternalID) values ('ADD', 'CPAS-RETURN AUTHORIZE CREDIT MEMO', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + i.cot_trxInternalID + "')";

                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        ////////////////////////////////////////////////////////////
                                        var updSalesTrx = "UPDATE cpas_otherstransaction SET cot_cnProgressStatus = '" + gjob_id.ToString() + "', cot_cnSeqNo = '" + rowCount + "' " +
                                                          "WHERE (cot_cnProgressStatus is NULL or cot_cnProgressStatus='') AND cot_recID = '" + i.cot_recID + "' ";

                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        ////////////////////////////////////////////////////////////

                                        invCount++;
                                        status = true;
                                    }
                                }
                                else
                                {
                                    //var updSalesTrx = "UPDATE cpas_salestransaction SET cst_invProgressStatus = 'NO RECORD FOUND' " +
                                    //                  "WHERE cst_invProgressStatus IS NULL AND cst_soInternalID '" + i.cot_trxInternalID + "' " +
                                    //                  "AND cst_ifUpdatedDate > '" + convertDateToString(rangeFrom) + "' " +
                                    //                  "AND cst_ifUpdatedDate <= '" + convertDateToString(rangeTo) + "'";
                                    //this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updSalesTrx);
                                    //entities.Database.ExecuteSqlCommand(updSalesTrx);
                                }

                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCreditMemo Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updSalesTrx = "UPDATE cpas_otherstransaction SET cot_cnProgressStatus = '" + jobID + "' WHERE cot_cnProgressStatus = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("CPASReturnAuthorizeCreditMemo: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASReturnAuthorizeCreditMemo Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASReturnAuthorizeCreditMemo: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }

        public Boolean CPASPaymentInst(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #831 */
            this.DataFromNetsuiteLog.Info("CPASPaymentInst *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                this.DataFromNetsuiteLog.Info("CPASPaymentInst : Start Login to NetSuite. ");
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASPaymentInst: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        String timeOutInd = string.Empty;

                        var qListMono = (from q1 in entities.cpas_payment
                                         where q1.cpm_createdDate > rangeFrom
                                         && q1.cpm_createdDate <= rangeTo
                                         && q1.cpm_subsidiary == "TH"
                                         && (q1.cpm_ProgressStatus == null || q1.cpm_ProgressStatus == "")
                                         && (!q1.cpm_salesPostingCat.Contains("CAD-CPAS"))
                                         && (!q1.cpm_salesPostingCat.Contains("COD-CPAS"))
                                         && (!q1.cpm_salesPostingCat.Contains("HS-CPAS"))
                                         select new
                                         {
                                             q1.cpm_recId,
                                             q1.cpm_contractYear,
                                             q1.cpm_contractMonth,
                                             q1.cpm_contractWeek,
                                             q1.cpm_subsidiary,
                                             q1.cpm_subsidiary_internalID,
                                             q1.cpm_businessChannel,
                                             q1.cpm_mb_businessChannel_internalID,
                                             q1.cpm_payment,
                                             q1.cpm_postingDate,
                                             q1.cpm_postingType,
                                             q1.cpm_salesPostingCat,
                                             q1.cpm_location,
                                             q1.cpm_ml_location_internalID
                                         }).ToList();

                        var qFilter = from p in qListMono
                                      let k = new
                                      {
                                          subsidiary = p.cpm_subsidiary,
                                          subsidiary_internalID = p.cpm_subsidiary_internalID,
                                          businessChannel = p.cpm_businessChannel,
                                          businessChannel_internalID = p.cpm_mb_businessChannel_internalID,
                                          location = p.cpm_location,
                                          location_internalID = p.cpm_ml_location_internalID,
                                          salesPostingCat = p.cpm_salesPostingCat
                                      }
                                      group p by k into g
                                      select new
                                      {
                                          subsidiary = g.Key.subsidiary,
                                          subsidiary_internalID = g.Key.subsidiary_internalID,
                                          businessChannel = g.Key.businessChannel,
                                          businessChannel_internalID = g.Key.businessChannel_internalID,
                                          location = g.Key.location,
                                          location_internalID = g.Key.location_internalID,
                                          salesPostingCat = g.Key.salesPostingCat,
                                          postingDate = g.Max(p => p.cpm_postingDate),
                                          payment = g.Sum(p => p.cpm_payment),
                                      };

                        var qFilterMono = (from rec in qFilter
                                           select rec).Take(150).ToList();

                        this.DataFromNetsuiteLog.Info("CPASPaymentInst:" + qFilterMono.Count() + " records to update.");
                        CustomerPayment[] invList = new CustomerPayment[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                string trxTypeVal = string.Empty;
                                string refInternalID = string.Empty;

                                this.DataFromNetsuiteLog.Info("CPASPaymentInst: Start Getting Invoice Internal ID from DB");
                                string _findSalesPostingCat = i.salesPostingCat.Substring(3, (i.salesPostingCat.Length - 3));
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == i.subsidiary_internalID
                                               && qTrx.cst_sDesc == _findSalesPostingCat
                                               && qTrx.cst_ml_location_internalID == i.location_internalID
                                               && qTrx.cst_salesType == "SALES-INST"
                                               select qTrx).FirstOrDefault();

                                this.DataFromNetsuiteLog.Info("CPASPaymentInst: Done getting Invoice Internal ID from DB");
                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }

                                //if (i.salesPostingCat.Contains("SALES 19")
                                //    || i.salesPostingCat.Contains("SALES 2000")
                                //    || i.salesPostingCat.Contains("SALES 2001")
                                //    || i.salesPostingCat.Contains("SALES 2002")
                                //    || i.salesPostingCat.Contains("SALES 2003")
                                //    || i.salesPostingCat.Contains("SALES 2004")
                                //    || i.salesPostingCat.Contains("SALES 2005")
                                //    || i.salesPostingCat.Contains("SALES 2006")
                                //    || i.salesPostingCat.Contains("SALES 2007")
                                //    || i.salesPostingCat.Contains("SALES 2008")
                                //    || i.salesPostingCat.Contains("SALES 2009")
                                //    || i.salesPostingCat.Contains("SALES 2010")
                                //    || i.salesPostingCat.Contains("SALES 2011")
                                //    || i.salesPostingCat.Contains("SALES 2012")
                                //    || i.salesPostingCat.Contains("SALES 2013")
                                //    || i.salesPostingCat.Contains("SALES 2014")
                                //    || i.salesPostingCat.Contains("SALES 20151")
                                //    || i.salesPostingCat.Contains("SALES 20152")
                                //    || i.salesPostingCat.Contains("SALES 20153")
                                //    || i.salesPostingCat.Contains("SALES 20154")
                                //    )
                                //{
                                //    refInternalID = @Resource.CPAS_INV_BEFORECUTOFFII_INTERNALID; //temp - to de decide the big SO
                                //}

                                this.DataFromNetsuiteLog.Info("CPASPaymentInst: Start connecting to NetSuite for Invoice Searching.");
                                //InitializeRef refSO = new InitializeRef();
                                //refSO.type = InitializeRefType.invoice;
                                //refSO.typeSpecified = true;
                                //refSO.internalId = refInternalID;

                                //InitializeRecord recSO = new InitializeRecord();
                                //recSO.type = InitializeType.customerPayment;
                                //recSO.reference = refSO;

                                //ReadResponse rrSO = service.initialize(recSO);
                                //Record rSO = rrSO.record;
                                //this.DataFromNetsuiteLog.Info("CPASPaymentInst: Done Invoice Searching.");

                                //CustomerPayment inv2 = (CustomerPayment)rSO;
                                CustomerPayment inv = new CustomerPayment();

                                this.DataFromNetsuiteLog.Info("CPASPaymentInst: Start apply payment.");
                                //if ((inv2 != null) && (i.payment>0))
                                //{ 
                                //    CustomerPaymentApplyList ifitemlist = inv2.applyList;
                                //    CustomerPaymentApply[] ifitems = new CustomerPaymentApply[ifitemlist.apply.Count()];
                                //    int count1 = 0;
                                //    //Boolean isInvoiceFound = false;

                                //    if (ifitemlist.apply.Count() > 0)
                                //    {
                                //        for (int ii = 0; ii < ifitemlist.apply.Length; ii++)
                                //        {
                                //            String invOffset = (refInternalID == null || refInternalID == "") ? @Resource.CPAS_TH_OPENJOURNAL_INTERNALID : refInternalID;
                                //            if (ifitemlist.apply[ii].doc == Convert.ToInt32(invOffset))
                                //            {
                                //                //isInvoiceFound = true;
                                //                ifitemlist.apply[ii].apply = true;
                                //                ifitemlist.apply[ii].applySpecified = true;

                                //                ifitemlist.apply[ii].amount = Double.Parse(i.payment.ToString());
                                //                ifitemlist.apply[ii].amountSpecified = true;
                                //            }
                                //            else
                                //            {
                                //                ifitemlist.apply[ii].apply = false;
                                //                ifitemlist.apply[ii].applySpecified = false;
                                //            }

                                //            ifitems[count1] = ifitemlist.apply[ii];
                                //            count1++;
                                //        }
                                //    }

                                //    //if (isInvoiceFound == true)
                                //    //{
                                //        //createdfrom 
                                //        //RecordRef refCreatedFrom = new RecordRef();
                                //        //refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                //        //inv.createdFrom = refCreatedFrom;

                                //        //RecordRef payMethod = new RecordRef();
                                //        //payMethod.internalId = "1"; // Add Credit Card information
                                //        //inv.paymentMethod = payMethod;

                                //    inv.customer = inv2.customer;
                                //    inv.subsidiary = inv2.subsidiary;

                                //    RecordRef refClass = new RecordRef();
                                //    refClass.internalId = i.businessChannel_internalID;
                                //    inv.@class = refClass;

                                //    inv.memo = i.salesPostingCat + "; " + i.location;

                                //    inv.tranDate = Convert.ToDateTime(i.postingDate).AddDays(-1);
                                //    inv.tranDateSpecified = true;

                                //    inv.undepFunds = false;
                                //    inv.undepFundsSpecified = true;

                                //    RecordRef refAccount = new RecordRef();
                                //    refAccount.internalId = @Resource.CPAS_TH_PAYMENT_DEFAULT_ACCTCODE;
                                //    inv.account = refAccount;

                                //    //inv.tranDate = inv2.tranDate; //DateTime.Now;
                                //    //inv.tranDateSpecified = true;

                                //    //inv.postingPeriod = inv2.postingPeriod;
                                //    //}

                                //    CustomerPaymentApplyList ifil1 = new CustomerPaymentApplyList();
                                //    ifil1.apply = ifitems;
                                //    ifil1.replaceAll = true;

                                //    inv.applyList = ifil1;
                                //}
                                //else if ((inv2 == null) && (i.payment > 0))
                                if (i.payment > 0)
                                {
                                    //createdfrom 
                                    //RecordRef refCreatedFrom = new RecordRef();
                                    //refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                    //inv.createdFrom = refCreatedFrom;

                                    //RecordRef payMethod = new RecordRef();
                                    //payMethod.internalId = "1"; // Add Credit Card information
                                    //inv.paymentMethod = payMethod;

                                    inv.payment = Double.Parse(i.payment.ToString());
                                    inv.paymentSpecified = true;

                                    RecordRef refEntity = new RecordRef();
                                    switch (i.subsidiary_internalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    inv.customer = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = i.businessChannel_internalID;
                                    inv.@class = refClass;

                                    //RecordRef refLocationSO = new RecordRef();
                                    //refLocationSO.internalId = i.location_internalID;
                                    //inv.location = refLocationSO;

                                    inv.memo = i.salesPostingCat + "; " + i.location + "; AutoApply";

                                    inv.autoApply = true;
                                    inv.autoApplySpecified = true;

                                    inv.tranDate = Convert.ToDateTime(i.postingDate).AddDays(-1);
                                    inv.tranDateSpecified = true;

                                    inv.undepFunds = false;
                                    inv.undepFundsSpecified = true;

                                    RecordRef refAccount = new RecordRef();
                                    refAccount.internalId = @Resource.CPAS_TH_PAYMENT_DEFAULT_ACCTCODE;
                                    inv.account = refAccount;

                                    RecordRef refSubsidairy = new RecordRef();
                                    refSubsidairy.internalId = i.subsidiary_internalID;
                                    inv.subsidiary = refSubsidairy;

                                    CustomerPaymentApply[] ifitems = new CustomerPaymentApply[1];
                                    CustomerPaymentApplyList ifil1 = new CustomerPaymentApplyList();
                                    ifil1.apply = ifitems;
                                    ifil1.replaceAll = false;

                                    inv.applyList = ifil1;
                                }

                                this.DataFromNetsuiteLog.Info("CPASPaymentInst: Done apply payment.");

                                if (i.payment > 0)
                                {
                                    invList[invCount] = inv;
                                    rowCount = invCount + 1;

                                    this.DataFromNetsuiteLog.Info("CPASPaymentInst: Start requestnetsuite_task record tracing.");
                                    ////////////////////////////////////////////////////////////
                                    String refNo = "NETSUITE_PAYMENT.SALESPOSTINGCAT." + i.salesPostingCat + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_createdFromInternalID) values ('ADD', 'CPAS-PAYMENT INST', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + refInternalID + "')";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    ////////////////////////////////////////////////////////////
                                    var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + gjob_id.ToString() + "', cpm_ProgressStatusSeqNo = '" + rowCount + "'" +
                                                        "WHERE (cpm_ProgressStatus is NULL or cpm_ProgressStatus='') AND cpm_subsidiary_internalID = '" + i.subsidiary_internalID + "' " +
                                                        "AND cpm_salesPostingCat = '" + i.salesPostingCat + "' " +
                                                        "AND cpm_ml_location_internalID = '" + i.location_internalID + "' AND cpm_createdDate > '" + convertDateToString(rangeFrom) + "' AND cpm_createdDate <= '" + convertDateToString(rangeTo) + "'";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    ////////////////////////////////////////////////////////////

                                    invCount++;
                                    status = true;
                                    this.DataFromNetsuiteLog.Info("CPASPaymentInst: Done requestnetsuite_task record tracing.");
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASPaymentInst Exception: " + ex.ToString());
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
                                    timeOutInd = "0";
                                    this.DataFromNetsuiteLog.Info("CPASPaymentInst: Start send JOB to NetSuite.");
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;
                                    timeOutInd = "1";
                                    this.DataFromNetsuiteLog.Info("CPASPaymentInst: Job Done and get Job ID = " + jobID + ")");
                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        timeOutInd = "2";
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        timeOutInd = "3";
                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT INST' and rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        timeOutInd = "4";
                                        var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + jobID + "' WHERE cpm_ProgressStatus = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        timeOutInd = "5";
                                        scope1.Complete();

                                        timeOutInd = "6";
                                    }
                                }
                                else
                                {
                                    timeOutInd = "4a";
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT INST' and rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    timeOutInd = "5a";
                                    scope1.Complete();
                                    timeOutInd = "6a";
                                }
                            }
                            else if (rowCount == 0)
                            {
                                timeOutInd = "4b";
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT INST' and rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("CPASPaymentInst: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                timeOutInd = "5b";
                                scope1.Complete();
                                timeOutInd = "6b";
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASPaymentInst Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; TimeoutIndicator = '" + timeOutInd + "' ;" + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASPaymentInst: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        public Boolean CPASPaymentCad(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #831 */
            this.DataFromNetsuiteLog.Info("CPASPaymentCad *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASPaymentCad: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.cpas_payment
                                         where q1.cpm_createdDate > rangeFrom
                                         && q1.cpm_createdDate <= rangeTo
                                         && q1.cpm_subsidiary == "TH"
                                         && (q1.cpm_ProgressStatus == null || q1.cpm_ProgressStatus == "")
                                         && (q1.cpm_salesPostingCat.Contains("CAD-CPAS"))
                                         select new
                                         {
                                             q1.cpm_recId,
                                             q1.cpm_contractYear,
                                             q1.cpm_contractMonth,
                                             q1.cpm_contractWeek,
                                             q1.cpm_subsidiary,
                                             q1.cpm_subsidiary_internalID,
                                             q1.cpm_businessChannel,
                                             q1.cpm_mb_businessChannel_internalID,
                                             q1.cpm_payment,
                                             q1.cpm_postingDate,
                                             q1.cpm_postingType,
                                             q1.cpm_salesPostingCat,
                                             q1.cpm_location,
                                             q1.cpm_ml_location_internalID
                                         }).ToList();

                        var qFilter = from p in qListMono
                                      let k = new
                                      {
                                          subsidiary = p.cpm_subsidiary,
                                          subsidiary_internalID = p.cpm_subsidiary_internalID,
                                          businessChannel = p.cpm_businessChannel,
                                          businessChannel_internalID = p.cpm_mb_businessChannel_internalID,
                                          location = p.cpm_location,
                                          location_internalID = p.cpm_ml_location_internalID,
                                          salesPostingCat = p.cpm_salesPostingCat
                                      }
                                      group p by k into g
                                      select new
                                      {
                                          subsidiary = g.Key.subsidiary,
                                          subsidiary_internalID = g.Key.subsidiary_internalID,
                                          businessChannel = g.Key.businessChannel,
                                          businessChannel_internalID = g.Key.businessChannel_internalID,
                                          location = g.Key.location,
                                          location_internalID = g.Key.location_internalID,
                                          salesPostingCat = g.Key.salesPostingCat,
                                          postingDate = g.Max(p => p.cpm_postingDate),
                                          payment = g.Sum(p => p.cpm_payment),
                                      };

                        var qFilterMono = (from rec in qFilter
                                           select rec).Take(150).ToList();

                        this.DataFromNetsuiteLog.Info("CPASPaymentCad:" + qFilterMono.Count() + " records to update.");
                        CustomerPayment[] invList = new CustomerPayment[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                string trxTypeVal = string.Empty;
                                string refInternalID = string.Empty;

                                string _findSalesPostingCat = i.salesPostingCat.Substring(3, (i.salesPostingCat.Length - 3));
                                var trxType = (from qTrx in entities.cpas_salestransaction
                                               where qTrx.cst_subsidiary_internalID == i.subsidiary_internalID
                                               && qTrx.cst_sDesc == _findSalesPostingCat
                                               && qTrx.cst_ml_location_internalID == i.location_internalID
                                               && qTrx.cst_salesType == "SALES-CAD"
                                               select qTrx).FirstOrDefault();

                                if (trxType != null)
                                {
                                    trxTypeVal = trxType.cst_salesType;
                                    refInternalID = trxType.cst_invInternalID;
                                }
                                else
                                {
                                    refInternalID = "";
                                }

                                //if (i.salesPostingCat.Contains("SALES 19")
                                //    || i.salesPostingCat.Contains("SALES 2000")
                                //    || i.salesPostingCat.Contains("SALES 2001")
                                //    || i.salesPostingCat.Contains("SALES 2002")
                                //    || i.salesPostingCat.Contains("SALES 2003")
                                //    || i.salesPostingCat.Contains("SALES 2004")
                                //    || i.salesPostingCat.Contains("SALES 2005")
                                //    || i.salesPostingCat.Contains("SALES 2006")
                                //    || i.salesPostingCat.Contains("SALES 2007")
                                //    || i.salesPostingCat.Contains("SALES 2008")
                                //    || i.salesPostingCat.Contains("SALES 2009")
                                //    || i.salesPostingCat.Contains("SALES 2010")
                                //    || i.salesPostingCat.Contains("SALES 2011")
                                //    || i.salesPostingCat.Contains("SALES 2012")
                                //    || i.salesPostingCat.Contains("SALES 2013")
                                //    || i.salesPostingCat.Contains("SALES 2014")
                                //    || i.salesPostingCat.Contains("SALES 20151")
                                //    || i.salesPostingCat.Contains("SALES 20152")
                                //    || i.salesPostingCat.Contains("SALES 20153")
                                //    || i.salesPostingCat.Contains("SALES 20154")
                                //    )
                                //{
                                //    refInternalID = @Resource.CPAS_INV_BEFORECUTOFFII_INTERNALID; //temp - to de decide the big SO
                                //}

                                InitializeRef refSO = new InitializeRef();
                                refSO.type = InitializeRefType.invoice;
                                refSO.typeSpecified = true;
                                refSO.internalId = refInternalID;

                                InitializeRecord recSO = new InitializeRecord();
                                recSO.type = InitializeType.customerPayment;
                                recSO.reference = refSO;

                                ReadResponse rrSO = service.initialize(recSO);
                                Record rSO = rrSO.record;

                                CustomerPayment inv2 = (CustomerPayment)rSO;
                                CustomerPayment inv = new CustomerPayment();

                                //if ((inv2 != null) && (i.payment > 0))
                                //{
                                //    CustomerPaymentApplyList ifitemlist = inv2.applyList;
                                //    CustomerPaymentApply[] ifitems = new CustomerPaymentApply[ifitemlist.apply.Count()];
                                //    int count1 = 0;
                                //    //Boolean isInvoiceFound = false;

                                //    if (ifitemlist.apply.Count() > 0)
                                //    {
                                //        for (int ii = 0; ii < ifitemlist.apply.Length; ii++)
                                //        {
                                //            String invOffset = (refInternalID == null || refInternalID == "") ? @Resource.CPAS_TH_OPENJOURNAL_INTERNALID : refInternalID;
                                //            if (ifitemlist.apply[ii].doc == Convert.ToInt32(invOffset))
                                //            {
                                //                //isInvoiceFound = true;
                                //                ifitemlist.apply[ii].apply = true;
                                //                ifitemlist.apply[ii].applySpecified = true;

                                //                ifitemlist.apply[ii].amount = Double.Parse(i.payment.ToString());
                                //                ifitemlist.apply[ii].amountSpecified = true;
                                //            }
                                //            else
                                //            {
                                //                ifitemlist.apply[ii].apply = false;
                                //                ifitemlist.apply[ii].applySpecified = false;
                                //            }

                                //            ifitems[count1] = ifitemlist.apply[ii];
                                //            count1++;
                                //        }
                                //    }

                                //    //if (isInvoiceFound == true)
                                //    //{
                                //    //createdfrom 
                                //    //RecordRef refCreatedFrom = new RecordRef();
                                //    //refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                //    //inv.createdFrom = refCreatedFrom;

                                //    //RecordRef payMethod = new RecordRef();
                                //    //payMethod.internalId = "1"; // Add Credit Card information
                                //    //inv.paymentMethod = payMethod;

                                //    inv.customer = inv2.customer;
                                //    inv.subsidiary = inv2.subsidiary;

                                //    RecordRef refClass = new RecordRef();
                                //    refClass.internalId = i.businessChannel_internalID;
                                //    inv.@class = refClass;

                                //    inv.memo = i.salesPostingCat + "; " + i.location;

                                //    inv.tranDate = Convert.ToDateTime(i.postingDate).AddDays(-1);
                                //    inv.tranDateSpecified = true;

                                //    inv.undepFunds = false;
                                //    inv.undepFundsSpecified = true;

                                //    RecordRef refAccount = new RecordRef();
                                //    refAccount.internalId = @Resource.CPAS_TH_PAYMENT_DEFAULT_ACCTCODE;
                                //    inv.account = refAccount;

                                //    //inv.tranDate = inv2.tranDate; //DateTime.Now;
                                //    //inv.tranDateSpecified = true;

                                //    //inv.postingPeriod = inv2.postingPeriod;
                                //    //}

                                //    CustomerPaymentApplyList ifil1 = new CustomerPaymentApplyList();
                                //    ifil1.apply = ifitems;
                                //    ifil1.replaceAll = true;

                                //    inv.applyList = ifil1;
                                //}
                                //else if ((inv2 == null) && (i.payment > 0))
                                if ((inv2 == null) && (i.payment > 0))
                                {
                                    //createdfrom 
                                    //RecordRef refCreatedFrom = new RecordRef();
                                    //refCreatedFrom.internalId = inv2.createdFrom.internalId;
                                    //inv.createdFrom = refCreatedFrom;

                                    //RecordRef payMethod = new RecordRef();
                                    //payMethod.internalId = "1"; // Add Credit Card information
                                    //inv.paymentMethod = payMethod;

                                    inv.payment = Double.Parse(i.payment.ToString());
                                    inv.paymentSpecified = true;

                                    RecordRef refEntity = new RecordRef();
                                    switch (i.subsidiary_internalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY_TRADE;
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_TRADE;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    inv.customer = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = i.businessChannel_internalID;
                                    inv.@class = refClass;

                                    //RecordRef refLocationSO = new RecordRef();
                                    //refLocationSO.internalId = i.location_internalID;
                                    //inv.location = refLocationSO;

                                    inv.memo = i.salesPostingCat + "; " + i.location + "; AutoApply";

                                    inv.autoApply = true;
                                    inv.autoApplySpecified = true;

                                    inv.tranDate = Convert.ToDateTime(i.postingDate).AddDays(-1);
                                    inv.tranDateSpecified = true;

                                    inv.undepFunds = false;
                                    inv.undepFundsSpecified = true;

                                    RecordRef refAccount = new RecordRef();
                                    refAccount.internalId = @Resource.CPAS_TH_PAYMENT_DEFAULT_ACCTCODE;
                                    inv.account = refAccount;

                                    RecordRef refSubsidairy = new RecordRef();
                                    refSubsidairy.internalId = i.subsidiary_internalID;
                                    inv.subsidiary = refSubsidairy;

                                    CustomerPaymentApply[] ifitems = new CustomerPaymentApply[1];
                                    CustomerPaymentApplyList ifil1 = new CustomerPaymentApplyList();
                                    ifil1.apply = ifitems;
                                    ifil1.replaceAll = false;

                                    inv.applyList = ifil1;
                                }

                                if (i.payment > 0)
                                {
                                    invList[invCount] = inv;
                                    rowCount = invCount + 1;

                                    ////////////////////////////////////////////////////////////
                                    String refNo = "NETSUITE_PAYMENT.SALESPOSTINGCAT." + i.salesPostingCat + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_createdFromInternalID) values ('ADD', 'CPAS-PAYMENT CAD', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + refInternalID + "')";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    ////////////////////////////////////////////////////////////
                                    var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + gjob_id.ToString() + "', cpm_ProgressStatusSeqNo = '" + rowCount + "'" +
                                                        "WHERE (cpm_ProgressStatus is NULL or cpm_ProgressStatus='') AND cpm_subsidiary_internalID = '" + i.subsidiary_internalID + "' " +
                                                        "AND cpm_salesPostingCat = '" + i.salesPostingCat + "' " +
                                                        "AND cpm_ml_location_internalID = '" + i.location_internalID + "' AND cpm_createdDate > '" + convertDateToString(rangeFrom) + "' AND cpm_createdDate <= '" + convertDateToString(rangeTo) + "'";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    ////////////////////////////////////////////////////////////

                                    invCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASPaymentCad Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT CAD' and rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + jobID + "' WHERE cpm_ProgressStatus = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT CAD' and rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT CAD' and rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("CPASPaymentCad: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASPaymentCad Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASPaymentCad: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        //kang added - payment for cpas COD
        public Boolean CPASPaymentCod(Int32 rn_id, DateTime rangeFrom, DateTime rangeTo)
        {
            /* #831 */
            this.DataFromNetsuiteLog.Info("CPASPaymentCod *****************");
            Boolean status = false;

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(2400)
            };

            using (TransactionScope scope1 = new TransactionScope())
            {
                Boolean loginStatus = login();
                if (loginStatus == true)
                {
                    this.DataFromNetsuiteLog.Info("CPASPaymentCod: Login Netsuite success.");
                    using (sdeEntities entities = new sdeEntities())
                    {
                        AsyncStatusResult job = new AsyncStatusResult();
                        Int32 invCount = 0;

                        Int32 rowCount = 0;
                        Guid gjob_id = Guid.NewGuid();

                        var qListMono = (from q1 in entities.cpas_payment
                                         where q1.cpm_createdDate > rangeFrom
                                         && q1.cpm_createdDate <= rangeTo
                                         && q1.cpm_subsidiary == "TH"
                                         && (q1.cpm_ProgressStatus == null || q1.cpm_ProgressStatus == "")
                                         && (q1.cpm_salesPostingCat.Contains("COD-CPAS"))
                                         select new
                                         {
                                             q1.cpm_recId,
                                             q1.cpm_contractYear,
                                             q1.cpm_contractMonth,
                                             q1.cpm_contractWeek,
                                             q1.cpm_subsidiary,
                                             q1.cpm_subsidiary_internalID,
                                             q1.cpm_businessChannel,
                                             q1.cpm_mb_businessChannel_internalID,
                                             q1.cpm_payment,
                                             q1.cpm_postingDate,
                                             q1.cpm_postingType,
                                             q1.cpm_salesPostingCat,
                                             q1.cpm_location,
                                             q1.cpm_ml_location_internalID
                                         }).ToList();

                        var qFilter = from p in qListMono
                                      let k = new
                                      {
                                          subsidiary = p.cpm_subsidiary,
                                          subsidiary_internalID = p.cpm_subsidiary_internalID,
                                          businessChannel = p.cpm_businessChannel,
                                          businessChannel_internalID = p.cpm_mb_businessChannel_internalID,
                                          location = p.cpm_location,
                                          location_internalID = p.cpm_ml_location_internalID,
                                          salesPostingCat = p.cpm_salesPostingCat
                                      }
                                      group p by k into g
                                      select new
                                      {
                                          subsidiary = g.Key.subsidiary,
                                          subsidiary_internalID = g.Key.subsidiary_internalID,
                                          businessChannel = g.Key.businessChannel,
                                          businessChannel_internalID = g.Key.businessChannel_internalID,
                                          location = g.Key.location,
                                          location_internalID = g.Key.location_internalID,
                                          salesPostingCat = g.Key.salesPostingCat,
                                          postingDate = g.Max(p => p.cpm_postingDate),
                                          payment = g.Sum(p => p.cpm_payment),
                                      };

                        var qFilterMono = (from rec in qFilter
                                           select rec).Take(150).ToList();

                        this.DataFromNetsuiteLog.Info("CPASPaymentCod:" + qFilterMono.Count() + " records to update.");
                        CustomerPayment[] invList = new CustomerPayment[qFilterMono.Count()];

                        foreach (var i in qFilterMono)
                        {
                            try
                            {
                                if (i.payment > 0)
                                {
                                    // create payment with auto apply
                                    CustomerPayment inv = new CustomerPayment();

                                    inv.payment = Double.Parse(i.payment.ToString());
                                    inv.paymentSpecified = true;

                                    RecordRef refEntity = new RecordRef();
                                    switch (i.subsidiary_internalID)
                                    {
                                        case "3"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_MY_CASH; //cpng
                                            break;
                                        case "5"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                            break;
                                        case "7"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_ID;
                                            break;
                                        case "6"://hard code
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_TH_CASH;
                                            break;
                                        case "9"://hard code - India
                                            refEntity.internalId = @Resource.CPAS_CUSTOMER_IN;
                                            break;
                                    }
                                    inv.customer = refEntity;

                                    RecordRef refClass = new RecordRef();
                                    refClass.internalId = i.businessChannel_internalID;
                                    inv.@class = refClass;

                                    inv.memo = i.salesPostingCat + "; " + i.location + "; AutoApply";

                                    inv.autoApply = true;
                                    inv.autoApplySpecified = true;

                                    inv.tranDate = Convert.ToDateTime(i.postingDate).AddDays(-1);
                                    inv.tranDateSpecified = true;

                                    inv.undepFunds = false;
                                    inv.undepFundsSpecified = true;

                                    RecordRef refAccount = new RecordRef();
                                    refAccount.internalId = @Resource.CPAS_TH_PAYMENT_DEFAULT_ACCTCODE;
                                    inv.account = refAccount;

                                    RecordRef refSubsidairy = new RecordRef();
                                    refSubsidairy.internalId = i.subsidiary_internalID;
                                    inv.subsidiary = refSubsidairy;

                                    CustomerPaymentApply[] ifitems = new CustomerPaymentApply[1];
                                    CustomerPaymentApplyList ifil1 = new CustomerPaymentApplyList();
                                    ifil1.apply = ifitems;
                                    ifil1.replaceAll = false;

                                    inv.applyList = ifil1;

                                    invList[invCount] = inv;
                                    rowCount = invCount + 1;

                                    ////////////////////////////////////////////////////////////
                                    String refNo = "NETSUITE_PAYMENT.SALESPOSTINGCAT." + i.salesPostingCat + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                    var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                        "rnt_seqNO, rnt_createdFromInternalID) values ('ADD', 'CPAS-PAYMENT COD', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                        "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + insertTask);
                                    entities.Database.ExecuteSqlCommand(insertTask);

                                    ////////////////////////////////////////////////////////////
                                    var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + gjob_id.ToString() + "', cpm_ProgressStatusSeqNo = '" + rowCount + "'" +
                                                        "WHERE (cpm_ProgressStatus is NULL or cpm_ProgressStatus='') AND cpm_subsidiary_internalID = '" + i.subsidiary_internalID + "' " +
                                                        "AND cpm_salesPostingCat = '" + i.salesPostingCat + "' " +
                                                        "AND cpm_ml_location_internalID = '" + i.location_internalID + "' AND cpm_createdDate > '" + convertDateToString(rangeFrom) + "' AND cpm_createdDate <= '" + convertDateToString(rangeTo) + "'";

                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updSalesTrx);
                                    entities.Database.ExecuteSqlCommand(updSalesTrx);

                                    ////////////////////////////////////////////////////////////

                                    invCount++;
                                    status = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                this.DataFromNetsuiteLog.Error("CPASPaymentCod Exception: " + ex.ToString());
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
                                    job = service.asyncAddList(invList);
                                    String jobID = job.jobId;

                                    if ((jobID != null) && (jobID.StartsWith("ASYNC") == true))
                                    {
                                        var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updateTask);
                                        entities.Database.ExecuteSqlCommand(updateTask);

                                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID = '" + jobID + "'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT COD' and rn_id = '" + rn_id + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updateRequestNetsuite);
                                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                        var updSalesTrx = "UPDATE cpas_payment SET cpm_ProgressStatus = '" + jobID + "' WHERE cpm_ProgressStatus = '" + gjob_id.ToString() + "' ";
                                        this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updSalesTrx);
                                        entities.Database.ExecuteSqlCommand(updSalesTrx);

                                        scope1.Complete();
                                    }
                                }
                                else
                                {
                                    var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                        "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT COD' and rn_id = '" + rn_id + "' ";
                                    this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updateRequestNetsuite);
                                    entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                    scope1.Complete();
                                }
                            }
                            else if (rowCount == 0)
                            {
                                var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
                                    "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-PAYMENT COD' and rn_id = '" + rn_id + "' ";
                                this.DataFromNetsuiteLog.Debug("CPASPaymentCod: " + updateRequestNetsuite);
                                entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

                                scope1.Complete();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.DataFromNetsuiteLog.Error("CPASPaymentCod Exception: rn_id= " + rn_id + ",rangeFrom = " + convertDateToString(rangeFrom) + ",rangeTo = " + convertDateToString(rangeTo) + "; " + ex.ToString());
                        }
                    }//end of sdeEntities
                }
                else
                {
                    this.DataFromNetsuiteLog.Fatal("CPASPaymentCod: Login Netsuite failed.");
                }
            }//end of scopeOuter
            logout();
            return status;
        }
        #endregion

        #region NOT USE
        //public Boolean CPASItemAdjustment(DateTime rangeFrom, DateTime rangeTo)
        //{
        //    this.DataFromNetsuiteLog.Info("CPASItemAdjustment ***************");
        //    Boolean status = false;
        //    var option = new TransactionOptions
        //    {
        //        IsolationLevel = IsolationLevel.RepeatableRead,
        //        Timeout = TimeSpan.FromSeconds(2400)
        //    };

        //    using (var scope1 = new TransactionScope(TransactionScopeOption.Required, option))
        //    {
        //        Boolean loginStatus = login();
        //        if (loginStatus == true)
        //        {
        //            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: Login Netsuite success.");
        //            using (sdeEntities entities = new sdeEntities())
        //            {
        //                AsyncStatusResult job = new AsyncStatusResult();
        //                Int32 daCount = 0;
        //                Int32 rowCount = 0;
        //                Guid gjob_id = Guid.NewGuid();

        //                try
        //                {
        //                    var groupQ1 = (from q1 in entities.cpas_dataposting
        //                                   //cpng start
        //                                   join b in entities.map_bin
        //                                   on q1.spl_ml_location_internalID equals b.mb_bin_internalID
        //                                   join m in entities.map_location
        //                                   on b.mb_bin_location_internalID equals m.ml_location_internalID
        //                                   //cpng end
        //                                   where (q1.spl_createdDate > rangeFrom && q1.spl_createdDate <= rangeTo)
        //                                   && q1.spl_subsidiary == "TH" //cpng
        //                                   && q1.spl_transactionType.Contains("ADJUSTMENT")
        //                                   select new
        //                                   {
        //                                       id = q1.spl_sp_id,
        //                                       tranType = q1.spl_transactionType,
        //                                       subsidiary = q1.spl_subsidiary_internalID,
        //                                       businessChannel = q1.spl_mb_businessChannel_internalID,
        //                                       memo = q1.spl_sDesc,
        //                                       postingDate = q1.spl_postingDate,
        //                                       rlocation_id = m.ml_location_internalID, //cpng
        //                                   }).Distinct().ToList();

        //                    //status = true;
        //                    InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];

        //                    foreach (var q1 in groupQ1)
        //                    {
        //                        InventoryAdjustment invAdj = new InventoryAdjustment();
        //                        InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

        //                        RecordRef refAccount = new RecordRef();
        //                        refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_REPLACEMENT;
        //                        invAdj.account = refAccount;

        //                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
        //                        StringCustomFieldRef scfr = new StringCustomFieldRef();
        //                        scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
        //                        scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
        //                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_REPLACEMENT;
        //                        cfrList[0] = scfr;
        //                        invAdj.customFieldList = cfrList;

        //                        RecordRef refSubsidiary = new RecordRef();
        //                        refSubsidiary.internalId = q1.subsidiary;
        //                        invAdj.subsidiary = refSubsidiary;

        //                        RecordRef refBusinessChannel = new RecordRef();
        //                        refBusinessChannel.internalId = q1.businessChannel;
        //                        invAdj.@class = refBusinessChannel;

        //                        invAdj.tranDate = Convert.ToDateTime(q1.postingDate).AddDays(-1);
        //                        invAdj.tranDateSpecified = true;
        //                        invAdj.memo = q1.memo;

        //                        //Set cost center to Finance & Accounting : Credit & Collections  - WY-30.OCT.2014
        //                        //RecordRef refCostCenter = new RecordRef();
        //                        //refCostCenter.internalId = @Resource.COSTCENTER_ACCOUNTING_INTERNALID;
        //                        //invAdj.department = refCostCenter; 

        //                        var ordAdj = (from o in entities.cpas_dataposting
        //                                      where o.spl_sp_id == q1.id
        //                                      && o.spl_transactionType == q1.tranType
        //                                      && o.spl_sDesc == q1.memo
        //                                      && (o.spl_createdDate > rangeFrom && o.spl_createdDate <= rangeTo)
        //                                      && ((o.spl_netsuiteProgress == null) || (o.spl_netsuiteProgress == ""))
        //                                      && (o.spl_mi_item_internalID != null)
        //                                      && (o.spl_mi_item_internalID != "DOWNPAYMENT")
        //                                      && (o.spl_mi_item_internalID != "VAT")
        //                                      select o).ToList();

        //                        var ordAdjItem = from p in ordAdj
        //                                         let k = new
        //                                         {
        //                                             item = p.spl_mi_item_internalID,
        //                                             tranType = p.spl_transactionType,
        //                                             loc = p.spl_ml_location_internalID,
        //                                             inOut = p.spl_inout
        //                                         }
        //                                         group p by k into g
        //                                         select new
        //                                         {
        //                                             item = g.Key.item,
        //                                             tranType = g.Key.tranType,
        //                                             loc = g.Key.loc,
        //                                             inout = g.Key.inOut,
        //                                             qty = g.Sum(p => p.spl_dQty)
        //                                         };

        //                        this.DataFromNetsuiteLog.Info("CPASItemAdjustment: " + ordAdjItem.Count() + " records to update.");

        //                        if (ordAdjItem.Count() > 0)
        //                        {
        //                            InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];

        //                            Int32 itemCount = 0;
        //                            foreach (var i in ordAdjItem)
        //                            {
        //                                InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

        //                                RecordRef refItem = new RecordRef();
        //                                refItem.internalId = i.item;
        //                                item.item = refItem;

        //                                RecordRef refLocation = new RecordRef();
        //                                refLocation.internalId = q1.rlocation_id; //cpng
        //                                item.location = refLocation;

        //                                if (i.inout.Equals("I"))
        //                                {
        //                                    item.adjustQtyBy = Convert.ToDouble(i.qty);
        //                                    //item.adjustQtyBy = 500;
        //                                }
        //                                else
        //                                {
        //                                    item.adjustQtyBy = -(Convert.ToDouble(i.qty));
        //                                    //item.adjustQtyBy = 500;
        //                                }
        //                                item.adjustQtyBySpecified = true;

        //                                //cpng start
        //                                InventoryAssignment[] IAA = new InventoryAssignment[1];
        //                                InventoryAssignment IA = new InventoryAssignment();
        //                                InventoryAssignmentList IAL = new InventoryAssignmentList();
        //                                InventoryDetail ID = new InventoryDetail();


        //                                if (i.inout.Equals("I"))
        //                                {
        //                                    IA.quantity = Convert.ToDouble(i.qty);
        //                                }
        //                                else
        //                                {
        //                                    IA.quantity = -Convert.ToDouble(i.qty);
        //                                }
        //                                IA.quantitySpecified = true;
        //                                IA.binNumber = new RecordRef { internalId = i.loc };
        //                                IAA[0] = IA;
        //                                IAL.inventoryAssignment = IAA;
        //                                ID.inventoryAssignmentList = IAL;

        //                                item.inventoryDetail = ID;
        //                                //cpng end

        //                                items[itemCount] = item;
        //                                itemCount++;
        //                            }
        //                            iail.inventory = items;
        //                            invAdj.inventoryList = iail;
        //                            invAdjList[daCount] = invAdj;

        //                            rowCount = daCount + 1;
        //                            var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
        //                                "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'CPAS-ITEM ADJUSTMENT', 'CPASDATAPOSTING.SPL_SP_ID." + q1.id + "', '" + gjob_id.ToString() + "'," +
        //                                "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
        //                            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + insertTask);
        //                            entities.Database.ExecuteSqlCommand(insertTask);

        //                            var updSalesTrx = "UPDATE cpas_dataposting SET spl_netsuiteProgress = '" + gjob_id.ToString() + "' " +
        //                                              "WHERE spl_sp_id= '" + q1.id + "'  AND spl_subsidiary_internalID = '" + q1.subsidiary + "' " +
        //                                              "AND spl_sDesc = '" + q1.memo + "' " +
        //                                              "AND (spl_transactionType = 'ADJUSTMENT') " +
        //                                              "AND (spl_netsuiteProgress is NULL or spl_netsuiteProgress= '')";
        //                            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + updSalesTrx);
        //                            entities.Database.ExecuteSqlCommand(updSalesTrx);

        //                            daCount++;
        //                            status = true;
        //                        }
        //                    }
        //                    if (status == true)
        //                    {
        //                        if (rowCount > 0)
        //                        {
        //                            job = service.asyncAddList(invAdjList);
        //                            String jobID = job.jobId;

        //                            var updateTask = "update requestnetsuite_task set rnt_jobID = '" + jobID + "' where rnt_jobID = '" + gjob_id.ToString() + "'";
        //                            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + updateTask);
        //                            entities.Database.ExecuteSqlCommand(updateTask);

        //                            var updateRequestNetsuite = "update requestnetsuite set rn_jobID='" + jobID + "'," +
        //                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-ITEM ADJUSTMENT' " +
        //                            "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
        //                            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + updateRequestNetsuite);
        //                            entities.Database.ExecuteSqlCommand(updateRequestNetsuite);

        //                            var updDataPost = "update cpas_dataposting set spl_netsuiteProgress = '" + jobID + "' where spl_netsuiteProgress = '" + gjob_id.ToString() + "'";
        //                            this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + updDataPost);
        //                            entities.Database.ExecuteSqlCommand(updDataPost);
        //                        }
        //                    }
        //                    else if (rowCount == 0)
        //                    {
        //                        var updateRequestNetsuite = "update requestnetsuite set rn_jobID='NO-DATA',rn_status='NO-DATA'," +
        //                            "rn_updatedDate = '" + convertDateToString(DateTime.Now) + "' where rn_sche_transactionType = 'CPAS-ITEM ADJUSTMENT' " +
        //                            "and rn_rangeTo = '" + convertDateToString(rangeTo) + "'";
        //                        this.DataFromNetsuiteLog.Debug("CPASItemAdjustment: " + updateRequestNetsuite);
        //                        entities.Database.ExecuteSqlCommand(updateRequestNetsuite);
        //                    }
        //                    scope1.Complete();
        //                }
        //                catch (Exception ex)
        //                {
        //                    this.DataFromNetsuiteLog.Error("CPASItemAdjustment Exception: " + ex.ToString());
        //                    status = false;
        //                }
        //            }//end of sdeEntities
        //        }
        //        else
        //        {
        //            this.DataFromNetsuiteLog.Fatal("CPASItemAdjustment: Login Netsuite failed.");
        //        }
        //    }//end of scope1
        //    logout();
        //    return status;
        //}
        #endregion

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

        public Boolean login()
        {
            service.Timeout = 820000000;
            service.CookieContainer = new CookieContainer();
            ApplicationInfo appinfo = new ApplicationInfo();
            appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
            service.applicationInfo = appinfo;

            Passport passport = new Passport();
            passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
            passport.email = @Resource.NETSUITE_LOGIN_EMAIL;

            RecordRef role = new RecordRef();
            role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

            passport.role = role;
            //kang get netsuite password from DB
            //passport.password = @Resource.NETSUITE_LOGIN_PASSWORD;
            passport.password = getNetsuitePassword();


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
                if (ex.Message.Contains("a session at a time"))
                {
                    this.DataFromNetsuiteLog.Debug(ex.ToString());
                }
                else if (ex.Message.Contains("Your connection has timed out"))
                {
                    this.DataFromNetsuiteLog.Debug(ex.ToString());
                }
                else
                {
                    this.DataFromNetsuiteLog.Error(ex.ToString());
                }
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