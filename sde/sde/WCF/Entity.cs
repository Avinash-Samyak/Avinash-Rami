using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace sde.WCF
{
    public class Entity
    {
    }

    public class SchedulerEntity
    {
        public string sche_transactionType { get; set; }
        public string sche_status { get; set; }
        public string sche_minuteGap { get; set; }
        public Nullable<System.DateTime> sche_nextRun { get; set; }
        public Nullable<int> sche_nextRunSeqNo { get; set; }
        public Nullable<System.DateTime> sche_lastRun { get; set; }
        public Nullable<int> sche_lastRunSeqNo { get; set; }
        public Nullable<System.DateTime> sche_date { get; set; }
        public Nullable<int> sche_sequence { get; set; }

        //ANET-23 - Scheduler Control Handling
        //Added by Brash Developer on 16-Jul-2021
        public int sche_wait { get; set; }
    }

    public struct ExcessFulfillment
    {
        public String moNo, josp_packID, jobOrdMasterID, itemInternalID, locationInternalID, refNo;
        public Double? excessQty;
    }

    public struct RequestNetsuiteEntity
    {
        public String rn_sche_transactionType, rn_status, rn_jobID;
        public DateTime? rn_createdDate, rn_rangeFrom, rn_rangeTo;
    }

    public struct JOB
    {
        public String jobID, businessChannel, countryTag, createdBy, jobDesc, jobNo, jobStatus, modifiedBy;
        public Int32 moCount;
        public Boolean jobActive;
        public DateTime createdDate, modifiedDate;
    }

    public struct JobMO
    {
        public String jobID, moNo, jobMoID, consignmentNote, contactPerson, country, deliveryAdd, deliveryAdd2, deliveryAdd3, postCode, processPeriod, schID, schName, status,
            telNo, schName2, jobNo, creditTerm, moCurrency, deliveryType, moLisence, moTransMode, moNoInternalID;
        public Decimal ordWeight;
        public Int32 ordRecNoCnt, clsCnt;
        public DateTime createdDate;
        public DateTime? shipdate;
    }

    public struct JobMOCls
    {
        public String jobMoClsID, jobID, jobMoID, clsNo, teacherName;
        public DateTime createdDate, rangeTo;
    }

    public struct JobMoAddress
    {
        public String jobMoAddrID, jobMoID, jobID, addrName, addr1, addr2, addr3, addrRef, moNo, contactName, deliveryType, addrTag, addrTel, addrTel2, addrFax, addr4;
        public DateTime createdDate;
    }

    public struct JobMoPack
    {
        public String jobMoPackID, jobID, period, moNo, schID, schName, packID, packTitles, packISBN, monoInternalID;
        public Int32 recID,qty;
        public Double packPrice, amount;
        public DateTime createdDate, rangeTo;
    }

    public struct JobItem
    {
        public String jobItemID, jobID, createdBy, itemID, postingType, moNo, moNoInternalID;
        public DateTime createdDate, rangeTo;
        public Decimal itemQty;
    }

    public struct JobOrdMaster
    {
        public String ordMasterID, jobID, ordRecNo, ordStudent, clsID, moNo, consignmentNote, processPeriod, country, moNoInternalID, scanDate, jobMoID, memo, jobCls_id;
        public Int32 recID;
        public DateTime createdDate, rangeTo;
    }

    public struct JobOrdMasterPack
    {
        public String ordMasterPackID, ordMasterID, jobID, ordNo, ordPack, ordReplace, ofrCode, status, ordPackStatus, skuCode, packTitle, ofrDesc, moNo, priceLevel, taxCode;
        public Int32 ordQty, ordFulfill;
        public Double ordPrice, ordPoint, ordRate, tax, discount, basedPrice, gstAmount;
        public DateTime ordDetDate, createdDate, rangeTo;
    }

    public struct JobOrdMasterPackDetail
    {
        public String ordMasterPackDetailID, ordMasterPackID, jobID, ordPack, skuNo, isbn, isbnSecondary, itemID, taxCode;
        public Int32 skuQty, totalQty, scannedQty;
        public DateTime createdDate, rangeTo;
        public Double itemPrice, gstAmount, deliveryCharge, deliveryChargeGst;
    }

    public struct DiscountAndTax
    {
        public String wms_jobordmaster_ID, itemID, moNo, moNoInternalID, wms_jobordmaster_pack_id, wms_job_id, memo;
        public Double? discount, tax, qty, price;
        public Int32? orderLine;
    }

    //CPAS_StockPosting
    public struct CanceledItem //RETN,RNCO
    {
        public Decimal? dQty, dQty3;
        public String sPID, sProduct, extractBatch, sLoc, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate; 
    }

    //CPAS_StockPosting
    public struct PostingItem //SALES,UNSHIP
    {
        public Decimal? dQty;
        public String sPID, extractBatch, sLoc, inout, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate; 
    }

    public struct CPAS_JournalEntryMonthly
    {
        public String country, trans_type, journalType, postingDate;
        public Int32? month, year;
        public Decimal? total_price;
        public DateTime modifydate;
    }

    public struct CPAS_JournalEntryWeekly
    {
        public String country, transaction, journalType, postingDate;
        public Decimal? Amount;
        public Int32 week, year;
        public DateTime modifydate;
    }

    public struct TempSOList
    {
        public String moNo, businessChannel, subsidiary;
    }

    public struct JobOrdScan
    {
        public String jobOrdScanID, consignmentNote, countryTag, deliveryRef, jobID, jobMoID, ordRecNo, moNo, businessChannelID, businessChannelCode, recID, loadInd, doNo, jobOrdMasterID;
        public DateTime scanDate, exportDate, createdDate, rangeTo;
    }

    public struct JobOrdScanPack
    {
        public String jobordmaster_pack_id, status, posted_ind, moNo, jobID, ordRecNo;
        public Int32 ordFulfill;
        public Double ordPoint;
        public DateTime rangeTo, exportDate;
    }

    public struct BCASStockPosting
    {
        public String jobordmaster_pack_id, status, posted_ind, ordPack, businessChannel_internalID, subsidiary;
        public Int32 ordFulfill;
        public Double ordPoint, ordPrice;
        public DateTime rangeTo;
    }

    public struct PurchaseRequest
    {
        public String prID, prNumber, desc, requestor, site, comments, supplier, approvalType, accoutClass, createdBy, email, businessChannelID, deleteReason, deliveryMethod;
        public Boolean active, exported;
        public DateTime? date, neededDate, createdDate;
        public Int32? day, month, year, status;
    }

    public struct PurchaseRequestItem
    {
        public String pritemID, prID, itemID, itemBusinessID, approvedBy, comments;
        public Decimal? price;
        public Int32? qty;
        public Boolean converted, approved;
        public DateTime? approvedDate, createdDate;
    }

    public struct POReceive
    {
        public String porID, createdBy, modifiedBy, porDesc, porNumber, porInvoice, referenceID, prID;
        public DateTime? modifiedDate, createdDate, rangeTo;
        public Boolean active;
    }

    public struct POReceiveItem
    {
        public String poriID, itemID, locationCode, porID, priItemID;
        public DateTime? createdDate;
        public Int32 invItemQty, porItemQty, dmgQty;
        public long? sort;
    }

    public struct ExcessPO
    {
        public String prID, poreceiveID, poreceiveItemID, itemID, itemInternalID;
        public Double? excessQty;
    }

    public struct requestDataForm
    {
        public String dataType;
        public DateTime rangeFrom;
        public DateTime rangeTo;
    }

    public struct SOReturn
    {
        public String rrID, schID, rrNumber, rrDesc, rrCreatedBy, rrReference, rrReturnBy;
        public DateTime? rrDate, rrReturnDate, rrCreatedDate, rrRangeTo;
        public Int32? rrStatus;
        public Boolean? rrActive;
    }

    public struct SOReturnItem
    {
        public String riID, rrID, riInvoice, riIsbn, riIsbn2, riCreatedBy, riRemarks, riItemID, riPackID;
        public DateTime? riCreatedDate, riRangeTo;
        public Int32? riStatus, riReturnQty, riMaxReturn, riReceiveQty, riPostingQty;
    }

    public struct TempSO2
    {
        public String moNo, moNoInternalID, status, itemID, itemInternalID, customer, customerInternalID, SEISmoNo, SEISmoNoInternalID, subsidiary,
                      custID,addressee,deliveryAdd,deliveryAdd2,deliveryAdd3,postCode,contactPerson,phone,country,
                      billingAddressee, billingAdd, billingAdd2, billingAdd3, billingPostcode, billingContactPerson, billingPhone,
                      shipMethod, creditTerm, pricelevel, soDate, businessChannel, customerBooked, isFas, teacherName, className;
        public Int32 seqID, qtyForWMS, fulFilledQty,ordQty;
        public Double tax,discount,rate,amount,basedPrice;
        public DateTime createdDate, rangeTo;
    }

   
    public struct CashSales
    {
        public String adjID, adjCode, adjRemarks, adjWarehouse, adjDesc, adjType, adjCreatedBy, adjModifiedBy;
        public Boolean? adjActive, adjPosted;
        public DateTime? adjCreatedDate, adjModifiedDate, adjRangeTo;
    }

    public struct CashSalesItem
    {
        public String adjItemID, adjItemBusinessID, adjID, adjItemRemarks;
        public Int32 adjItemQty;
        public Boolean? adjItemStatus;
        public DateTime? adjCreatedDate, adjRangeTo;
    }

    //WY-03.NOV.2014
    public struct TempBackOrder
    {
        public String moNoInternalID;
    }

    public struct DuplicateItemInSO
    {
        public String moNo, moNoInternalID, item, subsidiary;
    }

    public struct DuplicateItemInPO
    {
        public String prID, prNo, prInternalID, item, subsidiary;
    }

    public class NewSO
    {
        //public String moNo { get; set; }
        //public String moNoInternalID { get; set; }
        //public String status { get; set; }
        //public String itemID { get; set; }
        public String itemInternalID { get; set; }
        //public String customer { get; set; }
        //public String customerInternalID { get; set; }
        //public String SEISmoNo { get; set; }
        //public String SEISmoNoInternalID { get; set; }
        //public String subsidiary { get; set; }
        //public Nullable<Int32> seqID { get; set; }
        public Nullable<Int32> ordQty { get; set; }
        public Nullable<Int32> committedQty { get; set; }
        //public Nullable<Int32> fulfilledQty { get; set; }
        public Nullable<Double> tax { get; set; }
        public Nullable<Double> discount { get; set; }
        public Nullable<Double> rate { get; set; }
        public Nullable<Double> amount { get; set; }
        public Nullable<Double> basedPrice { get; set; }
        public String pricelevel { get; set; }
        //public Nullable<DateTime> createdDate { get; set; }
        //public Nullable<DateTime> rangeTo { get; set; }
    }

    public class SyncSO
    {
        //public String moNo { get; set; }
        //public String moNoInternalID { get; set; }
        //public String status { get; set; }
        //public String itemID { get; set; }
        public String itemInternalID { get; set; }
        //public String customer { get; set; }
        //public String customerInternalID { get; set; }
        public String SEISmoNo { get; set; }
        //public String SEISmoNoInternalID { get; set; }
        //public String subsidiary { get; set; }
        //public Nullable<Int32> seqID { get; set; }
        public Nullable<Int32> ordQty { get; set; }
        public Nullable<Int32> committedQty { get; set; }
        //public Nullable<Int32> fulfilledQty { get; set; }
        public Nullable<Double> tax { get; set; }
        public Nullable<Double> discount { get; set; }
        public Nullable<Double> rate { get; set; }
        public Nullable<Double> amount { get; set; }
        public Nullable<Double> basedPrice { get; set; }
        public String pricelevel { get; set; }
        //public Nullable<DateTime> createdDate { get; set; }
        //public Nullable<DateTime> rangeTo { get; set; }  
        public String isfas { get; set; }
        public String className { get; set; }
        public String teacherName { get; set; }
        public String Mo_number { get; set; }
    }

    public class SyncSO2
    {
        //public String moNo { get; set; }
        //public String moNoInternalID { get; set; }
        //public String status { get; set; }
        //public String itemID { get; set; }
        public String itemInternalID { get; set; }
        //public String customer { get; set; }
        //public String customerInternalID { get; set; }
        public String SEISmoNo { get; set; }
        //public String SEISmoNoInternalID { get; set; }
        //public String subsidiary { get; set; }
        //public Nullable<Int32> seqID { get; set; }
        public Nullable<Int32> ordQty { get; set; }
        public Nullable<Int32> committedQty { get; set; }
        //public Nullable<Int32> fulfilledQty { get; set; }
        public Nullable<Double> tax { get; set; }
        public Nullable<Double> discount { get; set; }
        public Nullable<Double> rate { get; set; }
        public Nullable<Double> amount { get; set; }
        public Nullable<Double> basedPrice { get; set; }
        public String pricelevel { get; set; }
        public String isfas { get; set; }
        public String className { get; set; }
        public String teacherName { get; set; }
        public String Mo_number { get; set; }
        //public Nullable<DateTime> createdDate { get; set; }
        //public Nullable<DateTime> rangeTo { get; set; }  
    }


    /***    NETSUITE PHASE II (TRX WITH AMOUNT) ***/
    public struct PostingSales
    {
        public Decimal? dQty;
        public String sPID, extractBatch, sLoc, inout, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate;
        public String noOfInstallments, taxCode;
        public Decimal totalNettPrice, totalGstAmount, totalUFC, totalDeliveryCharges;
    }

    public struct PostingSalesContract
    {
        public Decimal? dQty;
        public String sPID, extractBatch, sLoc, inout, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate;
        public String noOfInstallments, taxCode;
        public Decimal totalWholePrice, totalDownPay, totalTHCDM, totalTHC, totalFinCharges, totalTax, totalGPM, totalGPMReserve, totalDeliveryCharges,
        totalTSP, totalCustPay, totalosBalance;
    }

    public struct PostingCancelContract
    {
        public Decimal? dQty;
        public String sPID, extractBatch, sLoc, inout, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate;
        public String noOfInstallments, taxCode, salesPostingCat, cancelType, suspendDate;
        public Decimal totalWholePrice, totalDownPay, totalTHCDM, totalTHC, totalFinCharges, totalTax, totalGPM, totalGPMReserve, totalDeliveryCharges,
        totalTSP, totalCustPay, totalosBalance, totalRevFinCharges, totalUFC;
    }

    public struct PostingCancellation
    {
        public Decimal? dQty, dQty3;
        public String sPID, sProduct, extractBatch, sLoc, businessChannel, Country, TransactionType, postingDate;
        public DateTime SyncDate;
        public String noOfInstallments, taxCode, salesPostingCat, cancelType, suspendDate;
        public Decimal totalNettPrice, totalGstAmount, totalUFC, totalDeliveryCharges, totalUnearnedInt, totalRevenueFinCharges;
    }
    public struct PostingPayment
    {
        public String  postingType, postingDate, priceCode, contractYear, contractMonth, contractWeek, subsidiary;
        public DateTime syncDate;
        public String salesPostingCat, location, businessChannel;
        public Decimal payment;
        public Int32 totalcontracts;
    }
}
