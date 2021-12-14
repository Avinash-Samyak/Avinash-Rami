using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace sde.WCF_SSA
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

        //ANET-23 - Scheduler Control Handling
        //Added by Brash Developer on 16-Jul-2021
        public int sche_wait { get; set; }
    }

    public struct RequestNetsuiteEntity
    {
        public String rn_sche_transactionType, rn_status, rn_jobID;
        public DateTime? rn_createdDate, rn_rangeFrom, rn_rangeTo;
    }

    public struct JOB
    {
        public String jobID, businessChannel, subsidiary, countryTag, createdBy, jobDesc, jobNo, jobStatus, modifiedBy;
        public Int32 moCount, status;
        public Boolean jobActive;
        public DateTime createdDate, modifiedDate, rangeTo;
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
        public Decimal ordPoint;
        public DateTime rangeTo, exportDate;
    }

    public struct TempList1
    {
        public String jobID, jobMoID;
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

    public struct CPAS_JournalEntryMonthly
    {
        public String country, tran_type;
        public Int32? month, year;
        public Decimal? total_price;
        public DateTime modifydate;
    }

    public struct CPAS_JournalEntryWeekly
    {
        public String country, transaction;
        public Decimal? Amount;
        public Int32? week, year;
        public DateTime modifydate;
    }

    public struct requestDataForm
    {
        public String dataType;
        public DateTime rangeFrom;
        public DateTime rangeTo;
    }

    public struct CashSales
    {
        public String adjID, adjCode, adjRemarks, adjWarehouse, adjDesc, adjType, adjCreatedBy, adjModifiedBy;
        public Boolean adjActive, adjPosted;
        public DateTime? adjCreatedDate, adjModifiedDate, adjRangeTo;
    }

    public struct CashSalesItem
    {
        public String adjItemID, adjItemBusinessID, adjID, adjItemRemarks;
        public Int32 adjItemQty;
        public Boolean adjItemStatus;
        public DateTime? adjCreatedDate, adjRangeTo;
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

    public struct DiscountAndTax
    {
        public String wms_jobordmaster_ID, itemID, moNo, moNoInternalID, wms_jobordmaster_pack_id, wms_job_id,memo;
        public Double? discount, tax, qty, price;
        public Int32? orderLine;
    }
}