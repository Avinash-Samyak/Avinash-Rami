using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace sde.WCF_WMS
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
    }

    public struct JobMoAddress
    {
        public String jobMoAddrID, jobMoID, jobID, addrName, addr1, addr2, addr3, addrRef, moNo, contactName, deliveryType, addrTag, addrTel, addrTel2, addrFax;
    }

    public struct JobMoPack
    {
        public String jobMoPackID, jobID, period, moNo, schID, schName, packID, packTitles, packISBN, monoInternalID;
        public Int32 qty;
        public Double packPrice, amount;
    }

    public struct JobItem
    {
        public String jobItemID, jobID, createdBy, itemID, postingType, moNo, moNoInternalID;
        public DateTime createdDate;
        public Decimal itemQty;
    }

    public struct JobOrdMaster
    {
        public String ordMasterID, jobID, ordRecNo, ordStudent, clsID, moNo, consignmentNote, processPeriod, country, moNoInternalID;
        public DateTime scanDate;
    }

    public struct JobOrdMasterPack
    {
        public String ordMasterPackID, ordMasterID, jobID, ordNo, ordPack, ordReplace, ofrCode, status, ordPackStatus, skuCode, packTitle, ofrDesc;
        public Int32 ordQty, ordFulfill;
        public Double ordPrice, ordPoint;
        public DateTime ordDetDate;
    }

    public struct DiscountAndTax
    {
        public String wms_jobordmaster_ID, itemID, moNo, moNoInternalID, wms_jobordmaster_pack_id, wms_job_id;
        public Double? discount, tax, qty, price;
        public Int32? orderLine;
    }

    public struct ISSAC_FulfilledTransaction
    {
        public Decimal? dQty,dQty3;
        public String sPID,sProduct,sDesc,sCon,sBatch,sQty2,sLoc,sDesc2,businessChannel,Country,TransactionType;
        public DateTime? TransactionDate,SyncDate;
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
        public String jobordmaster_pack_id, status, posted_ind;
        public Int32 ordFulfill;
        public double ordPoint;
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
        public DateTime? rrDate, rrReturnDate;
        public Int32? rrStatus;
        public Boolean? rrActive;
    }

    public struct SOReturnItem
    {
        public String riID, rrID, riInvoice, riIsbn, riIsbn2, riCreatedBy, riRemarks, riItemID, riPackID;
        public DateTime? riCreatedDate;
        public Int32? riStatus, riReturnQty, riMaxReturn, riReceiveQty, riPostingQty;
    }

    public struct SOReturnUpdate
    {
        public List<SOReturn> sorList;
        public List<SOReturnItem> sriList;
    }
}
