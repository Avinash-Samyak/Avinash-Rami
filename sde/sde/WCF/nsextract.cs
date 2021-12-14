using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MySql.Data.MySqlClient;
using log4net;
using System.Transactions;
using sde.Models;
//using sde.comNetsuiteSandboxServices;
using sde.comNetsuiteServices;
using System.Net;
using System.Configuration;
using System.Collections;
using System.Security.Cryptography;

namespace sde.WCF
{
    public class nsextract
    {
        private readonly ILog DataFromNetsuiteLog = LogManager.GetLogger("DataFromNetsuite");    //#361
        private readonly ILog DataReqInMQLog = LogManager.GetLogger("DataReqInMQ");
     //   NetSuiteService service = new NetSuiteService();
        //synchronous runSynchronous = new synchronous();

        // TBA
        string account = @Resource.NETSUITE_LOGIN_ACCOUNT;
        string appID = @Resource.NETSUITE_LOGIN_APPLICATIONID;
        string consumerKey = @Resource.NETSUITE_Consumer_Key;
        string consumerSecret = @Resource.NETSUITE_Consumer_Secret;
        string tokenId, tokenSecret;

        #region Netsuite
        public string getNetsuitePassword(string loginEmail)
        {
            string returnPass = "";
            try
            {
                using (sdeEntities entities = new sdeEntities())
                {
                    var nsSetting = (from s in entities.netsuite_setting
                                     where s.nss_account == loginEmail
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

        public String TransactionAsyncSearch(NetSuiteService service, String jobID, RequestNetsuiteEntity r)
        {

            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);

            String tempJobID = null;
            if (String.IsNullOrEmpty(jobID))
            {
                tempJobID = "";
            }
            else
            {
                tempJobID = jobID;
            }

            this.DataFromNetsuiteLog.Info("PushNetsuite TransachtionAsyncSearch: " + r.rn_sche_transactionType + " (" + tempJobID + ")");
            using (sdeEntities entities = new sdeEntities())
            {
                try
                {
                    string loginEmail = "";
                    if (r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER") ||
                        r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 2") ||
                        r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 3") ||
                        r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 4") ||
                        r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 5") ||
                        r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 6"))
                    {
                        loginEmail = @Resource.NETSUITE_LOGIN_EMAIL_PULL;
                        tokenId = @Resource.ASIA_WEBSERVICE_4_TOKEN_ID;
                        tokenSecret = @Resource.ASIA_WEBSERVICE_4_TOKEN_SECRET;
                    }
                    else
                    {
                        loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                        tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                        tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;
                    }
                    /*
                    DateTime rangeFrom = Convert.ToDateTime(tempDateRangeFrom);
                    DateTime rangeTo = Convert.ToDateTime(tempDateRangeTo);
                    String dateRangeFrom = Convert.ToDateTime(tempDateRangeFrom).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                    String dateRangeTo = Convert.ToDateTime(tempDateRangeTo).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                    */

                    //TBA
                    ItemSearchBasic basic = new ItemSearchBasic()
                    {
                        internalId = new SearchMultiSelectField()
                        {
                            @operator = SearchMultiSelectFieldOperator.anyOf,
                            operatorSpecified = true,
                            searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                        }
                    };


                    DateTime rangeFrom = Convert.ToDateTime(r.rn_rangeFrom);
                    DateTime rangeTo = Convert.ToDateTime(r.rn_rangeTo);
                    String dateRangeFrom = Convert.ToDateTime(r.rn_rangeFrom).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                    String dateRangeTo = Convert.ToDateTime(r.rn_rangeTo).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                    AsyncStatusResult job = new AsyncStatusResult();

                    //////
                    //SearchPreferences sp = new SearchPreferences();
                    //sp.bodyFieldsOnly = false;
                    //sp.pageSize = 1000;
                    //sp.pageSizeSpecified = true;

                    //service.searchPreferences = sp;
                    //service.Timeout = 100000000;
                    //service.CookieContainer = new CookieContainer();
                    //ApplicationInfo appinfo = new ApplicationInfo();
                    //appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
                    //service.applicationInfo = appinfo;

                    //Passport passport = new Passport();
                    //passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
                    //passport.email = loginEmail;

                    //RecordRef role = new RecordRef();
                    //role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

                    //passport.role = role;
                    ////kang get netsuite password from DB
                    ////passport.password = @Resource.NETSUITE_LOGIN_PASSWORD;
                    //passport.password = getNetsuitePassword(loginEmail);

                    //Status status = service.login(passport).status;

                    SearchPreferences sp = new SearchPreferences();
                    sp.bodyFieldsOnly = false;
                    sp.pageSize = 1000;
                    sp.pageSizeSpecified = true;

                    netsuiteService.searchPreferences = sp;
                    netsuiteService.Timeout = 100000000;
                    netsuiteService.CookieContainer = new CookieContainer();
                    ApplicationInfo appinfo = new ApplicationInfo();
                    //  appinfo.applicationId = appID;
                    netsuiteService.applicationInfo = appinfo;

                    try
                    {
                        Console.WriteLine("Success");
                        netsuiteService.tokenPassport = createTokenPassport();
                        SearchResult status = netsuiteService.search(basic);
                        if (status.status.isSuccess == true)
                        {
                            this.DataFromNetsuiteLog.Debug("PushNetsuite TransactionAsyncSearch: Login Netsuite success.");
                            //if (String.IsNullOrEmpty(jobID))
                            //{
                            switch (r.rn_sche_transactionType)
                            {
                                #region Sales Order
                                #region NS-SALES ORDER
                                case "NS-SALES ORDER":
                                    //Extract once per day
                                    //must schedule an hour later than UPD-STATUS.NS-LATEST SALES ORDER

                                    //var query1 = (from q1 in entities.requestnetsuites
                                    //              where q1.rn_sche_transactionType == "UPD-STATUS.NS-LATEST SALES ORDER" && q1.rn_rangeTo == rangeTo && q1.rn_status.Contains("UPLOADED")
                                    //              select q1).ToList();

                                    String seisCustomer = @Resource.SEIS_CUSTOMER_MY;
                                    ////TH DROPSHIP
                                    String seisCustomerTH = @Resource.DROPSHIP_CUSTOMER_TH;
                                    String seisCustomerEDUGENERAL = @Resource.SEIS_CUSTOMER_MY_EDUGENERAL;
                                    var query2 = (from q2 in entities.netsuite_syncso
                                                  where q2.nt2_rangeTo > rangeFrom && q2.nt2_rangeTo <= rangeTo
                                                  && q2.nt2_customer != seisCustomer
                                                  && q2.nt2_customer != seisCustomerTH //TH DROPSHIP
                                                  && q2.nt2_customer != seisCustomerEDUGENERAL //TH DROPSHIP
                                                  && q2.nt2_progressStatus == null && q2.nt2_qtyForWMS > 0
                                                  select q2.nt2_moNo_internalID).Distinct().ToList();

                                    var query3 = (from q5 in entities.netsuite_syncso
                                                  join q4 in entities.netsuite_newso on q5.nt2_SEIS_moNo equals q4.nt1_moNo
                                                  where q5.nt2_rangeTo > rangeFrom && q5.nt2_rangeTo <= rangeTo
                                                      //&& q5.nt2_customer == seisCustomer && q5.nt2_SEIS_moNo != ""
                                                  && (q5.nt2_customer == seisCustomer || q5.nt2_customer == seisCustomerTH || q5.nt2_customer == seisCustomerEDUGENERAL) && q5.nt2_SEIS_moNo != "" //TH DROPSHIP
                                                  && q5.nt2_progressStatus == null && q5.nt2_qtyForWMS > 0
                                                  select q5.nt2_moNo_internalID).Distinct().ToList();

                                    var query4 = query2.Concat(query3).ToList();

                                    if (query4.Count() > 0)
                                    {
                                        this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                        TransactionSearchAdvanced sotsa = new TransactionSearchAdvanced();
                                        TransactionSearch sots = new TransactionSearch();
                                        TransactionSearchBasic sotsb = new TransactionSearchBasic();

                                        SearchEnumMultiSelectField soType = new SearchEnumMultiSelectField();
                                        soType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                        soType.operatorSpecified = true;
                                        soType.searchValue = new String[] { "_salesOrder" };
                                        sotsb.type = soType;

                                        RecordRef[] refInternalID = new RecordRef[query4.Count()];
                                        for (int i = 0; i < query4.Count(); i++)
                                        {
                                            RecordRef tempRef = new RecordRef();
                                            tempRef.internalId = query4[i];
                                            refInternalID[i] = tempRef;
                                        }

                                        /*
                                        var q33 = (from q in entities.map_businesschannel
                                                   where q.mb_imas_businessChannel_code == "ET"
                                                   select q.mb_businessChannel_internalID).FirstOrDefault();

                                        if (q33 != null)
                                        {
                                            refSOET.internalId = q33;//Trade or Book Clubs
                                        }*/

                                        //New search criteria - WY-10.OCT.2014
                                        RecordRef refSOET = new RecordRef();
                                        refSOET.internalId = @Resource.LOB_TRADE_INTERNALID;

                                        RecordRef refSOBC = new RecordRef();
                                        refSOBC.internalId = @Resource.LOB_BOOKCLUBS_INTERNALID;

                                        RecordRef refSOEDUDigital = new RecordRef();
                                        refSOEDUDigital.internalId = @Resource.LOB_EDUDIGITAL_INTERNALID;

                                        RecordRef refSOEDUEarly = new RecordRef();
                                        refSOEDUEarly.internalId = @Resource.LOB_EDUEARLY_INTERNALID;

                                        RecordRef refSOEDUFranchise = new RecordRef();
                                        refSOEDUFranchise.internalId = @Resource.LOB_EDUFRANCHISE_INTERNALID;

                                        RecordRef refSOEDUGeneral = new RecordRef();
                                        refSOEDUGeneral.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                        //Added Code for online store on 22-Mar-2021 by Brash Developer - START
                                        RecordRef refSOOnlineStore = new RecordRef();
                                        refSOOnlineStore.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                        SearchMultiSelectField soBusinessChannel = new SearchMultiSelectField();
                                        soBusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soBusinessChannel.operatorSpecified = true;
                                        soBusinessChannel.searchValue = new RecordRef[] { refSOET, refSOBC, refSOEDUDigital, refSOEDUEarly, refSOEDUFranchise, refSOEDUGeneral, refSOOnlineStore };
                                        sotsb.@class = soBusinessChannel;

                                        //END

                                        SearchMultiSelectField soInternalID = new SearchMultiSelectField();
                                        soInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soInternalID.operatorSpecified = true;
                                        soInternalID.searchValue = refInternalID;
                                        sotsb.internalId = soInternalID;

                                        //To exclude these 3 Customer - WY-10.OCT.2014
                                        RecordRef refBCASMY = new RecordRef();
                                        refBCASMY.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                        RecordRef refBCASID = new RecordRef();
                                        refBCASID.internalId = @Resource.CUST_BCASID_INTERNALID;

                                        RecordRef refBCASSG = new RecordRef();
                                        refBCASSG.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                        SearchMultiSelectField soBCASCust = new SearchMultiSelectField();
                                        soBCASCust.@operator = SearchMultiSelectFieldOperator.noneOf;
                                        soBCASCust.operatorSpecified = true;
                                        soBCASCust.searchValue = new RecordRef[] { refBCASMY, refBCASID, refBCASSG };
                                        sotsb.entity = soBCASCust;

                                        sots.basic = sotsb;
                                        sotsa.criteria = sots;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                           //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotsa);
                                            if (sr.recordList.Count() > 0)
                                            {
                                                //runSynchronous.SalesOrders(entities, r, sr);
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotsa);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion

                                #region NS-LATEST SALES ORDER
                                case "NS-LATEST SALES ORDER"://Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1.operatorSpecified = true;
                                    salesOrderStatus1.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed", "_salesOrderCancelled" };
                                    sotsb1.status = salesOrderStatus1;


                                    RecordRef refGMY = new RecordRef();
                                    refGMY.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS = new RecordRef();
                                    refSEIS.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH = new RecordRef();
                                    refTH.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    /*
                                    var q34 = (from q in entities.map_businesschannel
                                               where q.mb_imas_businessChannel_code == "ET"
                                               select q.mb_businessChannel_internalID).FirstOrDefault();

                                    if (q34 != null)
                                    {
                                        refSO1ET.internalId = q34;//Trade or Book Clubs
                                    }
                                    */

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET = new RecordRef();
                                    refSO1ET.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refSO1EDUGeneral = new RecordRef();
                                    refSO1EDUGeneral.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added Code for online store on 22-Mar-2021 by Brash Developer
                                    RecordRef refSO1OnlineStore = new RecordRef();
                                    refSO1OnlineStore.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    /* David Comment
                                    RecordRef refSO1BC = new RecordRef();
                                    refSO1BC.internalId = @Resource.LOB_BOOKCLUBS_INTERNALID;
                                
                                    RecordRef refSO1EDUDigital = new RecordRef();
                                    refSO1EDUDigital.internalId = @Resource.LOB_EDUDIGITAL_INTERNALID;
                                
                                    RecordRef refSO1EDUEarly = new RecordRef();
                                    refSO1EDUEarly.internalId = @Resource.LOB_EDUEARLY_INTERNALID;
                                
                                    RecordRef refSO1EDUFranchise = new RecordRef();
                                    refSO1EDUFranchise.internalId = @Resource.LOB_EDUFRANCHISE_INTERNALID;
                                    */

                                    SearchMultiSelectField so1BusinessChannel = new SearchMultiSelectField();
                                    so1BusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel.operatorSpecified = true;
                                    //so1BusinessChannel.searchValue = new RecordRef[] { refSO1ET, refSO1BC, refSO1EDUDigital, refSO1EDUEarly, refSO1EDUFranchise };  // David Comment
                                    so1BusinessChannel.searchValue = new RecordRef[] { refSO1ET, refSO1EDUGeneral, refSO1OnlineStore };
                                    sotsb1.@class = so1BusinessChannel;

                                    SearchMultiSelectField salesOrderSubsidiary1 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1.operatorSpecified = true;
                                    //salesOrderSubsidiary1.searchValue = new RecordRef[] { refGMY, refSEIS };
                                    //TH DROPSHIP
                                    salesOrderSubsidiary1.searchValue = new RecordRef[] { refGMY, refSEIS, refTH };
                                    sotsb1.subsidiary = salesOrderSubsidiary1;

                                    //To exclude these 3 Customer - WY-10.OCT.2014
                                    RecordRef refBCASMY1 = new RecordRef();
                                    refBCASMY1.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1 = new RecordRef();
                                    refBCASID1.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1 = new RecordRef();
                                    refBCASSG1.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip = new RecordRef();//P00892
                                    refDropShip.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    //TH DROPSHIP
                                    RecordRef refDropShipTH = new RecordRef();//P01042
                                    refDropShipTH.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    RecordRef refDropShipEDUGENERAL = new RecordRef();//P00892
                                    refDropShipEDUGENERAL.internalId = @Resource.SEIS_CUSTOMER_MY_EDUGENERAL_INTERNALID;

                                    SearchMultiSelectField soBCASCust1 = new SearchMultiSelectField();
                                    soBCASCust1.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1.operatorSpecified = true;
                                    //soBCASCust1.searchValue = new RecordRef[] { refDropShip }; 
                                    //TH DROPSHIP
                                    soBCASCust1.searchValue = new RecordRef[] { refDropShip, refDropShipTH, refDropShipEDUGENERAL };
                                    sotsb1.entity = soBCASCust1;

                                    //Change filter when insert into netsuite_syncso - WY-25.AUG.2014
                                    /*
                                    SearchMultiSelectCustomField soSync = new SearchMultiSelectCustomField();
                                    soSync.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    soSync.operatorSpecified = true;
                                    soSync.scriptId = "custbody_wms_field";
                                    ListOrRecordRef soListOrRecordRef = new ListOrRecordRef();
                                    soListOrRecordRef.internalId = "1";
                                    soListOrRecordRef.typeId = "136";
                                    soSync.searchValue = new ListOrRecordRef[] { soListOrRecordRef };
                                    SearchCustomField[] soScf = new SearchCustomField[] { soSync };
                                    sotsb1.customFieldList = soScf;
                                    */

                                    /*
                                    SearchEnumMultiSelectField salesOrderCommitType = new SearchEnumMultiSelectField();
                                    salesOrderCommitType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderCommitType.operatorSpecified = true;
                                    salesOrderCommitType.searchValue = new String[] { "_availableQty", "_completeQty" };
                                    sotsb.commit = salesOrderCommitType;

                                    SearchDoubleField salesOrderCommitted = new SearchDoubleField();
                                    salesOrderCommitted.@operator = SearchDoubleFieldOperator.greaterThan;
                                    salesOrderCommitted.operatorSpecified = true;
                                    salesOrderCommitted.searchValue = 0;
                                    salesOrderCommitted.searchValueSpecified = true;
                                    sotsb.quantityCommitted = salesOrderCommitted;
                                    */

                                    SearchDateField salesOrderDate1 = new SearchDateField();
                                    salesOrderDate1.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1.operatorSpecified = true;
                                    salesOrderDate1.searchValueSpecified = true;
                                    salesOrderDate1.searchValue2Specified = true;
                                    salesOrderDate1.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1.lastModifiedDate = salesOrderDate1;

                                    ////Int32 count = 2;
                                    ////RecordRef[] refTempInternalID = new RecordRef[count];
                                    ////for (int i = 0; i < count; i++)
                                    ////{
                                    ////    RecordRef tempRefIn = new RecordRef();
                                    ////    if (i == 0)
                                    ////    {
                                    ////        tempRefIn.internalId = "114744";
                                    ////    }
                                    ////    else
                                    ////    {
                                    ////        tempRefIn.internalId = "108935"; 
                                    ////    }
                                    ////    refTempInternalID[i] = tempRefIn;
                                    ////}

                                    ////SearchMultiSelectField soTempInternalID = new SearchMultiSelectField();
                                    ////soTempInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    ////soTempInternalID.operatorSpecified = true;
                                    ////soTempInternalID.searchValue = refTempInternalID;
                                    ////sotsb1.internalId = soTempInternalID;

                                    sots1.basic = sotsb1;
                                    sotsa1.criteria = sots1;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.LatestSalesOrders(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //SearchResult sr = service.search(sotsa1);
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion

                                ///// solved timeout issue
                                #region NS-LATEST SALES ORDER_1_40
                                case "NS-LATEST SALES ORDER_1_40": //Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1_40 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1_40 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1_40 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1_40 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1_40.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1_40.operatorSpecified = true;
                                    salesOrderStatus1_40.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed" };
                                    sotsb1_40.status = salesOrderStatus1_40;


                                    RecordRef refGMY_40 = new RecordRef();
                                    refGMY_40.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS_40 = new RecordRef();
                                    refSEIS_40.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH_40 = new RecordRef();
                                    refTH_40.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET_40 = new RecordRef();
                                    refSO1ET_40.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel_40 = new SearchMultiSelectField();
                                    so1BusinessChannel_40.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel_40.operatorSpecified = true;
                                    so1BusinessChannel_40.searchValue = new RecordRef[] { refSO1ET_40 };
                                    sotsb1_40.@class = so1BusinessChannel_40;

                                    SearchMultiSelectField salesOrderSubsidiary1_40 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1_40.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1_40.operatorSpecified = true;
                                    salesOrderSubsidiary1_40.searchValue = new RecordRef[] { refGMY_40, refSEIS_40, refTH_40 };
                                    sotsb1_40.subsidiary = salesOrderSubsidiary1_40;

                                    RecordRef refBCASMY1_40 = new RecordRef();
                                    refBCASMY1_40.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1_40 = new RecordRef();
                                    refBCASID1_40.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1_40 = new RecordRef();
                                    refBCASSG1_40.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip_40 = new RecordRef();//P00892
                                    refDropShip_40.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    RecordRef refDropShipTH_40 = new RecordRef();//P01042
                                    refDropShipTH_40.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    SearchMultiSelectField soBCASCust1_40 = new SearchMultiSelectField();
                                    soBCASCust1_40.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1_40.operatorSpecified = true;
                                    soBCASCust1_40.searchValue = new RecordRef[] { refDropShip_40, refDropShipTH_40 };
                                    sotsb1_40.entity = soBCASCust1_40;

                                    SearchDateField salesOrderDate1_40 = new SearchDateField();
                                    salesOrderDate1_40.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1_40.operatorSpecified = true;
                                    salesOrderDate1_40.searchValueSpecified = true;
                                    salesOrderDate1_40.searchValue2Specified = true;
                                    salesOrderDate1_40.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1_40.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1_40.lastModifiedDate = salesOrderDate1_40;

                                    sots1_40.basic = sotsb1_40;
                                    sotsa1_40.criteria = sots1_40;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1_40);
                                        if (sr.recordList.Count() > 0)
                                        {
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1_40);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region NS-LATEST SALES ORDER_41_80
                                case "NS-LATEST SALES ORDER_41_80": //Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1_80 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1_80 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1_80 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1_80 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1_80.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1_80.operatorSpecified = true;
                                    salesOrderStatus1_80.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed" };
                                    sotsb1_80.status = salesOrderStatus1_80;


                                    RecordRef refGMY_80 = new RecordRef();
                                    refGMY_80.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS_80 = new RecordRef();
                                    refSEIS_80.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH_80 = new RecordRef();
                                    refTH_80.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET_80 = new RecordRef();
                                    refSO1ET_80.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel_80 = new SearchMultiSelectField();
                                    so1BusinessChannel_80.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel_80.operatorSpecified = true;
                                    so1BusinessChannel_80.searchValue = new RecordRef[] { refSO1ET_80 };
                                    sotsb1_80.@class = so1BusinessChannel_80;

                                    SearchMultiSelectField salesOrderSubsidiary1_80 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1_80.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1_80.operatorSpecified = true;
                                    salesOrderSubsidiary1_80.searchValue = new RecordRef[] { refGMY_80, refSEIS_80, refTH_80 };
                                    sotsb1_80.subsidiary = salesOrderSubsidiary1_80;

                                    RecordRef refBCASMY1_80 = new RecordRef();
                                    refBCASMY1_80.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1_80 = new RecordRef();
                                    refBCASID1_80.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1_80 = new RecordRef();
                                    refBCASSG1_80.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip_80 = new RecordRef();//P00892
                                    refDropShip_80.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    RecordRef refDropShipTH_80 = new RecordRef();//P01042
                                    refDropShipTH_80.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    SearchMultiSelectField soBCASCust1_80 = new SearchMultiSelectField();
                                    soBCASCust1_80.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1_80.operatorSpecified = true;
                                    soBCASCust1_80.searchValue = new RecordRef[] { refDropShip_80, refDropShipTH_80 };
                                    sotsb1_80.entity = soBCASCust1_80;

                                    SearchDateField salesOrderDate1_80 = new SearchDateField();
                                    salesOrderDate1_80.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1_80.operatorSpecified = true;
                                    salesOrderDate1_80.searchValueSpecified = true;
                                    salesOrderDate1_80.searchValue2Specified = true;
                                    salesOrderDate1_80.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1_80.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1_80.lastModifiedDate = salesOrderDate1_80;

                                    sots1_80.basic = sotsb1_80;
                                    sotsa1_80.criteria = sots1_80;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1_80);
                                        if (sr.recordList.Count() > 0)
                                        {
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1_80);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region NS-LATEST SALES ORDER_81_120
                                case "NS-LATEST SALES ORDER_81_120": //Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1_120 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1_120 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1_120 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1_120 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1_120.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1_120.operatorSpecified = true;
                                    salesOrderStatus1_120.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed" };
                                    sotsb1_120.status = salesOrderStatus1_120;


                                    RecordRef refGMY_120 = new RecordRef();
                                    refGMY_120.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS_120 = new RecordRef();
                                    refSEIS_120.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH_120 = new RecordRef();
                                    refTH_120.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET_120 = new RecordRef();
                                    refSO1ET_120.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel_120 = new SearchMultiSelectField();
                                    so1BusinessChannel_120.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel_120.operatorSpecified = true;
                                    so1BusinessChannel_120.searchValue = new RecordRef[] { refSO1ET_120 };
                                    sotsb1_120.@class = so1BusinessChannel_120;

                                    SearchMultiSelectField salesOrderSubsidiary1_120 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1_120.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1_120.operatorSpecified = true;
                                    salesOrderSubsidiary1_120.searchValue = new RecordRef[] { refGMY_120, refSEIS_120, refTH_120 };
                                    sotsb1_120.subsidiary = salesOrderSubsidiary1_120;

                                    RecordRef refBCASMY1_120 = new RecordRef();
                                    refBCASMY1_120.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1_120 = new RecordRef();
                                    refBCASID1_120.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1_120 = new RecordRef();
                                    refBCASSG1_120.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip_120 = new RecordRef();//P00892
                                    refDropShip_120.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    RecordRef refDropShipTH_120 = new RecordRef();//P01042
                                    refDropShipTH_120.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    SearchMultiSelectField soBCASCust1_120 = new SearchMultiSelectField();
                                    soBCASCust1_120.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1_120.operatorSpecified = true;
                                    soBCASCust1_120.searchValue = new RecordRef[] { refDropShip_120, refDropShipTH_120 };
                                    sotsb1_120.entity = soBCASCust1_120;

                                    SearchDateField salesOrderDate1_120 = new SearchDateField();
                                    salesOrderDate1_120.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1_120.operatorSpecified = true;
                                    salesOrderDate1_120.searchValueSpecified = true;
                                    salesOrderDate1_120.searchValue2Specified = true;
                                    salesOrderDate1_120.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1_120.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1_120.lastModifiedDate = salesOrderDate1_120;

                                    sots1_120.basic = sotsb1_120;
                                    sotsa1_120.criteria = sots1_120;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1_120);
                                        if (sr.recordList.Count() > 0)
                                        {
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1_120);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region NS-LATEST SALES ORDER_121_160
                                case "NS-LATEST SALES ORDER_121_160": //Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1_160 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1_160 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1_160 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1_160 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1_160.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1_160.operatorSpecified = true;
                                    salesOrderStatus1_160.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed" };
                                    sotsb1_160.status = salesOrderStatus1_160;


                                    RecordRef refGMY_160 = new RecordRef();
                                    refGMY_160.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS_160 = new RecordRef();
                                    refSEIS_160.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH_160 = new RecordRef();
                                    refTH_160.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET_160 = new RecordRef();
                                    refSO1ET_160.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel_160 = new SearchMultiSelectField();
                                    so1BusinessChannel_160.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel_160.operatorSpecified = true;
                                    so1BusinessChannel_160.searchValue = new RecordRef[] { refSO1ET_160 };
                                    sotsb1_160.@class = so1BusinessChannel_160;

                                    SearchMultiSelectField salesOrderSubsidiary1_160 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1_160.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1_160.operatorSpecified = true;
                                    salesOrderSubsidiary1_160.searchValue = new RecordRef[] { refGMY_160, refSEIS_160, refTH_160 };
                                    sotsb1_160.subsidiary = salesOrderSubsidiary1_160;

                                    RecordRef refBCASMY1_160 = new RecordRef();
                                    refBCASMY1_160.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1_160 = new RecordRef();
                                    refBCASID1_160.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1_160 = new RecordRef();
                                    refBCASSG1_160.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip_160 = new RecordRef();//P00892
                                    refDropShip_160.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    RecordRef refDropShipTH_160 = new RecordRef();//P01042
                                    refDropShipTH_160.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    SearchMultiSelectField soBCASCust1_160 = new SearchMultiSelectField();
                                    soBCASCust1_160.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1_160.operatorSpecified = true;
                                    soBCASCust1_160.searchValue = new RecordRef[] { refDropShip_160, refDropShipTH_160 };
                                    sotsb1_160.entity = soBCASCust1_160;

                                    SearchDateField salesOrderDate1_160 = new SearchDateField();
                                    salesOrderDate1_160.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1_160.operatorSpecified = true;
                                    salesOrderDate1_160.searchValueSpecified = true;
                                    salesOrderDate1_160.searchValue2Specified = true;
                                    salesOrderDate1_160.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1_160.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1_160.lastModifiedDate = salesOrderDate1_160;

                                    sots1_160.basic = sotsb1_160;
                                    sotsa1_160.criteria = sots1_160;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1_160);
                                        if (sr.recordList.Count() > 0)
                                        {
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1_160);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region NS-LATEST SALES ORDER_161_9999
                                case "NS-LATEST SALES ORDER_161_9999": //Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa1_9999 = new TransactionSearchAdvanced();
                                    TransactionSearch sots1_9999 = new TransactionSearch();
                                    TransactionSearchBasic sotsb1_9999 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus1_9999 = new SearchEnumMultiSelectField();
                                    salesOrderStatus1_9999.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus1_9999.operatorSpecified = true;
                                    salesOrderStatus1_9999.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed" };
                                    sotsb1_9999.status = salesOrderStatus1_9999;


                                    RecordRef refGMY_9999 = new RecordRef();
                                    refGMY_9999.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS_9999 = new RecordRef();
                                    refSEIS_9999.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH_9999 = new RecordRef();
                                    refTH_9999.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET_9999 = new RecordRef();
                                    refSO1ET_9999.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel_9999 = new SearchMultiSelectField();
                                    so1BusinessChannel_9999.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel_9999.operatorSpecified = true;
                                    so1BusinessChannel_9999.searchValue = new RecordRef[] { refSO1ET_9999 };
                                    sotsb1_9999.@class = so1BusinessChannel_9999;

                                    SearchMultiSelectField salesOrderSubsidiary1_9999 = new SearchMultiSelectField();
                                    salesOrderSubsidiary1_9999.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary1_9999.operatorSpecified = true;
                                    salesOrderSubsidiary1_9999.searchValue = new RecordRef[] { refGMY_9999, refSEIS_9999, refTH_9999 };
                                    sotsb1_9999.subsidiary = salesOrderSubsidiary1_9999;

                                    RecordRef refBCASMY1_9999 = new RecordRef();
                                    refBCASMY1_9999.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID1_9999 = new RecordRef();
                                    refBCASID1_9999.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG1_9999 = new RecordRef();
                                    refBCASSG1_9999.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    RecordRef refDropShip_9999 = new RecordRef();//P00892
                                    refDropShip_9999.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    RecordRef refDropShipTH_9999 = new RecordRef();//P01042
                                    refDropShipTH_9999.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    SearchMultiSelectField soBCASCust1_9999 = new SearchMultiSelectField();
                                    soBCASCust1_9999.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust1_9999.operatorSpecified = true;
                                    soBCASCust1_9999.searchValue = new RecordRef[] { refDropShip_9999, refDropShipTH_9999 };
                                    sotsb1_9999.entity = soBCASCust1_9999;

                                    SearchDateField salesOrderDate1_9999 = new SearchDateField();
                                    salesOrderDate1_9999.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate1_9999.operatorSpecified = true;
                                    salesOrderDate1_9999.searchValueSpecified = true;
                                    salesOrderDate1_9999.searchValue2Specified = true;
                                    salesOrderDate1_9999.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate1_9999.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb1_9999.lastModifiedDate = salesOrderDate1_9999;

                                    sots1_9999.basic = sotsb1_9999;
                                    sotsa1_9999.criteria = sots1_9999;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa1_9999);
                                        if (sr.recordList.Count() > 0)
                                        {
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa1_9999);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                /////

                                //To handle back order case with no update last modified date status = PartialFulfillment and PendingBilling Partial Fulfillment- WY-30.SEPT.2014
                                #region NS-LATEST SALES ORDER 2
                                case "NS-LATEST SALES ORDER 2"://Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER 2 within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa2 = new TransactionSearchAdvanced();
                                    TransactionSearch sots2 = new TransactionSearch();
                                    TransactionSearchBasic sotsb2 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus2 = new SearchEnumMultiSelectField();
                                    salesOrderStatus2.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus2.operatorSpecified = true;
                                    salesOrderStatus2.searchValue = new String[] { "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled" };
                                    sotsb2.status = salesOrderStatus2;

                                    RecordRef refGMY2 = new RecordRef();
                                    refGMY2.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS2 = new RecordRef();
                                    refSEIS2.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH2 = new RecordRef();
                                    refTH2.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    /*
                                    var qBiz = (from q in entities.map_businesschannel
                                               where q.mb_imas_businessChannel_code == "ET"
                                               select q.mb_businessChannel_internalID).FirstOrDefault();

                                    if (qBiz != null)
                                    {
                                        refSO1ET2.internalId = qBiz;//Trade or Book Clubs
                                    }*/

                                    //New search criteria - WY-10.OCT.2014
                                    RecordRef refSO1ET2 = new RecordRef();
                                    refSO1ET2.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refSOEDUGeneral2 = new RecordRef();
                                    refSOEDUGeneral2.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added Code for online store on 22-Mar-2021 by Brash Developer
                                    RecordRef refSOOnlineStore2 = new RecordRef();
                                    refSOOnlineStore2.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    /* David Comment
                                    RecordRef refSO1BC2 = new RecordRef();
                                    refSO1BC2.internalId = @Resource.LOB_BOOKCLUBS_INTERNALID;

                                    RecordRef refSO1EDUDigital2 = new RecordRef();
                                    refSO1EDUDigital2.internalId = @Resource.LOB_EDUDIGITAL_INTERNALID;

                                    RecordRef refSO1EDUEarly2 = new RecordRef();
                                    refSO1EDUEarly2.internalId = @Resource.LOB_EDUEARLY_INTERNALID;

                                    RecordRef refSO1EDUFranchise2 = new RecordRef();
                                    refSO1EDUFranchise2.internalId = @Resource.LOB_EDUFRANCHISE_INTERNALID;
                                    */

                                    SearchMultiSelectField so2BusinessChannel = new SearchMultiSelectField();
                                    so2BusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so2BusinessChannel.operatorSpecified = true;
                                    //so2BusinessChannel.searchValue = new RecordRef[] { refSO1ET2, refSO1BC2, refSO1EDUDigital2, refSO1EDUEarly2, refSO1EDUFranchise2 };
                                    so2BusinessChannel.searchValue = new RecordRef[] { refSO1ET2, refSOEDUGeneral2, refSOOnlineStore2 };
                                    sotsb2.@class = so2BusinessChannel;

                                    SearchMultiSelectField salesOrderSubsidiary2 = new SearchMultiSelectField();
                                    salesOrderSubsidiary2.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary2.operatorSpecified = true;
                                    //salesOrderSubsidiary2.searchValue = new RecordRef[] { refGMY2, refSEIS2 };
                                    //TH DROPSHIP
                                    salesOrderSubsidiary2.searchValue = new RecordRef[] { refGMY2, refSEIS2, refTH2 };
                                    sotsb2.subsidiary = salesOrderSubsidiary2;

                                    //To exclude these 3 Customer - WY-10.OCT.2014
                                    RecordRef refBCASMY2 = new RecordRef();
                                    refBCASMY2.internalId = @Resource.CUST_BCASMY_INTERNALID;

                                    RecordRef refBCASID2 = new RecordRef();
                                    refBCASID2.internalId = @Resource.CUST_BCASID_INTERNALID;

                                    RecordRef refBCASSG2 = new RecordRef();
                                    refBCASSG2.internalId = @Resource.CUST_BCASSG_INTERNALID;

                                    SearchMultiSelectField soBCASCust2 = new SearchMultiSelectField();
                                    soBCASCust2.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    soBCASCust2.operatorSpecified = true;
                                    soBCASCust2.searchValue = new RecordRef[] { refBCASMY2, refBCASID2, refBCASSG2 };
                                    sotsb2.entity = soBCASCust2;

                                    SearchMultiSelectCustomField soSync2 = new SearchMultiSelectCustomField();
                                    soSync2.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    soSync2.operatorSpecified = true;
                                    soSync2.scriptId = "custbody_wms_field";
                                    ListOrRecordRef soListOrRecordRef2 = new ListOrRecordRef();
                                    soListOrRecordRef2.internalId = "1";
                                    soListOrRecordRef2.typeId = "136";
                                    soSync2.searchValue = new ListOrRecordRef[] { soListOrRecordRef2 };
                                    SearchCustomField[] soScf2 = new SearchCustomField[] { soSync2 };
                                    sotsb2.customFieldList = soScf2;

                                    SearchDateField salesOrderDate2 = new SearchDateField();
                                    salesOrderDate2.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate2.operatorSpecified = true;
                                    salesOrderDate2.searchValueSpecified = true;
                                    salesOrderDate2.searchValue2Specified = true;
                                    salesOrderDate2.searchValue = DateTime.Parse(dateRangeFrom).AddDays(-2);
                                    salesOrderDate2.searchValue2 = DateTime.Parse(dateRangeFrom).AddDays(-1);
                                    sotsb2.lastModifiedDate = salesOrderDate2;

                                    sots2.basic = sotsb2;
                                    sotsa2.criteria = sots2;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa2);
                                        if (sr.recordList.Count() > 0)
                                        {

                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa2);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                //To handle auto-back order extraction - WY-03.NOV.2014
                                #region NS-LATEST SALES ORDER 3
                                case "NS-LATEST SALES ORDER 3"://Extract every 1 hr

                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER 3 within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa3 = new TransactionSearchAdvanced();
                                    TransactionSearch sots3 = new TransactionSearch();
                                    TransactionSearchBasic sotsb3 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField soBoType = new SearchEnumMultiSelectField();
                                    soBoType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    soBoType.operatorSpecified = true;
                                    soBoType.searchValue = new String[] { "_salesOrder" };
                                    sotsb3.type = soBoType;

                                    string connStr = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                                    MySqlConnection mysqlCon = new MySqlConnection(connStr);
                                    mysqlCon.Open();
                                    String strRangeTo = convertDateToString(Convert.ToDateTime(r.rn_rangeTo));

                                    Int32 tolBackOrder = 0;
                                    List<TempBackOrder> boList = new List<TempBackOrder>();

                                    //To Select Back Order
                                    var queryBO = "SELECT so1.nt1_moNo_internalID,so1.nt1_seqID, so1.nt1_moNo,so1.nt1_moNo_internalID, so1.nt1_status, " +
                                                  "sum(so1.nt1_ordQty)  AS OrdQty,so2.nt2_qtyForWMS  AS QtyForWMS,so2.nt2_fulfilledQty  AS FulFilledQty, " +
                                                  "(sum(so1.nt1_ordQty)  - if (so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS)) as calc_difference  " +
                                                  "FROM view_newso so1 " +
                                                  "left join " +
                                                  "(SELECT nt2_seqID, nt2_moNo_internalID " +
                                                  ",if (sum(nt2_qtyForWMS) is null,0,sum(nt2_qtyForWMS)) as nt2_qtyForWMS, " +
                                                  "if (sum(nt2_fulfilledQty) is null,0,sum(nt2_fulfilledQty)) as nt2_fulfilledQty " +
                                                  "FROM netsuite_syncso " +
                                                  "group by nt2_moNo_internalID) so2 " +
                                                  "on so1.nt1_moNo_internalID = so2.nt2_moNo_internalID " +
                                                  "where so1.nt1_status in ('PENDING FULFILLMENT','PENDING BILLING/PARTIALLY FULFILLED','PARTIALLY FULFILLED') " +
                                                  "and so1.nt1_subsidiary = '" + @Resource.SUBSIDIARY_NAME_MY + "' " +
                                                  "and so1.nt1_rangeTo < '" + strRangeTo + "' " +
                                                  "group by so1.nt1_moNo_internalID " +
                                                  "having (calc_difference > 0) order by so1.nt1_rangeTo ASC LIMIT 30 ";

                                    MySqlCommand cmd = new MySqlCommand(queryBO, mysqlCon);
                                    MySqlDataReader dtr = cmd.ExecuteReader();

                                    while (dtr.Read())
                                    {
                                        TempBackOrder backOrder = new TempBackOrder();
                                        backOrder.moNoInternalID = (dtr.GetValue(0) == DBNull.Value) ? String.Empty : dtr.GetString(0);
                                        boList.Add(backOrder);
                                        tolBackOrder++;
                                    }
                                    dtr.Close();
                                    cmd.Dispose();

                                    //To Select Latest SO with Customer ID is empty or null
                                    var qCustNull = "SELECT DISTINCT nt1_moNo_internalID FROM netsuite_newso WHERE nt1_createdDate >= '2014-11-01' " +
                                                    "AND (nt1_custID IS NULL OR nt1_custID = '') ";

                                    MySqlCommand cmd2 = new MySqlCommand(qCustNull, mysqlCon);
                                    MySqlDataReader dtr2 = cmd2.ExecuteReader();
                                    while (dtr2.Read())
                                    {
                                        TempBackOrder backOrder = new TempBackOrder();
                                        backOrder.moNoInternalID = (dtr2.GetValue(0) == DBNull.Value) ? String.Empty : dtr2.GetString(0);
                                        boList.Add(backOrder);
                                        tolBackOrder++;
                                    }

                                    RecordRef[] refBOInternalID = new RecordRef[tolBackOrder];
                                    for (int i = 0; i < tolBackOrder; i++)
                                    {
                                        RecordRef tempBORef = new RecordRef();
                                        tempBORef.internalId = boList[i].moNoInternalID.ToString();
                                        refBOInternalID[i] = tempBORef;
                                    }

                                    dtr2.Close();
                                    cmd2.Dispose();
                                    mysqlCon.Close();
                                    mysqlCon.Dispose();

                                    if (tolBackOrder > 0)
                                    {
                                        SearchMultiSelectField soBOInternalID = new SearchMultiSelectField();
                                        soBOInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soBOInternalID.operatorSpecified = true;
                                        soBOInternalID.searchValue = refBOInternalID;
                                        sotsb3.internalId = soBOInternalID;

                                        sots3.basic = sotsb3;
                                        sotsa3.criteria = sots3;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotsa3);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotsa3);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                //Same as NS-LATEST SALES ORDER 2 but with status = Pending Fulfillment - WY-03.DEC.2014
                                #region NS-LATEST SALES ORDER 4
                                case "NS-LATEST SALES ORDER 4"://Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER 4 within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa4 = new TransactionSearchAdvanced();
                                    TransactionSearch sots4 = new TransactionSearch();
                                    TransactionSearchBasic sotsb4 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus4 = new SearchEnumMultiSelectField();
                                    salesOrderStatus4.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus4.operatorSpecified = true;
                                    salesOrderStatus4.searchValue = new String[] { "_salesOrderPendingFulfillment" };
                                    sotsb4.status = salesOrderStatus4;

                                    RecordRef refGMY4 = new RecordRef();
                                    refGMY4.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS4 = new RecordRef();
                                    refSEIS4.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH4 = new RecordRef();
                                    refTH4.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    RecordRef refSO1ET4 = new RecordRef();
                                    refSO1ET4.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refSOEDUGeneral4 = new RecordRef();
                                    refSOEDUGeneral4.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added Code for online store on 22-Mar-2021 by Brash Developer
                                    RecordRef refSOOnlineStore4 = new RecordRef();
                                    refSOOnlineStore4.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    SearchMultiSelectField so4BusinessChannel = new SearchMultiSelectField();
                                    so4BusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so4BusinessChannel.operatorSpecified = true;
                                    so4BusinessChannel.searchValue = new RecordRef[] { refSO1ET4, refSOEDUGeneral4, refSOOnlineStore4 };
                                    sotsb4.@class = so4BusinessChannel;

                                    SearchMultiSelectField salesOrderSubsidiary4 = new SearchMultiSelectField();
                                    salesOrderSubsidiary4.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary4.operatorSpecified = true;
                                    //salesOrderSubsidiary4.searchValue = new RecordRef[] { refGMY4, refSEIS4 };
                                    //TH DROPSHIP
                                    salesOrderSubsidiary4.searchValue = new RecordRef[] { refGMY4, refSEIS4, refTH4 };
                                    sotsb4.subsidiary = salesOrderSubsidiary4;

                                    SearchMultiSelectCustomField soSync4 = new SearchMultiSelectCustomField();
                                    soSync4.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    soSync4.operatorSpecified = true;
                                    soSync4.scriptId = "custbody_wms_field";
                                    ListOrRecordRef soListOrRecordRef4 = new ListOrRecordRef();
                                    soListOrRecordRef4.internalId = "1";
                                    soListOrRecordRef4.typeId = "136";
                                    soSync4.searchValue = new ListOrRecordRef[] { soListOrRecordRef4 };
                                    SearchCustomField[] soScf4 = new SearchCustomField[] { soSync4 };
                                    sotsb4.customFieldList = soScf4;

                                    SearchDateField salesOrderDate4 = new SearchDateField();
                                    salesOrderDate4.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate4.operatorSpecified = true;
                                    salesOrderDate4.searchValueSpecified = true;
                                    salesOrderDate4.searchValue2Specified = true;
                                    salesOrderDate4.searchValue = DateTime.Parse(dateRangeFrom).AddDays(-2);
                                    salesOrderDate4.searchValue2 = DateTime.Parse(dateRangeFrom).AddDays(-1);
                                    sotsb4.lastModifiedDate = salesOrderDate4;

                                    sots4.basic = sotsb4;
                                    sotsa4.criteria = sots4;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa4);
                                        if (sr.recordList.Count() > 0)
                                        {

                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa4);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                //Split Dropshipment so extraction from NS-LATEST SALES ORDER - 17.DEC.2014    
                                #region NS-LATEST SALES ORDER 5
                                case "NS-LATEST SALES ORDER 5"://Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER 5 within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa7 = new TransactionSearchAdvanced();
                                    TransactionSearch sots7 = new TransactionSearch();
                                    TransactionSearchBasic sotsb7 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus7 = new SearchEnumMultiSelectField();
                                    salesOrderStatus7.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus7.operatorSpecified = true;
                                    salesOrderStatus7.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed", "_salesOrderCancelled" };
                                    sotsb7.status = salesOrderStatus7;

                                    RecordRef refGMY7 = new RecordRef();
                                    refGMY7.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS7 = new RecordRef();
                                    refSEIS7.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH7 = new RecordRef();
                                    refTH7.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    RecordRef refSO1ET7 = new RecordRef();
                                    refSO1ET7.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refSOEDUGeneral7 = new RecordRef();
                                    refSOEDUGeneral7.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added Code for online store on 22-Mar-2021 by Brash Developer
                                    RecordRef refSOOnlineStore7 = new RecordRef();
                                    refSOOnlineStore7.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel7 = new SearchMultiSelectField();
                                    so1BusinessChannel7.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel7.operatorSpecified = true;
                                    so1BusinessChannel7.searchValue = new RecordRef[] { refSO1ET7, refSOEDUGeneral7, refSOOnlineStore7 };
                                    sotsb7.@class = so1BusinessChannel7;

                                    SearchMultiSelectField salesOrderSubsidiary7 = new SearchMultiSelectField();
                                    salesOrderSubsidiary7.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary7.operatorSpecified = true;
                                    //salesOrderSubsidiary7.searchValue = new RecordRef[] { refGMY7, refSEIS7 };
                                    //TH DROPSHIP
                                    salesOrderSubsidiary7.searchValue = new RecordRef[] { refGMY7, refSEIS7, refTH7 };
                                    sotsb7.subsidiary = salesOrderSubsidiary7;

                                    RecordRef refDropShip7 = new RecordRef();//P00892
                                    refDropShip7.internalId = @Resource.SEIS_CUSTOMER_MY_INTERNALID;

                                    //TH DROPSHIP
                                    RecordRef refDropShipTH7 = new RecordRef();//P01042
                                    refDropShipTH7.internalId = @Resource.DROPSHIP_CUSTOMER_TH_INTERNALID;

                                    RecordRef refDropShipEDUGENERAL7 = new RecordRef();//P00892
                                    refDropShipEDUGENERAL7.internalId = @Resource.SEIS_CUSTOMER_MY_EDUGENERAL_INTERNALID;

                                    SearchMultiSelectField soBCASCust7 = new SearchMultiSelectField();
                                    soBCASCust7.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    soBCASCust7.operatorSpecified = true;
                                    //soBCASCust7.searchValue = new RecordRef[] { refDropShip7 };
                                    //TH DROPSHIP
                                    soBCASCust7.searchValue = new RecordRef[] { refDropShip7, refDropShipTH7, refDropShipEDUGENERAL7 };
                                    sotsb7.entity = soBCASCust7;

                                    SearchDateField salesOrderDate7 = new SearchDateField();
                                    salesOrderDate7.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate7.operatorSpecified = true;
                                    salesOrderDate7.searchValueSpecified = true;
                                    salesOrderDate7.searchValue2Specified = true;
                                    salesOrderDate7.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate7.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb7.lastModifiedDate = salesOrderDate7;

                                    sots7.basic = sotsb7;
                                    sotsa7.criteria = sots7;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa7);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.LatestSalesOrders(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //SearchResult sr = service.search(sotsa1);
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(sotsa7);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region NS-LATEST SALES ORDER 6 - for update Sync To WMS only
                                case "NS-LATEST SALES ORDER 6"://Extract every 1 hr
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-LATEST SALES ORDER 6 within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa8 = new TransactionSearchAdvanced();
                                    TransactionSearch sots8 = new TransactionSearch();
                                    TransactionSearchBasic sotsb8 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField salesOrderStatus8 = new SearchEnumMultiSelectField();
                                    salesOrderStatus8.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    salesOrderStatus8.operatorSpecified = true;
                                    salesOrderStatus8.searchValue = new String[] { "_salesOrderPendingFulfillment", "_salesOrderPartiallyFulfilled", "_salesOrderPendingBillingPartiallyFulfilled", "_salesOrderPendingBilling", "_salesOrderPendingApproval", "_salesOrderClosed", "_salesOrderCancelled" };
                                    sotsb8.status = salesOrderStatus8;

                                    RecordRef refGMY8 = new RecordRef();
                                    refGMY8.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    RecordRef refSEIS8 = new RecordRef();
                                    refSEIS8.internalId = @Resource.SUBSIDIARY_INTERNALID_SEIS;

                                    //TH DROPSHIP
                                    RecordRef refTH8 = new RecordRef();
                                    refTH8.internalId = @Resource.SUBSIDIARY_INTERNALID_TH;

                                    RecordRef refSO1ET8 = new RecordRef();
                                    refSO1ET8.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refSOEDUGeneral8 = new RecordRef();
                                    refSOEDUGeneral8.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added Code for online store on 22-Mar-2021 by Brash Developer
                                    RecordRef refSOOnlineStore8 = new RecordRef();
                                    refSOOnlineStore8.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    SearchMultiSelectField so1BusinessChannel8 = new SearchMultiSelectField();
                                    so1BusinessChannel8.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    so1BusinessChannel8.operatorSpecified = true;
                                    so1BusinessChannel8.searchValue = new RecordRef[] { refSO1ET8, refSOEDUGeneral8, refSOOnlineStore8 };
                                    sotsb8.@class = so1BusinessChannel8;

                                    SearchMultiSelectField salesOrderSubsidiary8 = new SearchMultiSelectField();
                                    salesOrderSubsidiary8.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    salesOrderSubsidiary8.operatorSpecified = true;
                                    //salesOrderSubsidiary8.searchValue = new RecordRef[] { refGMY8, refSEIS8 };
                                    //TH DROPSHIP
                                    salesOrderSubsidiary8.searchValue = new RecordRef[] { refGMY8, refSEIS8, refTH8 };
                                    sotsb8.subsidiary = salesOrderSubsidiary8;

                                    SearchMultiSelectCustomField soSync8 = new SearchMultiSelectCustomField();
                                    soSync8.@operator = SearchMultiSelectFieldOperator.noneOf; //use noneof
                                    soSync8.operatorSpecified = true;
                                    soSync8.scriptId = "custbody_wms_field";
                                    ListOrRecordRef soListOrRecordRef8 = new ListOrRecordRef();
                                    soListOrRecordRef8.internalId = "1";
                                    soListOrRecordRef8.typeId = "136";
                                    soSync8.searchValue = new ListOrRecordRef[] { soListOrRecordRef8 };
                                    SearchCustomField[] soScf8 = new SearchCustomField[] { soSync8 };
                                    sotsb8.customFieldList = soScf8;

                                    SearchDateField salesOrderDate8 = new SearchDateField();
                                    salesOrderDate8.@operator = SearchDateFieldOperator.within;
                                    salesOrderDate8.operatorSpecified = true;
                                    salesOrderDate8.searchValueSpecified = true;
                                    salesOrderDate8.searchValue2Specified = true;
                                    salesOrderDate8.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    salesOrderDate8.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    sotsb8.lastModifiedDate = salesOrderDate8;

                                    sots8.basic = sotsb8;
                                    sotsa8.criteria = sots8;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(sotsa8);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.LatestSalesOrders(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        //SearchResult sr = service.search(sotsa1);
                                        job = netsuiteService.asyncSearch(sotsa8);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                //To extract update synced list - WY-04.NOV.2014
                                #region NS-SALES ORDER SYNC UPDATE
                                case "NS-SALES ORDER SYNC UPDATE":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-SALES ORDER SYNC UPDATE within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa5 = new TransactionSearchAdvanced();
                                    TransactionSearch sots5 = new TransactionSearch();
                                    TransactionSearchBasic sotsb5 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField soSyncType = new SearchEnumMultiSelectField();
                                    soSyncType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    soSyncType.operatorSpecified = true;
                                    soSyncType.searchValue = new String[] { "_salesOrder" };
                                    sotsb5.type = soSyncType;

                                    var syncList = (from synced in entities.netsuite_syncso
                                                    where synced.nt2_lastModifiedDate > rangeFrom
                                                       && synced.nt2_lastModifiedDate <= rangeTo
                                                    select new { synced.nt2_moNo_internalID }).Distinct().ToList();

                                    RecordRef[] refSyncInternalID = new RecordRef[syncList.Count()];

                                    for (int i = 0; i < syncList.Count(); i++)
                                    {
                                        RecordRef tempSyncRef = new RecordRef();
                                        tempSyncRef.internalId = syncList[i].nt2_moNo_internalID.ToString();
                                        refSyncInternalID[i] = tempSyncRef;
                                    }

                                    if (syncList.Count() > 0)
                                    {
                                        SearchMultiSelectField soSyncInternalID = new SearchMultiSelectField();
                                        soSyncInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soSyncInternalID.operatorSpecified = true;
                                        soSyncInternalID.searchValue = refSyncInternalID;
                                        sotsb5.internalId = soSyncInternalID;

                                        sots5.basic = sotsb5;
                                        sotsa5.criteria = sots5;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotsa5);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotsa5);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                //To extract deduct synced list - WY-07.NOV.2014
                                #region NS-SO DEDUCT SYNC
                                case "NS-SO DEDUCT SYNC":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-SO DEDUCT SYNC within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsa6 = new TransactionSearchAdvanced();
                                    TransactionSearch sots6 = new TransactionSearch();
                                    TransactionSearchBasic sotsb6 = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField sodeSyncType = new SearchEnumMultiSelectField();
                                    sodeSyncType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    sodeSyncType.operatorSpecified = true;
                                    sodeSyncType.searchValue = new String[] { "_salesOrder" };
                                    sotsb6.type = sodeSyncType;

                                    var desyncList = (from synced in entities.netsuite_syncso
                                                      where synced.nt2_lastfulfilledDate > rangeFrom
                                                         && synced.nt2_lastfulfilledDate <= rangeTo
                                                      select new { synced.nt2_moNo_internalID }).Distinct().ToList();

                                    RecordRef[] refdeSyncInternalID = new RecordRef[desyncList.Count()];

                                    for (int i = 0; i < desyncList.Count(); i++)
                                    {
                                        RecordRef tempdeSyncRef = new RecordRef();
                                        tempdeSyncRef.internalId = desyncList[i].nt2_moNo_internalID.ToString();
                                        refdeSyncInternalID[i] = tempdeSyncRef;
                                    }

                                    if (desyncList.Count() > 0)
                                    {
                                        SearchMultiSelectField sodeSyncInternalID = new SearchMultiSelectField();
                                        sodeSyncInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        sodeSyncInternalID.operatorSpecified = true;
                                        sodeSyncInternalID.searchValue = refdeSyncInternalID;
                                        sotsb6.internalId = sodeSyncInternalID;

                                        sots6.basic = sotsb6;
                                        sotsa6.criteria = sots6;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotsa6);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotsa6);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                //To update committed qty in newso - WY-20.JAN.2015
                                #region NS-COMMMIT QUANTITY
                                case "NS-COMMMIT QUANTITY":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-COMMMIT QUANTITY within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced sotsacqty = new TransactionSearchAdvanced();
                                    TransactionSearch sotscqty = new TransactionSearch();
                                    TransactionSearchBasic sotsbcqty = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField socqty = new SearchEnumMultiSelectField();
                                    socqty.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    socqty.operatorSpecified = true;
                                    socqty.searchValue = new String[] { "_salesOrder" };
                                    sotsbcqty.type = socqty;

                                    string connStrcqty = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                                    MySqlConnection mysqlConcqty = new MySqlConnection(connStrcqty);
                                    mysqlConcqty.Open();
                                    String strRangeTocqty = convertDateToString(Convert.ToDateTime(r.rn_rangeTo));

                                    Int32 tolSOList = 0;
                                    List<TempBackOrder> soList = new List<TempBackOrder>();

                                    var querySO = "SELECT so1.nt1_seqID, so1.nt1_moNo,so1.nt1_moNo_internalID, so1.nt1_status,so1.nt1_itemID," +//4
                                                    "so1.nt1_item_internalID, sum(so1.nt1_committedQty) as nt1_committedQty,sum(so1.nt1_fulfilledQty) as nt1_fulfilledQty, " +//7
                                                    "so1.nt1_rangeTo, " +//8
                                                    "if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) as nt2_qtyForWMS, " +//9
                                                    "if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty) as nt2_fulfilledQty, " +//10
                                                    "(sum(so1.nt1_ordQty) - if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS)) as calc_qtyForWMS, " +//11
                                                    "(sum(so1.nt1_ordQty) - sum(so1.nt1_fulfilledQty)) as calc_difference, " +//12
                                                    "so1.nt1_ordQty, so1.nt1_tax, so1.nt1_discount, so1.nt1_rate, so1.nt1_amount, " +//17
                                                    "so1.nt1_customer, so1.nt1_customer_internalID, so1.nt1_SEIS_moNo,so1.nt1_SEIS_moNo_internalID, so1.nt1_subsidiary, " +//22
                                                    "so1.nt1_custID,so1.nt1_addressee,so1.nt1_deliveryAdd,so1.nt1_deliveryAdd2,so1.nt1_deliveryAdd3,so1.nt1_postCode,so1.nt1_contactPerson,so1.nt1_phone,so1.nt1_country, " + //31
                                                    "so1.nt1_billingAddressee,so1.nt1_billingAdd,so1.nt1_billingAdd2,so1.nt1_billingAdd3,so1.nt1_billingPostcode,so1.nt1_billingContactPerson,so1.nt1_billingPhone, " + //38
                                                    "so1.nt1_shipMethod,so1.nt1_creditTerm,so1.nt1_basedprice,so1.nt1_pricelevel " + //42
                                                    "FROM view_newso so1 " +
                                                    "left join " +
                                                    "(SELECT nt2_seqID, nt2_moNo_internalID, nt2_item_internalID " +
                                                    ",sum(nt2_qtyForWMS) as nt2_qtyForWMS, " +
                                                    "sum(nt2_unfulfilledQty) as nt2_unfulfilledQty, " +
                                                    "sum(nt2_fulfilledQty) as nt2_fulfilledQty " +
                                                    "FROM netsuite_syncso " +
                                                    "group by nt2_moNo_internalID,nt2_item_internalID) so2 " +
                                                    "on so1.nt1_moNo_internalID = so2.nt2_moNo_internalID " +
                                                    "and so1.nt1_item_internalID = so2.nt2_item_internalID " +
                                                    "where so1.nt1_status in ('PENDING FULFILLMENT','PENDING BILLING/PARTIALLY FULFILLED','PARTIALLY FULFILLED') " +
                                                    "and so1.nt1_subsidiary = '" + @Resource.SUBSIDIARY_NAME_MY + "' " +
                                                    "and so1.nt1_synctowms = '1' " +
                                                    "and so1.nt1_rangeTo <= '" + strRangeTocqty + "' " +
                                                    "group by so1.nt1_moNo_internalID, so1.nt1_item_internalID " +
                                                    "having calc_qtyForWMS > 0  and nt1_ordQty > 0 and calc_difference>0";

                                    #region no use
                                    /*
                                 var querySO =   "SELECT so1.nt1_seqID, so1.nt1_moNo,so1.nt1_moNo_internalID, so1.nt1_status,so1.nt1_itemID," +//4
                                                "so1.nt1_item_internalID, sum(so1.nt1_committedQty) as nt1_committedQty,sum(so1.nt1_fulfilledQty) as nt1_fulfilledQty, " +//7
                                                "so1.nt1_rangeTo, " +//8
                                                "if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS) as nt2_qtyForWMS, " +//9
                                                "if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty) as nt2_fulfilledQty, " +//10
                                                "(so1.nt1_committedQty) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS)+ if(so2.nt2_unfulfilledQty is null,0,so2.nt2_unfulfilledQty) - (so1.nt1_fulfilledQty)) as calc_qtyForWMS, " +//11
                                                "((so1.nt1_committedQty + so1.nt1_fulfilledQty) - (if(so2.nt2_qtyForWMS is null,0,so2.nt2_qtyForWMS)+ if(so2.nt2_unfulfilledQty is null,0,so2.nt2_unfulfilledQty) + if(so2.nt2_fulfilledQty is null,0,so2.nt2_fulfilledQty))) as calc_difference, " +//12
                                                "so1.nt1_ordQty, so1.nt1_tax, so1.nt1_discount, so1.nt1_rate, so1.nt1_amount, " +//17
                                                "so1.nt1_customer, so1.nt1_customer_internalID, so1.nt1_SEIS_moNo,so1.nt1_SEIS_moNo_internalID, so1.nt1_subsidiary, " +//22
                                                "so1.nt1_custID,so1.nt1_addressee,so1.nt1_deliveryAdd,so1.nt1_deliveryAdd2,so1.nt1_deliveryAdd3,so1.nt1_postCode,so1.nt1_contactPerson,so1.nt1_phone,so1.nt1_country, " + //31
                                                "so1.nt1_billingAddressee,so1.nt1_billingAdd,so1.nt1_billingAdd2,so1.nt1_billingAdd3,so1.nt1_billingPostcode,so1.nt1_billingContactPerson,so1.nt1_billingPhone, " + //38
                                                "so1.nt1_shipMethod,so1.nt1_creditTerm,so1.nt1_basedprice,so1.nt1_pricelevel " + //42
                                                "FROM view_newso so1 " +
                                                "left join " +
                                                "(SELECT nt2_seqID, nt2_moNo_internalID, nt2_item_internalID " +
                                                ",sum(nt2_qtyForWMS) as nt2_qtyForWMS, " +
                                                "sum(nt2_unfulfilledQty) as nt2_unfulfilledQty, " +
                                                "sum(nt2_fulfilledQty) as nt2_fulfilledQty " +
                                                "FROM netsuite_syncso " +
                                                "group by nt2_moNo_internalID,nt2_item_internalID) so2 " +
                                                "on so1.nt1_moNo_internalID = so2.nt2_moNo_internalID " +
                                                "and so1.nt1_item_internalID = so2.nt2_item_internalID " +
                                                "where so1.nt1_status in ('PENDING FULFILLMENT','PENDING BILLING/PARTIALLY FULFILLED','PARTIALLY FULFILLED') " + 
                                                "and so1.nt1_subsidiary = '"+ @Resource.SUBSIDIARY_NAME_MY +"' " +
                                                "and so1.nt1_synctowms = '1' " +
                                                "and so1.nt1_rangeTo <= '" + strRangeTocqty + "' " +  
                                                "group by so1.nt1_moNo_internalID, so1.nt1_item_internalID " +
                                                "having (calc_qtyForWMS > 0  or calc_difference > 0) and nt1_committedQty > 0";
                                 */
                                    #endregion

                                    MySqlCommand cmdcqty = new MySqlCommand(querySO, mysqlConcqty);
                                    cmdcqty.CommandTimeout = 180;//Added comman timeout - WY-12.MAY.2016
                                    MySqlDataReader dtrcqty = cmdcqty.ExecuteReader();
                                    Hashtable htSOList = new Hashtable();

                                    while (dtrcqty.Read())
                                    {
                                        String mono = (dtrcqty.GetValue(1) == DBNull.Value) ? String.Empty : dtrcqty.GetString(1);
                                        String mono_InternalID = (dtrcqty.GetValue(2) == DBNull.Value) ? String.Empty : dtrcqty.GetString(2);
                                        Boolean isExist = false;
                                        TempBackOrder salesOrder = new TempBackOrder();

                                        isExist = htSOList.Contains(mono_InternalID);
                                        if (isExist == false)
                                        {
                                            htSOList.Add(mono_InternalID, mono);
                                            salesOrder.moNoInternalID = mono_InternalID;
                                            soList.Add(salesOrder);
                                            tolSOList++;
                                        }
                                    }
                                    dtrcqty.Close();
                                    cmdcqty.Dispose();

                                    RecordRef[] refSOInternalID = new RecordRef[tolSOList];
                                    for (int i = 0; i < tolSOList; i++)
                                    {
                                        RecordRef tempBORef = new RecordRef();
                                        tempBORef.internalId = soList[i].moNoInternalID.ToString();
                                        refSOInternalID[i] = tempBORef;
                                    }

                                    dtrcqty.Close();
                                    cmdcqty.Dispose();
                                    mysqlConcqty.Close();
                                    mysqlConcqty.Dispose();

                                    if (tolSOList > 0)
                                    {
                                        SearchMultiSelectField soSOInternalID = new SearchMultiSelectField();
                                        soSOInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soSOInternalID.operatorSpecified = true;
                                        soSOInternalID.searchValue = refSOInternalID;
                                        sotsbcqty.internalId = soSOInternalID;

                                        //cpng start : select specific column only
                                        TransactionSearchRow tsr = new TransactionSearchRow();
                                        TransactionSearchRowBasic tsrb = new TransactionSearchRowBasic();

                                        SearchColumnSelectField[] scsf = new SearchColumnSelectField[1];
                                        SearchColumnSelectField refscsf = new SearchColumnSelectField();
                                        SearchColumnDoubleField[] scdf = new SearchColumnDoubleField[1];
                                        SearchColumnDoubleField refscdf = new SearchColumnDoubleField();
                                        SearchColumnLongField[] sclf = new SearchColumnLongField[1];
                                        SearchColumnLongField refsclf = new SearchColumnLongField();
                                        SearchColumnStringField[] scstringf = new SearchColumnStringField[1];
                                        SearchColumnStringField refscstringf = new SearchColumnStringField();
                                        SearchColumnBooleanField[] scbf = new SearchColumnBooleanField[1];
                                        SearchColumnBooleanField refscbf = new SearchColumnBooleanField();
                                        SearchColumnBooleanCustomField refscbcf = new SearchColumnBooleanCustomField();
                                        refscbcf.scriptId = @Resource.CUSTOMFIELD_CUST_BOOKING_SCRIPTID;
                                        SearchColumnCustomField[] sccf = new SearchColumnCustomField[] { refscbcf };
                                        scsf[0] = refscsf;
                                        scdf[0] = refscdf;
                                        sclf[0] = refsclf;
                                        scstringf[0] = refscstringf;
                                        scbf[0] = refscbf;


                                        tsrb.internalId = scsf;
                                        tsrb.quantity = scdf;
                                        tsrb.item = scsf;
                                        tsrb.line = sclf;
                                        tsrb.tranId = scstringf;
                                        tsrb.closed = scbf;
                                        tsrb.customFieldList = sccf;
                                        tsr.basic = tsrb;
                                        sotsacqty.columns = tsr;
                                        //cpng end

                                        sotscqty.basic = sotsbcqty;
                                        sotsacqty.criteria = sotscqty;



                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotsacqty);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotsacqty);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                #endregion
                                #region Invoice
                                case "NS-INVOICE":
                                    this.DataFromNetsuiteLog.Info("Asnyc search pending fulfillment within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced invtsa = new TransactionSearchAdvanced();
                                    TransactionSearch invts = new TransactionSearch();
                                    TransactionSearchBasic invtsb = new TransactionSearchBasic();

                                    //SearchEnumMultiSelectField invSalesOrderStatus = new SearchEnumMultiSelectField();
                                    //invSalesOrderStatus.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    //invSalesOrderStatus.operatorSpecified = true;
                                    //invSalesOrderStatus.searchValue = new String[] { "_salesOrderPendingBilling", "_salesOrderPendingBillingPartiallyFulfilled" };
                                    //invtsb.status = invSalesOrderStatus;

                                    //SearchDateField invSalesOrderDate = new SearchDateField();
                                    //invSalesOrderDate.@operator = SearchDateFieldOperator.within;
                                    //invSalesOrderDate.operatorSpecified = true;
                                    //invSalesOrderDate.searchValueSpecified = true;
                                    //invSalesOrderDate.searchValue2Specified = true;
                                    //invSalesOrderDate.searchValue = DateTime.Parse(dateRangeFrom);
                                    //invSalesOrderDate.searchValue2 = DateTime.Parse(dateRangeTo);
                                    //invtsb.lastModifiedDate = invSalesOrderDate;

                                    RecordRef[] invInternalID = new RecordRef[1];
                                    RecordRef tempinvRef = new RecordRef();
                                    tempinvRef.internalId = "227119";
                                    invInternalID[0] = tempinvRef;

                                    SearchMultiSelectField inv2InternalID = new SearchMultiSelectField();
                                    inv2InternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    inv2InternalID.operatorSpecified = true;
                                    inv2InternalID.searchValue = invInternalID;
                                    invtsb.internalId = inv2InternalID;

                                    invts.basic = invtsb;
                                    invtsa.criteria = invts;
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(invtsa);
                                    jobID = job.jobId;
                                    break;
                                #endregion
                                #region Purchase Request
                                case "NS-PURCHASE REQUEST":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-PURCHASE REQUEST  within " + dateRangeFrom + " to " + dateRangeTo);

                                    RecordRef refBC = new RecordRef();
                                    RecordRef refET = new RecordRef();

                                    var q3 = (from q in entities.map_businesschannel
                                              where q.mb_imas_businessChannel_code == "BC" || q.mb_imas_businessChannel_code == "ET"
                                              select q.mb_businessChannel_internalID).ToArray();

                                    for (int i = 0; i < q3.Count(); i++)
                                    {
                                        if (i == 0)
                                        {

                                            refBC.internalId = q3[i];//Trade or Book Clubs
                                        }
                                        else if (i == 1)
                                        {

                                            refET.internalId = q3[i];//Trade or Book Clubs
                                        }
                                    }

                                    TransactionSearchBasic prtsb = new TransactionSearchBasic();

                                    //Include Subsidiary = 'GMY' - WY-20.OCT.2014
                                    RecordRef refPRGMY = new RecordRef();
                                    refPRGMY.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    SearchMultiSelectField prSubsidiary = new SearchMultiSelectField();
                                    prSubsidiary.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    prSubsidiary.operatorSpecified = true;
                                    prSubsidiary.searchValue = new RecordRef[] { refPRGMY };
                                    prtsb.subsidiary = prSubsidiary;

                                    SearchEnumMultiSelectField poStatus = new SearchEnumMultiSelectField();
                                    poStatus.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    poStatus.operatorSpecified = true;
                                    poStatus.searchValue = new String[] { "_purchaseOrderPendingReceipt" };
                                    prtsb.status = poStatus;

                                    SearchMultiSelectField poBusinessChannel = new SearchMultiSelectField();
                                    poBusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    poBusinessChannel.operatorSpecified = true;
                                    poBusinessChannel.searchValue = new RecordRef[] { refBC, refET };
                                    prtsb.@class = poBusinessChannel;

                                    //Change to extract Completed and Yes - WY-23.DEC.2014
                                    //SearchMultiSelectCustomField poSync = new SearchMultiSelectCustomField();
                                    //poSync.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    //poSync.operatorSpecified = true;
                                    //poSync.scriptId = "custbody_wms_field";
                                    //ListOrRecordRef listOrRecordRef = new ListOrRecordRef();
                                    //listOrRecordRef.internalId = "1";
                                    //listOrRecordRef.typeId = "136";
                                    //poSync.searchValue = new ListOrRecordRef[] { listOrRecordRef };
                                    //SearchCustomField[] scf = new SearchCustomField[] { poSync };
                                    //prtsb.customFieldList = scf;
                                    SearchMultiSelectCustomField poSync = new SearchMultiSelectCustomField();
                                    poSync.@operator = SearchMultiSelectFieldOperator.noneOf;
                                    poSync.operatorSpecified = true;
                                    poSync.scriptId = "custbody_wms_field";
                                    ListOrRecordRef listOrRecordRef = new ListOrRecordRef();
                                    listOrRecordRef.internalId = "2";
                                    listOrRecordRef.typeId = "136";
                                    poSync.searchValue = new ListOrRecordRef[] { listOrRecordRef };
                                    SearchCustomField[] scf = new SearchCustomField[] { poSync };
                                    prtsb.customFieldList = scf;

                                    SearchDateField poDate = new SearchDateField();
                                    poDate.@operator = SearchDateFieldOperator.within;
                                    poDate.operatorSpecified = true;
                                    poDate.searchValueSpecified = true;
                                    poDate.searchValue2Specified = true;
                                    poDate.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    poDate.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    prtsb.lastModifiedDate = poDate;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(prtsb);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.PurchaseRequests(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(prtsb);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Inventory Adjustment
                                case "NS-INVENTORY ADJUSTMENT":
                                    this.DataFromNetsuiteLog.Info("TransachtionAsyncSearch: NS-INVENTORY ADJUSTMENT within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced invadjtsa = new TransactionSearchAdvanced();
                                    TransactionSearch invadjts = new TransactionSearch();
                                    TransactionSearchBasic invadjtsb = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField invAdjType = new SearchEnumMultiSelectField();
                                    invAdjType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    invAdjType.operatorSpecified = true;
                                    invAdjType.searchValue = new String[] { "_inventoryAdjustment" };
                                    invadjtsb.type = invAdjType;

                                    SearchDateField invAdjDate = new SearchDateField();
                                    invAdjDate.@operator = SearchDateFieldOperator.within;
                                    invAdjDate.operatorSpecified = true;
                                    invAdjDate.searchValueSpecified = true;
                                    invAdjDate.searchValue2Specified = true;
                                    invAdjDate.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    invAdjDate.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    invadjtsb.lastModifiedDate = invAdjDate;

                                    invadjts.basic = invadjtsb;
                                    invadjtsa.criteria = invadjts;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(invadjtsa);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.SRInventoryAdjustments(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(invadjtsa);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Return Authorization (Receive)
                                case "NS-RETURN AUTHORIZATION (RECEIVE)":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: NS-RETURN AUTHORIZATION (RECEIVE) within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced rr1tsa = new TransactionSearchAdvanced();
                                    TransactionSearch rr1ts = new TransactionSearch();
                                    TransactionSearchBasic rr1tsb = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField raStatus = new SearchEnumMultiSelectField();
                                    raStatus.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    raStatus.operatorSpecified = true;
                                    raStatus.searchValue = new String[] { "returnAuthorizationPendingReceipt", "returnAuthorizationPartiallyReceived", "_returnAuthorizationPendingRefund", "returnAuthorizationPendingRefundPartiallyReceived" };
                                    rr1tsb.status = raStatus;

                                    //Include Subsidiary = 'GMY' and LOB = 'TRADE' - WY-20.OCT.2014
                                    //RecordRef refRAET = new RecordRef(); 
                                    //var qTrade = (from q in entities.map_businesschannel
                                    //           where q.mb_imas_businessChannel_code == "ET"
                                    //           select q.mb_businessChannel_internalID).FirstOrDefault();

                                    //if (qTrade != null)
                                    //{
                                    //    refRAET.internalId = qTrade;//Trade
                                    //}

                                    RecordRef refRATrade = new RecordRef();
                                    refRATrade.internalId = @Resource.LOB_TRADE_INTERNALID;

                                    RecordRef refRAEDUGeneral = new RecordRef();
                                    refRAEDUGeneral.internalId = @Resource.LOB_EDUCATIONGENERAL_INTERNALID;

                                    //Added LOB OS by Brash Developer on 21-Apr-2021 Start

                                    RecordRef refRAOnlineStore = new RecordRef();
                                    refRAOnlineStore.internalId = @Resource.LOB_ONLINESTORE_INTERNALID;

                                    //End

                                    SearchMultiSelectField raBusinessChannel = new SearchMultiSelectField();
                                    raBusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    raBusinessChannel.operatorSpecified = true;
                                    raBusinessChannel.searchValue = new RecordRef[] { refRATrade, refRAEDUGeneral, refRAOnlineStore };
                                    rr1tsb.@class = raBusinessChannel;

                                    RecordRef refRAGMY = new RecordRef();
                                    refRAGMY.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;

                                    SearchMultiSelectField raSubsidiary = new SearchMultiSelectField();
                                    raSubsidiary.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    raSubsidiary.operatorSpecified = true;
                                    raSubsidiary.searchValue = new RecordRef[] { refRAGMY };
                                    rr1tsb.subsidiary = raSubsidiary;

                                    SearchMultiSelectCustomField raSync = new SearchMultiSelectCustomField();
                                    raSync.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    raSync.operatorSpecified = true;
                                    raSync.scriptId = "custbody_wms_field";
                                    ListOrRecordRef raListOrRecordRef = new ListOrRecordRef();
                                    raListOrRecordRef.internalId = "1";
                                    raListOrRecordRef.typeId = "136";
                                    raSync.searchValue = new ListOrRecordRef[] { raListOrRecordRef };
                                    SearchCustomField[] raScf = new SearchCustomField[] { raSync };
                                    rr1tsb.customFieldList = raScf;

                                    SearchDateField raDate = new SearchDateField();
                                    raDate.@operator = SearchDateFieldOperator.within;
                                    raDate.operatorSpecified = true;
                                    raDate.searchValueSpecified = true;
                                    raDate.searchValue2Specified = true;
                                    raDate.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    raDate.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    rr1tsb.lastModifiedDate = raDate;

                                    rr1ts.basic = rr1tsb;
                                    rr1tsa.criteria = rr1ts;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(rr1tsa);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.ReturnAuthorizationsReceive(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(rr1tsa);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Return Authorization (Refund)
                                /*case "NS-RETURN AUTHORIZATION (REFUND)":
                                    this.DataFromNetsuiteLog.Info("Asnyc search return authorization within " + dateRangeFrom + " to " + dateRangeTo);
	                                TransactionSearchAdvanced rr2tsa = new TransactionSearchAdvanced();
	                                TransactionSearch rr2ts = new TransactionSearch();
	                                TransactionSearchBasic rr2tsb = new TransactionSearchBasic();

	                                SearchEnumMultiSelectField raStatus2 = new SearchEnumMultiSelectField();
                                    raStatus2.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    raStatus2.operatorSpecified = true;
                                    raStatus2.searchValue = new String[] { "returnAuthorizationPendingRefundPartiallyReceived" };
                                    rr2tsb.status = raStatus2;

	                                SearchDateField raDate2 = new SearchDateField();
                                    raDate2.@operator = SearchDateFieldOperator.within;
                                    raDate2.operatorSpecified = true;
                                    raDate2.searchValueSpecified = true;
                                    raDate2.searchValue2Specified = true;
                                    //raDate2.searchValue = DateTime.Now.AddDays(-360);
                                    //raDate2.searchValue2 = DateTime.Now;
                                    raDate2.searchValue = DateTime.Parse(dateRangeFrom);
                                    raDate2.searchValue2 = DateTime.Parse(dateRangeTo);
                                    rr2tsb.lastModifiedDate = raDate2;

                                    rr2ts.basic = rr2tsb;
                                    rr2tsa.criteria = rr2ts;
                                    job = service.asyncSearch(rr2tsa);
	                                jobID = job.jobId;
                                    break;*/
                                #endregion
                                #region Cash Sales
                                case "NS-CASH SALES":
                                    this.DataFromNetsuiteLog.Info("TransachtionAsyncSearch: NS-CASH SALES within " + dateRangeFrom + " to " + dateRangeTo);
                                    TransactionSearchAdvanced cstsa = new TransactionSearchAdvanced();
                                    TransactionSearch csts = new TransactionSearch();
                                    TransactionSearchBasic cstsb = new TransactionSearchBasic();

                                    SearchEnumMultiSelectField csType = new SearchEnumMultiSelectField();
                                    csType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    csType.operatorSpecified = true;
                                    csType.searchValue = new String[] { "_cashSale" };
                                    cstsb.type = csType;

                                    RecordRef refCSET = new RecordRef();

                                    var q35 = (from q in entities.map_businesschannel
                                               where q.mb_imas_businessChannel_code == "ET"
                                               select q.mb_businessChannel_internalID).FirstOrDefault();

                                    if (q35 != null)
                                    {
                                        refCSET.internalId = q35;//Trade or Book Clubs
                                    }

                                    SearchMultiSelectField csBusinessChannel = new SearchMultiSelectField();
                                    csBusinessChannel.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    csBusinessChannel.operatorSpecified = true;
                                    csBusinessChannel.searchValue = new RecordRef[] { refCSET };
                                    cstsb.@class = csBusinessChannel;

                                    RecordRef refCSGMY = new RecordRef();
                                    //refCSGMY.internalId = @Resource.BCAS_DUMMYSALES_MY;
                                    refCSGMY.internalId = @Resource.SUBSIDIARY_INTERNALID_MY; //WY-20.OCT.2014

                                    SearchMultiSelectField csSubsidiary = new SearchMultiSelectField();
                                    csSubsidiary.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    csSubsidiary.operatorSpecified = true;
                                    csSubsidiary.searchValue = new RecordRef[] { refCSGMY };
                                    cstsb.subsidiary = csSubsidiary;

                                    SearchMultiSelectCustomField csSync = new SearchMultiSelectCustomField();
                                    csSync.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    csSync.operatorSpecified = true;
                                    csSync.scriptId = "custbody_wms_field";
                                    ListOrRecordRef csListOrRecordRef = new ListOrRecordRef();
                                    csListOrRecordRef.internalId = "1";
                                    csListOrRecordRef.typeId = "136";
                                    csSync.searchValue = new ListOrRecordRef[] { csListOrRecordRef };
                                    SearchCustomField[] csScf = new SearchCustomField[] { csSync };
                                    cstsb.customFieldList = csScf;

                                    SearchDateField csDate = new SearchDateField();
                                    csDate.@operator = SearchDateFieldOperator.within;
                                    csDate.operatorSpecified = true;
                                    csDate.searchValueSpecified = true;
                                    csDate.searchValue2Specified = true;
                                    csDate.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    csDate.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    cstsb.lastModifiedDate = csDate;

                                    csts.basic = cstsb;
                                    cstsa.criteria = csts;

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(cstsa);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.CashSales(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(cstsa);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Conf Item
                                case "CONF-ITEM":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-ITEM within " + dateRangeFrom + " to " + dateRangeTo);
                                    ItemSearchBasic itemSearch = new ItemSearchBasic();

                                    SearchEnumMultiSelectField itemType = new SearchEnumMultiSelectField();
                                    itemType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    itemType.operatorSpecified = true;
                                    itemType.searchValue = new String[] { "_inventoryItem" };
                                    itemSearch.type = itemType;

                                    RecordRef refGMYitem = new RecordRef();
                                    refGMYitem.internalId = @Resource.SUBSIDIARY_INTERNALID_MY;
                                    SearchMultiSelectField itemSubsidiary = new SearchMultiSelectField();
                                    itemSubsidiary.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemSubsidiary.operatorSpecified = true;
                                    itemSubsidiary.searchValue = new RecordRef[] { refGMYitem };
                                    itemSearch.subsidiary = itemSubsidiary;

                                    SearchDateField itemDate = new SearchDateField();
                                    itemDate.@operator = SearchDateFieldOperator.within;
                                    itemDate.operatorSpecified = true;
                                    itemDate.searchValueSpecified = true;
                                    itemDate.searchValue2Specified = true;
                                    itemDate.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    itemDate.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    //itemSearch.created = itemDate; 
                                    itemSearch.lastModifiedDate = itemDate; //Change to using LastModifiedDate - WY-08.OCT.2014

                                    SearchMultiSelectCustomField itemSync = new SearchMultiSelectCustomField();
                                    itemSync.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemSync.operatorSpecified = true;
                                    itemSync.scriptId = "custitem_wms_item_field";
                                    ListOrRecordRef itemListOrRecordRef = new ListOrRecordRef();
                                    itemListOrRecordRef.internalId = "1";
                                    itemListOrRecordRef.typeId = "187";//"136";
                                    itemSync.searchValue = new ListOrRecordRef[] { itemListOrRecordRef };
                                    SearchCustomField[] itemScf = new SearchCustomField[] { itemSync };
                                    itemSearch.customFieldList = itemScf;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(itemSearch);
                                    jobID = job.jobId;

                                    ////Search for Internal ID only - WY-21.OCT.2014
                                    //ItemSearchBasic itemSearch = new ItemSearchBasic();
                                    //var qItem = (from qi in entities.tosyncitems
                                    //             where qi.item_internalID != null && qi.item_internalID != ""
                                    //             && qi.isDone == "0"
                                    //             select qi.item_internalID).Distinct().ToList();

                                    //if (qItem.Count() > 0)
                                    //{
                                    //    RecordRef[] refItemInternalID = new RecordRef[qItem.Count()];
                                    //    for (int i = 0; i < qItem.Count(); i++)
                                    //    {
                                    //        RecordRef tempItemRef = new RecordRef();
                                    //        tempItemRef.internalId = qItem[i];
                                    //        refItemInternalID[i] = tempItemRef;
                                    //    }

                                    //    SearchMultiSelectField itemInternalID = new SearchMultiSelectField();
                                    //    itemInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    //    itemInternalID.operatorSpecified = true;
                                    //    itemInternalID.searchValue = refItemInternalID;

                                    //    itemSearch.internalId = itemInternalID;
                                    //    job = service.asyncSearch(itemSearch);
                                    //    jobID = job.jobId;
                                    //} 
                                    break;
                                #endregion

                                //ANET-35 Item master missing all Grolier Subsidiary
                                #region Conf Item Direct Sales
                                case "CONF-ITEM DS":

                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-ITEM DS within " + dateRangeFrom + " to " + dateRangeTo);
                                    ItemSearchBasic itemSearchDS = new ItemSearchBasic();

                                    SearchEnumMultiSelectField itemTypeDS = new SearchEnumMultiSelectField();
                                    itemTypeDS.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    itemTypeDS.operatorSpecified = true;
                                    itemTypeDS.searchValue = new String[] { "_inventoryItem" };
                                    itemSearchDS.type = itemTypeDS;

                                    RecordRef refGMYitem1 = new RecordRef();
                                    refGMYitem1.internalId = "3";//Grolier Malaysia

                                    RecordRef refGSGitem = new RecordRef();
                                    refGSGitem.internalId = @Resource.SUBSIDIARY_INTERNALID_SG; //"5" Grolier Singapore

                                    RecordRef refGTHitem = new RecordRef();
                                    refGTHitem.internalId = @Resource.SUBSIDIARY_INTERNALID_TH; //"6" Grolier Thailand

                                    RecordRef refGIDitem = new RecordRef();
                                    refGIDitem.internalId = @Resource.BCAS_ID_SUBSIDAIRY; //"7" Grolier Indonesia

                                    RecordRef refGPHitem = new RecordRef();
                                    refGPHitem.internalId = "8"; //Grolier Philippines 

                                    RecordRef refGINitem = new RecordRef();
                                    refGINitem.internalId = "9";//Grolier India 

                                    SearchMultiSelectField itemSubsidiaryDS = new SearchMultiSelectField();
                                    itemSubsidiaryDS.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemSubsidiaryDS.operatorSpecified = true;
                                    itemSubsidiaryDS.searchValue = new RecordRef[] { refGMYitem1, refGSGitem, refGTHitem, refGIDitem, refGPHitem, refGINitem };
                                    itemSearchDS.subsidiary = itemSubsidiaryDS;

                                    SearchDateField itemDateDS = new SearchDateField();
                                    itemDateDS.@operator = SearchDateFieldOperator.within;
                                    itemDateDS.operatorSpecified = true;
                                    itemDateDS.searchValueSpecified = true;
                                    itemDateDS.searchValue2Specified = true;
                                    itemDateDS.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    itemDateDS.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    itemSearchDS.lastModifiedDate = itemDateDS; //Change to using LastModifiedDate - WY-08.OCT.2014

                                    SearchMultiSelectCustomField itemSyncDS = new SearchMultiSelectCustomField();
                                    itemSyncDS.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemSyncDS.operatorSpecified = true;
                                    itemSyncDS.scriptId = "custitem_wms_item_field";
                                    ListOrRecordRef itemListOrRecordRefDS = new ListOrRecordRef();
                                    itemListOrRecordRefDS.internalId = "1";
                                    itemListOrRecordRefDS.typeId = "187";
                                    itemSyncDS.searchValue = new ListOrRecordRef[] { itemListOrRecordRefDS };

                                    SearchMultiSelectCustomField itemLOBRights = new SearchMultiSelectCustomField();
                                    itemLOBRights.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemLOBRights.operatorSpecified = true;
                                    itemLOBRights.scriptId = "custitem_business_channel_rights";
                                    ListOrRecordRef itemListOrRecordRefLOB = new ListOrRecordRef();
                                    itemListOrRecordRefLOB.internalId = "105";
                                    itemListOrRecordRefLOB.typeId = "-101";
                                    itemLOBRights.searchValue = new ListOrRecordRef[] { itemListOrRecordRefLOB };

                                    SearchCustomField[] itemScfDS = new SearchCustomField[] { itemSyncDS, itemLOBRights };
                                    itemSearchDS.customFieldList = itemScfDS;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(itemSearchDS);
                                    jobID = job.jobId;

                                    break;
                                #endregion

                                //Added to get daily created items - WY-17.OCT.2014
                                #region Conf New Item
                                case "CONF-NEW ITEM":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-NEW ITEM within " + dateRangeFrom + " to " + dateRangeTo);
                                    ItemSearchBasic itemSearch2 = new ItemSearchBasic();

                                    SearchEnumMultiSelectField itemType2 = new SearchEnumMultiSelectField();
                                    itemType2.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                    itemType2.operatorSpecified = true;
                                    itemType2.searchValue = new String[] { "_inventoryItem" };
                                    itemSearch2.type = itemType2;

                                    SearchDateField itemDate2 = new SearchDateField();
                                    itemDate2.@operator = SearchDateFieldOperator.within;
                                    itemDate2.operatorSpecified = true;
                                    itemDate2.searchValueSpecified = true;
                                    itemDate2.searchValue2Specified = true;
                                    itemDate2.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    itemDate2.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    itemSearch2.created = itemDate2;

                                    SearchMultiSelectCustomField itemSync2 = new SearchMultiSelectCustomField();
                                    itemSync2.@operator = SearchMultiSelectFieldOperator.anyOf;
                                    itemSync2.operatorSpecified = true;
                                    itemSync2.scriptId = "custitem_wms_item_field";
                                    ListOrRecordRef itemListOrRecordRef2 = new ListOrRecordRef();
                                    itemListOrRecordRef2.internalId = "1";
                                    itemListOrRecordRef2.typeId = "187";//"136";
                                    itemSync2.searchValue = new ListOrRecordRef[] { itemListOrRecordRef2 };
                                    SearchCustomField[] itemScf2 = new SearchCustomField[] { itemSync2 };
                                    itemSearch2.customFieldList = itemScf2;

                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(itemSearch2);
                                    jobID = job.jobId;
                                    break;
                                #endregion
                                //To search null item at netsuite_pritem - WY-26.NOV.2014 
                                #region Conf Null Item
                                case "CONF-NULL ITEM":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-NULL ITEM within " + dateRangeFrom + " to " + dateRangeTo);
                                    ItemSearchBasic itemNullSearch = new ItemSearchBasic();
                                    var qItem = (from qi in entities.netsuite_pritem
                                                 where qi.nspi_createdDate > rangeFrom && qi.nspi_createdDate <= rangeTo
                                                 select qi.nspi_item_internalID).Distinct().ToList();

                                    if (qItem.Count() > 0)
                                    {
                                        RecordRef[] refItemInternalID = new RecordRef[qItem.Count()];
                                        for (int i = 0; i < qItem.Count(); i++)
                                        {
                                            RecordRef tempItemRef = new RecordRef();
                                            tempItemRef.internalId = qItem[i];
                                            refItemInternalID[i] = tempItemRef;
                                        }

                                        SearchMultiSelectField itemInternalID = new SearchMultiSelectField();
                                        itemInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        itemInternalID.operatorSpecified = true;
                                        itemInternalID.searchValue = refItemInternalID;

                                        itemNullSearch.internalId = itemInternalID;
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(itemNullSearch);
                                        jobID = job.jobId;
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                #region Conf ID Item
                                case "CONF-ID ITEM":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-NULL ITEM within " + dateRangeFrom + " to " + dateRangeTo);
                                    ItemSearchBasic itemIDSearch = new ItemSearchBasic();
                                    string[] IDItem = { "9050" };

                                    if (IDItem.Count() > 0)
                                    {
                                        RecordRef[] IDrefItemInternalID = new RecordRef[IDItem.Count()];
                                        for (int i = 0; i < IDItem.Count(); i++)
                                        {
                                            RecordRef tempItemRef = new RecordRef();
                                            tempItemRef.internalId = IDItem[i];
                                            IDrefItemInternalID[i] = tempItemRef;
                                        }

                                        SearchMultiSelectField IDitemInternalID = new SearchMultiSelectField();
                                        IDitemInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        IDitemInternalID.operatorSpecified = true;
                                        IDitemInternalID.searchValue = IDrefItemInternalID;

                                        itemIDSearch.internalId = IDitemInternalID;
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(itemIDSearch);
                                        jobID = job.jobId;
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                #region Conf Business Channel
                                case "CONF-BUSINESS CHANNEL":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-BUSINESS CHANNEL within " + dateRangeFrom + " to " + dateRangeTo);
                                    ClassificationSearchBasic classSearch = new ClassificationSearchBasic();

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(classSearch);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.ConfBusinessChannel(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(classSearch);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Conf Subsidiary
                                case "CONF-SUBSIDIARY":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-SUBSIDIARY within " + dateRangeFrom + " to " + dateRangeTo);
                                    SubsidiarySearchBasic subSearch = new SubsidiarySearchBasic();

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(subSearch);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.ConfSubsidiary(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(subSearch);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Conf Location
                                case "CONF-LOCATION":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-LOCATION within " + dateRangeFrom + " to " + dateRangeTo);
                                    LocationSearchBasic locSearch = new LocationSearchBasic();

                                    if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        SearchResult sr = netsuiteService.search(locSearch);
                                        if (sr.recordList.Count() > 0)
                                        {
                                            //runSynchronous.ConfLocation(entities, r, sr);
                                        }
                                    }
                                    else
                                    {
                                        //TBA
                                        netsuiteService.tokenPassport = createTokenPassport();
                                        job = netsuiteService.asyncSearch(locSearch);
                                        jobID = job.jobId;
                                    }
                                    break;
                                #endregion
                                #region Conf Customer
                                case "CONF-CUSTOMER":
                                    this.DataFromNetsuiteLog.Info("TransactionAsyncSearch: CONF-CUSTOMER within " + dateRangeFrom + " to " + dateRangeTo);
                                    CustomerSearchBasic custSearch = new CustomerSearchBasic();
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(custSearch);
                                    jobID = job.jobId;
                                    break;
                                #endregion
                                //To get forwarder Address - WY-05.NOV.2014
                                #region Forwarder Address
                                case "NS-FORWARDER ADDRESS":
                                    CustomRecordSearch custRecSearch = new CustomRecordSearch();
                                    CustomRecordSearchBasic custRecSearchBasic = new CustomRecordSearchBasic();

                                    RecordRef recRef = new RecordRef();
                                    recRef.internalId = @Resource.CUSTOMREC_FORWADD_INTERNALID;//177 
                                    custRecSearchBasic.recType = recRef;

                                    SearchDateField srcLastModified = new SearchDateField();
                                    srcLastModified.@operator = SearchDateFieldOperator.within;
                                    srcLastModified.operatorSpecified = true;
                                    srcLastModified.searchValueSpecified = true;
                                    srcLastModified.searchValue2Specified = true;
                                    srcLastModified.searchValue = DateTime.Parse(dateRangeFrom).AddHours(-8);
                                    srcLastModified.searchValue2 = DateTime.Parse(dateRangeTo).AddHours(-8);
                                    custRecSearchBasic.lastModified = srcLastModified;

                                    custRecSearch.basic = custRecSearchBasic;
                                    //TBA
                                    netsuiteService.tokenPassport = createTokenPassport();
                                    job = netsuiteService.asyncSearch(custRecSearch);
                                    jobID = job.jobId;
                                    break;
                                #endregion
                                //Testing - WY-07.OCT.2014
                                #region PATCH DATA
                                case "NS-PATCHDATA":
                                    Int32 tolQueryPatch = 0;
                                    string connStr2 = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                                    MySqlConnection mysqlCon2 = new MySqlConnection(connStr2);
                                    mysqlCon2.Open();
                                    //var queryPatch = "SELECT DISTINCT nt1_moNo_internalID FROM netsuite_newso WHERE nt1_createdDate >= '2014-11-01' " +
                                    //                "AND (nt1_custID IS NULL OR nt1_custID = '') ";

                                    var queryPatch = "select moNo_InternalID from patchmo where isRun = 0";
                                    //var queryPatch = "select distinct nt2_mono_internalId from netsuite_syncso so join (select sum(nt1_fulfilledQty) AS nt1_fulfilledQty,nt1_mono,nt1_itemID,nt1_status from netsuite_newso " +
                                    //                 " group by nt1_mono,nt1_itemID) as nso on so.nt2_mono =  nso.nt1_mono and nso.nt1_itemID = nt2_itemID  WHERE nso.nt1_status not in ('PENDING FULFILLMENT')" +
                                    //                 " and nt2_mono NOt IN ('SO-MY001940','SO-MY00709','SO-MY00705','SO-MY00689','SO-MY00594') group by nt2_mono,nt2_itemID " +
                                    //                 " having sum(nso.nt1_fulfilledQty) < sum(nt2_wmsfulfilledqty)";

                                    MySqlCommand cmd3 = new MySqlCommand(queryPatch, mysqlCon2);
                                    MySqlDataReader dtr3 = cmd3.ExecuteReader();
                                    List<TempBackOrder> patchList = new List<TempBackOrder>();
                                    while (dtr3.Read())
                                    {
                                        TempBackOrder backOrder = new TempBackOrder();
                                        backOrder.moNoInternalID = (dtr3.GetValue(0) == DBNull.Value) ? String.Empty : dtr3.GetString(0);
                                        patchList.Add(backOrder);
                                        tolQueryPatch++;
                                    }


                                    if (tolQueryPatch > 0)
                                    {
                                        TransactionSearchAdvanced sotPatchsa = new TransactionSearchAdvanced();
                                        TransactionSearch sotPatchs = new TransactionSearch();
                                        TransactionSearchBasic sotPatchsb = new TransactionSearchBasic();

                                        SearchEnumMultiSelectField soPatchType = new SearchEnumMultiSelectField();
                                        soPatchType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                        soPatchType.operatorSpecified = true;
                                        soPatchType.searchValue = new String[] { "_salesOrder" };
                                        sotPatchsb.type = soPatchType;

                                        RecordRef[] refPatchInternalID = new RecordRef[tolQueryPatch];
                                        for (int i = 0; i < tolQueryPatch; i++)
                                        {
                                            RecordRef tempRef = new RecordRef();
                                            tempRef.internalId = patchList[i].moNoInternalID.ToString();
                                            refPatchInternalID[i] = tempRef;
                                        }

                                        SearchMultiSelectField soPatchInternalID = new SearchMultiSelectField();
                                        soPatchInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soPatchInternalID.operatorSpecified = true;
                                        soPatchInternalID.searchValue = refPatchInternalID;
                                        sotPatchsb.internalId = soPatchInternalID;

                                        sotPatchs.basic = sotPatchsb;
                                        sotPatchsa.criteria = sotPatchs;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotPatchsa);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotPatchsa);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                                //Used for Patch SYNCED COL when live only.
                                #region NS-SYNCEDCOL
                                case "NS-SYNCEDCOL":
                                    Int32 tolQuerySynced = 0;
                                    string connStr3 = ConfigurationManager.ConnectionStrings["mysql2"].ConnectionString;
                                    MySqlConnection mysqlCon3 = new MySqlConnection(connStr3);
                                    mysqlCon3.Open();

                                    //var querySynced = "SELECT nt2_mono,nt2_mono_internalID,sum(nt2_OrdQty) AS OrderQty,sum(nt2_qtyforwms) AS QtyForWMS,SUM(nt2_wmsfulfilledqty) AS WMSFulfilledQty " +
                                    //                  " FROM netsuite_syncso WHERE nt2_status NOT IN ('BILLED','CLOSED','DELETED') GROUP BY nt2_mono HAVING SUM(nt2_OrdQty) <> SUM(nt2_wmsfulfilledqty) ";

                                    //var querySynced = "select moNo_InternalID from patchmo where isRun = 0";
                                    var querySynced = "select distinct nt1_moNo_internalID from netsuite_newso WHERE nt1_mono in ('SO-SG00311','SO-SG002059','SO-SG002981')";

                                    MySqlCommand cmd4 = new MySqlCommand(querySynced, mysqlCon3);
                                    MySqlDataReader dtr4 = cmd4.ExecuteReader();
                                    List<TempBackOrder> syncedList = new List<TempBackOrder>();
                                    while (dtr4.Read())
                                    {
                                        TempBackOrder backOrder = new TempBackOrder();
                                        backOrder.moNoInternalID = (dtr4.GetValue(0) == DBNull.Value) ? String.Empty : dtr4.GetString(0);
                                        syncedList.Add(backOrder);
                                        tolQuerySynced++;
                                    }

                                    if (tolQuerySynced > 0)
                                    {
                                        TransactionSearchAdvanced sotSyncedsa = new TransactionSearchAdvanced();
                                        TransactionSearch sotSynceds = new TransactionSearch();
                                        TransactionSearchBasic sotSyncedsb = new TransactionSearchBasic();

                                        SearchEnumMultiSelectField soSyncedType = new SearchEnumMultiSelectField();
                                        soSyncedType.@operator = SearchEnumMultiSelectFieldOperator.anyOf;
                                        soSyncedType.operatorSpecified = true;
                                        soSyncedType.searchValue = new String[] { "_salesOrder" };
                                        sotSyncedsb.type = soSyncedType;

                                        RecordRef[] refSyncedInternalID = new RecordRef[tolQuerySynced];
                                        for (int i = 0; i < tolQuerySynced; i++)
                                        {
                                            RecordRef tempRef = new RecordRef();
                                            tempRef.internalId = syncedList[i].moNoInternalID.ToString();
                                            refSyncedInternalID[i] = tempRef;
                                        }

                                        SearchMultiSelectField soSycnedInternalID = new SearchMultiSelectField();
                                        soSycnedInternalID.@operator = SearchMultiSelectFieldOperator.anyOf;
                                        soSycnedInternalID.operatorSpecified = true;
                                        soSycnedInternalID.searchValue = refSyncedInternalID;
                                        sotSyncedsb.internalId = soSycnedInternalID;

                                        sotSynceds.basic = sotSyncedsb;
                                        sotSyncedsa.criteria = sotSynceds;

                                        if (@Resource.RUNSYNCHRONOUS.Equals("YES"))
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            SearchResult sr = netsuiteService.search(sotSyncedsa);
                                            if (sr.recordList.Count() > 0)
                                            {
                                            }
                                        }
                                        else
                                        {
                                            //TBA
                                            netsuiteService.tokenPassport = createTokenPassport();
                                            job = netsuiteService.asyncSearch(sotSyncedsa);
                                            jobID = job.jobId;
                                        }
                                    }
                                    else
                                    {
                                        jobID = "NO-DATA";
                                    }
                                    break;
                                #endregion
                            }
                            //}
                            //else
                            /*{
                                AsyncStatusResult jobstatus = service.checkAsyncStatus(jobID);
                                jobID = jobstatus.status.ToString();  //use jobID to store STATUS
                                this.DataFromNetsuiteLog.Info("Async search within " + dateRangeFrom + " to " + dateRangeTo);
                                this.DataFromNetsuiteLog.Info("Async search status: " + jobstatus.status.ToString().ToUpper());
                            }*/
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error!! ");
                        Console.WriteLine(ex.Message);
                        if (ex.Message.Contains("a session at a time"))
                        {
                            this.DataFromNetsuiteLog.Debug("PushNetsuite TransachtionAsyncSearch Exception: " + ex.ToString());
                        }
                        else
                        {
                            this.DataFromNetsuiteLog.Error("PushNetsuite TransachtionAsyncSearch Exception: " + ex.ToString());
                        }
                    }
                    /////
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("a session at a time"))
                    {
                        this.DataFromNetsuiteLog.Debug("PushNetsuite TransachtionAsyncSearch Exception: " + ex.ToString());
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Error("PushNetsuite TransachtionAsyncSearch Exception: " + ex.ToString());
                    }
                }
                finally
                {
                  //  Status logoutStatus = (service.logout()).status;
                    this.DataFromNetsuiteLog.Info("PushNetsuite TransachtionAsyncSearch: Logout Netsuite ");
                }
            }
            return jobID;
        }
        public String CheckAsyncStatus(NetSuiteService service, String jobID, RequestNetsuiteEntity r)
        {
            //TBA
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DataCenterAwareNetSuiteService netsuiteService = new DataCenterAwareNetSuiteService(account);
            try
            {
                string loginEmail = "";
                if (r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER") ||
                    r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 2") ||
                    r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 3") ||
                    r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 4") ||
                    r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 5") ||
                    r.rn_sche_transactionType.Equals("NS-LATEST SALES ORDER 6"))

                //TBA
                {
                    loginEmail = @Resource.NETSUITE_LOGIN_EMAIL_PULL;
                    tokenId = @Resource.ASIA_WEBSERVICE_4_TOKEN_ID;
                    tokenSecret = @Resource.ASIA_WEBSERVICE_4_TOKEN_SECRET;
                }
                else
                {
                    loginEmail = @Resource.NETSUITE_LOGIN_EMAIL;
                    tokenId = @Resource.ASIA_WEBSERVICE_TOKEN_ID;
                    tokenSecret = @Resource.ASIA_WEBSERVICE_TOKEN_SECRET;
                }
                ItemSearchBasic basic = new ItemSearchBasic()
                {
                    internalId = new SearchMultiSelectField()
                    {
                        @operator = SearchMultiSelectFieldOperator.anyOf,
                        operatorSpecified = true,
                        searchValue = new RecordRef[] {
                    new RecordRef() {
                    internalId = "14943"
                         }
                    }
                    }
                };

                DateTime rangeFrom = Convert.ToDateTime(r.rn_rangeFrom);
                DateTime rangeTo = Convert.ToDateTime(r.rn_rangeTo);
                String dateRangeFrom = Convert.ToDateTime(r.rn_rangeFrom).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                String dateRangeTo = Convert.ToDateTime(r.rn_rangeTo).ToString("yyyy-MM-dd HH:mm:ss") + "Z";
                AsyncStatusResult job = new AsyncStatusResult();

                //SearchPreferences sp = new SearchPreferences();
                //sp.bodyFieldsOnly = false;
                //sp.pageSize = 1000;
                //sp.pageSizeSpecified = true;

                //service.searchPreferences = sp;
                //service.Timeout = 100000000;
                //service.CookieContainer = new CookieContainer();
                //ApplicationInfo appinfo = new ApplicationInfo();
                //appinfo.applicationId = @Resource.NETSUITE_LOGIN_APPLICATIONID;
                //service.applicationInfo = appinfo;

                //Passport passport = new Passport();
                //passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
                //passport.email = loginEmail;

                //RecordRef role = new RecordRef();
                //role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

                //passport.role = role;
                ////kang get netsuitepassword from DB
                ////passport.password = @Resource.NETSUITE_LOGIN_PASSWORD;
                //passport.password = getNetsuitePassword(loginEmail);

                //Status status = service.login(passport).status;

                try
                {
                    //TBA
                    Console.WriteLine("Success");
                    netsuiteService.tokenPassport = createTokenPassport();
                    SearchResult status = netsuiteService.search(basic);
                    if (status.status.isSuccess == true)
                    {

                        this.DataFromNetsuiteLog.Debug("PushNetsuite CheckAsyncStatus: Login Netsuite success.");
                        //TBA
                        netsuiteService.tokenPassport = createTokenPassport();
                        AsyncStatusResult jobstatus = netsuiteService.checkAsyncStatus(jobID);
                        jobID = jobstatus.status.ToString();  //use jobID to store STATUS
                        this.DataFromNetsuiteLog.Info("PushNetsuite CheckAsyncStatus: Async search within " + dateRangeFrom + " to " + dateRangeTo);
                        this.DataFromNetsuiteLog.Info("PushNetsuite CheckAsyncStatus: Async search status: " + jobstatus.status.ToString().ToUpper());
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error!! ");
                    Console.WriteLine(ex.Message);
                    if (ex.Message.Contains("a session at a time"))
                    {
                        this.DataFromNetsuiteLog.Debug("PushNetsuite CheckAsyncStatus Exception: " + ex.ToString());
                    }
                    else
                    {
                        this.DataFromNetsuiteLog.Error("PushNetsuite CheckAsyncStatus Exception: " + ex.ToString());
                    }
                }

                //if (status.isSuccess == true)
                //{
                //    this.DataFromNetsuiteLog.Debug("PushNetsuite CheckAsyncStatus: Login Netsuite success.");
                //    AsyncStatusResult jobstatus = service.checkAsyncStatus(jobID);
                //    jobID = jobstatus.status.ToString();  //use jobID to store STATUS
                //    this.DataFromNetsuiteLog.Info("PushNetsuite CheckAsyncStatus: Async search within " + dateRangeFrom + " to " + dateRangeTo);
                //    this.DataFromNetsuiteLog.Info("PushNetsuite CheckAsyncStatus: Async search status: " + jobstatus.status.ToString().ToUpper());
                //}                
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("a session at a time"))
                {
                    this.DataFromNetsuiteLog.Debug("PushNetsuite CheckAsyncStatus Exception: " + ex.ToString());
                }
                else if(ex.Message.Contains("You have specified an invalid Job"))
                {
                    this.DataFromNetsuiteLog.Error("PushNetsuite CheckAsyncStatus Exception: " + ex.ToString() + ">>>" + jobID);
                    jobID = "INVALID JOB ID";
                }
                else 
                {
                    this.DataFromNetsuiteLog.Error("PushNetsuite CheckAsyncStatus Exception: " + ex.ToString());
                }
            }
            finally
            {
              //  Status logoutStatus = (service.logout()).status;
                this.DataFromNetsuiteLog.Info("PushNetsuite CheckAsyncStatus: Logout Netsuite " );
            }
            return jobID;
            //TBA
        }
        #endregion

        #region General
        /*
        public Boolean login()
        {
            SearchPreferences sp = new SearchPreferences();
            sp.bodyFieldsOnly = false;
            sp.pageSize = 1000;
            sp.pageSizeSpecified = true;

            service.searchPreferences = sp;
            service.Timeout = 100000000;
            service.CookieContainer = new CookieContainer();

            Passport passport = new Passport();
            passport.account = @Resource.NETSUITE_LOGIN_ACCOUNT;
            passport.email = @Resource.NETSUITE_LOGIN_EMAIL;

            RecordRef role = new RecordRef();
            role.internalId = @Resource.NETSUITE_LOGIN_ROLE_INTERNALID;

            passport.role = role;
            passport.password = @Resource.NETSUITE_LOGIN_PASSWORD;

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
                this.DataFromNetsuiteLog.Error(ex.ToString());
            }
        }
        */
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
        private TokenPassport createTokenPassport()
        {
            string nonce = computeNonce();
            long timestamp = computeTimestamp();
            TokenPassportSignature signature = computeSignature(account, consumerKey, consumerSecret, tokenId, tokenSecret, nonce, timestamp);

            TokenPassport tokenPassport = new TokenPassport();
            tokenPassport.account = account;
            tokenPassport.consumerKey = consumerKey;
            tokenPassport.token = tokenId;
            tokenPassport.nonce = nonce;
            tokenPassport.timestamp = timestamp;
            tokenPassport.signature = signature;
            return tokenPassport;
        }

        private static string computeNonce()
        {
            RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();
            byte[] data = new byte[20];
            rng.GetBytes(data);
            int value = Math.Abs(BitConverter.ToInt32(data, 0));
            return value.ToString();
        }

        private static long computeTimestamp()
        {
            return ((long)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds);
        }

        private static TokenPassportSignature computeSignature(string compId, string consumerKey, string consumerSecret, string tokenId, string tokenSecret, string nonce, long timestamp)
        {
            string baseString = compId + "&" + consumerKey + "&" + tokenId + "&" + nonce + "&" + timestamp;
            string key = consumerSecret + "&" + tokenSecret;
            string signature = "";
            var encoding = new System.Text.ASCIIEncoding();
            byte[] keyBytes = encoding.GetBytes(key);
            byte[] baseStringBytes = encoding.GetBytes(baseString);
            //using (var hmacSha1 = new HMACSHA1(keyBytes))
            //{
            //    byte[] hashBaseString = hmacSha1.ComputeHash(baseStringBytes);
            //    signature = Convert.ToBase64String(hashBaseString);
            //}
            //TokenPassportSignature sign = new TokenPassportSignature();
            //sign.algorithm = "HMAC-SHA1";

            //ANET-44 SDE/ISAAC - TBA changing the signature method.
            //Commented above code by Brash Developer on 24-June-2021
            //Issue :- There is an ongoing 24 hours test window for HMAC-SHA1 Deprecation for Integrations Using Token-based Authentication (TBA), 
            //if possible you can use HMAC-SHA256 signature method.
            using (var hmacSha256 = new HMACSHA256(keyBytes))
            {
                byte[] hashBaseString = hmacSha256.ComputeHash(baseStringBytes);
                signature = Convert.ToBase64String(hashBaseString);
            }
            TokenPassportSignature sign = new TokenPassportSignature();
            sign.algorithm = "HMAC-SHA256";

            sign.Value = signature;
            return sign;
        }

        #endregion
    }
    class DataCenterAwareNetSuiteService : NetSuiteService
    {
        public DataCenterAwareNetSuiteService(string account)
            : base()
        {
            System.Uri originalUri = new System.Uri(this.Url);
            DataCenterUrls urls = getDataCenterUrls(account).dataCenterUrls;
            Uri dataCenterUri = new Uri(urls.webservicesDomain + originalUri.PathAndQuery);
            this.Url = dataCenterUri.ToString();
        }
    }
}
