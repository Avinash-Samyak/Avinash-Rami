using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_gstDataExtraction
    { 
    } 

    public class cls_gstDS
    {
        public DateTime? dateSubmit { get; set; }
        public String entity { get; set; }
        public String fromPeriod { get; set; }
        public String toPeriod { get; set; }
        public String status { get; set; }
        public Int32 totalRowsInput { get; set; }
        public Int32 totalRowsExport { get; set; }
        public Decimal totalAmountExport { get; set; }
        public Decimal totalGstAmountExport { get; set; }
        public String exportFolderPath { get; set; }
        public String exportFileName { get; set; }

    }

    public class gstDSList
    {
        public List<cls_gstDS> gstDSEntity { get; set; }
    }

    public class cls_gstBC
    {
        public DateTime? dateSubmit { get; set; }
        public String entity { get; set; }
        public String fromPeriod { get; set; }
        public String toPeriod { get; set; }
        public String status { get; set; }
        public Int32 totalRowsInput { get; set; }
        public Int32 totalRowsExport { get; set; }
        public Decimal totalAmountExport { get; set; }
        public Decimal totalGstAmountExport { get; set; }
        public String exportFolderPath { get; set; }
        public String exportFileName { get; set; }
    }

    public class gstBCList
    {
        public List<cls_gstBC> gstBCEntity { get; set; }
    }
     
    public class cls_gstDSPeriodTo
    { 
        public Int32 periodTo { get; set; }
        public String periodFiscal { get; set; }
        public Int32 periodMonth { get; set; }
        public String periodFrom { get; set; }
    }

    public class gstDSListPeriodTo
    {
        public List<cls_gstDSPeriodTo> gstDSEntityPeriodTo { get; set; }
    }

    public class cls_gstBCPeriodTo
    {
        public Int32 periodTo { get; set; }
        public String periodFiscal { get; set; }
        public Int32 periodYear { get; set; }
        public Int32 periodMonth { get; set; }
    }

    public class gstBCListPeriodTo
    {
        public List<cls_gstBCPeriodTo> gstBCEntityPeriodTo { get; set; }
    }
      
    public class cls_DSCustomer
    { 
        public String accCode { get; set; }
        public String custName { get; set; }
        public String billAddress1 { get; set; }
        public String billAddress2 { get; set; }
        public String billCity { get; set; }
        public String billPostcode { get; set; }
        public String billState { get; set; }
        public String telephone { get; set; }
        public String email { get; set; }
        public String description { get; set; }
        public String contractPeriod { get; set; }  
    }

    public class DSCustomerList
    {
        public List<cls_DSCustomer> DSCustomerEntity { get; set; }
    }


}