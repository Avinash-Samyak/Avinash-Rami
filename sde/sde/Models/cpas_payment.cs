//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace sde.Models
{
    using System;
    using System.Collections.Generic;
    
    public partial class cpas_payment
    {
        public int cpm_recId { get; set; }
        public string cpm_postingType { get; set; }
        public Nullable<System.DateTime> cpm_postingDate { get; set; }
        public string cpm_priceCode { get; set; }
        public string cpm_contractYear { get; set; }
        public string cpm_contractMonth { get; set; }
        public string cpm_contractWeek { get; set; }
        public string cpm_subsidiary { get; set; }
        public string cpm_subsidiary_internalID { get; set; }
        public Nullable<System.DateTime> cpm_syncDate { get; set; }
        public string cpm_salesPostingCat { get; set; }
        public string cpm_location { get; set; }
        public string cpm_ml_location_internalID { get; set; }
        public Nullable<decimal> cpm_payment { get; set; }
        public Nullable<decimal> cpm_applyPayment { get; set; }
        public Nullable<int> cpm_totalcontracts { get; set; }
        public string cpm_businessChannel { get; set; }
        public string cpm_mb_businessChannel_internalID { get; set; }
        public string cpm_sp_id { get; set; }
        public Nullable<System.DateTime> cpm_createdDate { get; set; }
        public string cpm_ProgressStatus { get; set; }
        public string cpm_ProgressStatusSeqNo { get; set; }
        public string cpm_salesPostingCatOld { get; set; }
        public string cpm_pmInternalId { get; set; }
        public Nullable<System.DateTime> cpm_pmUpdatedDate { get; set; }
    }
}
