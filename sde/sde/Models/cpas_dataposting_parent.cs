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
    
    public partial class cpas_dataposting_parent
    {
        public int spl_recID { get; set; }
        public string spl_transactionType { get; set; }
        public string spl_sDesc { get; set; }
        public string spl_sLoc { get; set; }
        public string spl_ml_location_internalID { get; set; }
        public string spl_businessChannel { get; set; }
        public string spl_mb_businessChannel_internalID { get; set; }
        public string spl_subsidiary { get; set; }
        public string spl_subsidiary_internalID { get; set; }
        public Nullable<System.DateTime> spl_postingDate { get; set; }
        public Nullable<System.DateTime> spl_syncDate { get; set; }
        public Nullable<System.DateTime> spl_createdDate { get; set; }
        public Nullable<System.DateTime> spl_readyPushDate { get; set; }
        public string spl_suspendDate { get; set; }
        public string spl_sp_id { get; set; }
        public string spl_salespostingcategory { get; set; }
        public string spl_noOfInstallments { get; set; }
        public string spl_taxCode { get; set; }
        public string spl_cancelType { get; set; }
        public Nullable<double> spl_WP { get; set; }
        public Nullable<double> spl_DP { get; set; }
        public Nullable<double> spl_THC_DM { get; set; }
        public Nullable<double> spl_THC { get; set; }
        public Nullable<double> spl_FC { get; set; }
        public Nullable<double> spl_Tax_PCT { get; set; }
        public Nullable<double> spl_GPM { get; set; }
        public Nullable<double> spl_GPM_RS { get; set; }
        public Nullable<double> spl_DC { get; set; }
        public Nullable<double> spl_TSP { get; set; }
        public Nullable<double> spl_CP { get; set; }
        public Nullable<double> spl_OS_BAL { get; set; }
        public Nullable<double> spl_RFC { get; set; }
        public Nullable<double> spl_UFC { get; set; }
        public string spl_netsuiteProgress { get; set; }
    }
}