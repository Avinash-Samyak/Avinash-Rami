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
    
    public partial class wms_jobitem_unscan
    {
        public string jiu_country { get; set; }
        public string jiu_businessChannel_code { get; set; }
        public string jiu_itemID { get; set; }
        public string jiu_unscan_ID { get; set; }
        public Nullable<int> jiu_unscan_qty { get; set; }
        public string jiu_writeOff_tag { get; set; }
        public string jiu_batchNo { get; set; }
        public Nullable<System.DateTime> jiu_createdDate { get; set; }
        public string jiu_posted_ind { get; set; }
        public Nullable<decimal> jiu_recID { get; set; }
    }
}