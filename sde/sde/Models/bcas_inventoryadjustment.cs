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
    
    public partial class bcas_inventoryadjustment
    {
        public int ia_recID { get; set; }
        public string ia_jobID { get; set; }
        public string ia_businessChannel_code { get; set; }
        public string ia_countryTag { get; set; }
        public string ia_moNo_prefix { get; set; }
        public string ia_item_internalID { get; set; }
        public Nullable<int> ia_adjQty { get; set; }
        public Nullable<System.DateTime> ia_createdDate { get; set; }
        public Nullable<System.DateTime> ia_rangeFrom { get; set; }
        public Nullable<System.DateTime> ia_rangeTo { get; set; }
    }
}