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
    
    public partial class wms_cashsale
    {
        public string cs_cashsaleID { get; set; }
        public string cs_entity { get; set; }
        public string cs_entity_internalID { get; set; }
        public Nullable<System.DateTime> cs_tranDate { get; set; }
        public string cs_subsidiary { get; set; }
        public string cs_subsidiary_internalID { get; set; }
        public string cs_businessChannel { get; set; }
        public string cs_businessChannel_internalID { get; set; }
        public string cs_location { get; set; }
        public string cs_location_internalID { get; set; }
        public Nullable<System.DateTime> cs_createdDate { get; set; }
        public Nullable<System.DateTime> cs_rangeTo { get; set; }
    }
}
