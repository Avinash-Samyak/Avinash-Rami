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
    
    public partial class bcas_ordersfulfill
    {
        public string of_jobID { get; set; }
        public string of_moNo { get; set; }
        public System.DateTime of_rangeTo { get; set; }
        public string of_country_tag { get; set; }
        public string of_pack_ID { get; set; }
        public string of_jobOrdMaster_ID { get; set; }
        public string of_item_ID { get; set; }
        public string of_item_internalID { get; set; }
        public Nullable<int> of_ordQty { get; set; }
        public Nullable<int> of_ordFulfillQty { get; set; }
        public int of_ordSkuQty { get; set; }
        public System.DateTime of_createdDate { get; set; }
        public string of_netsuiteDummySo { get; set; }
        public string of_netsuiteSalesorder { get; set; }
        public string of_netsuiteAdjustment { get; set; }
        public string of_item_ISBN { get; set; }
        public string of_ordPack { get; set; }
        public Nullable<int> of_ordPackQty { get; set; }
        public Nullable<double> of_ordPackPrice { get; set; }
        public Nullable<double> of_ordPackGst { get; set; }
    }
}
