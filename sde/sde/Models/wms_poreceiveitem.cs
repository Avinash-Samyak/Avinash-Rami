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
    
    public partial class wms_poreceiveitem
    {
        public string poi_poreceiveitem_ID { get; set; }
        public Nullable<System.DateTime> poi_createdDate { get; set; }
        public Nullable<int> poi_invoiceItem_qty { get; set; }
        public string poi_item_ID { get; set; }
        public string poi_location_code { get; set; }
        public string poi_poreceive_ID { get; set; }
        public Nullable<decimal> poi_poreceiveItem_qty { get; set; }
        public string poi_priItem_ID { get; set; }
        public long poi_sort { get; set; }
        public Nullable<int> poi_damage_qty { get; set; }
        public Nullable<int> poi_excessQty { get; set; }
        public string poi_item_internalID { get; set; }
        public string poi_netsuitePO { get; set; }
    }
}
