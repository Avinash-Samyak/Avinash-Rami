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
    
    public partial class map_item
    {
        public int mi_recID { get; set; }
        public string mi_item_ID { get; set; }
        public string mi_item_description { get; set; }
        public string mi_item_title { get; set; }
        public string mi_item_uom { get; set; }
        public string mi_item_isbn { get; set; }
        public string mi_isbn_secondary { get; set; }
        public Nullable<System.DateTime> mi_lastModifiedDate { get; set; }
        public string mi_modifiedBy { get; set; }
        public Nullable<System.DateTime> mi_createdDate { get; set; }
        public string mi_item_reorder_level { get; set; }
        public string mi_reorder_qty { get; set; }
        public Nullable<System.DateTime> mi_reorder_date { get; set; }
        public string mi_createdBy { get; set; }
        public Nullable<decimal> mi_item_weight { get; set; }
        public string mi_accountClassID { get; set; }
        public string mi_item_internalID { get; set; }
        public Nullable<System.DateTime> mi_rangeTo { get; set; }
        public string mi_businesschannel_name { get; set; }
        public string mi_businesschannel_InternalID { get; set; }
        public string mi_prodfamily { get; set; }
        public string mi_tax_schedule { get; set; }
        public string mi_tax_code { get; set; }
        public string mi_ns_itemID { get; set; }
        public string mi_pmd_id { get; set; }
        public Nullable<int> mi_pmd_sync { get; set; }
    }
}
