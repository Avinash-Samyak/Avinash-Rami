using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_map_item 
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
        public Nullable<decimal> mip_item_price { get; set; }
    }
    public class RequestMapItemList
    {
        public List<cls_map_item> error { get; set; }
    }
}
