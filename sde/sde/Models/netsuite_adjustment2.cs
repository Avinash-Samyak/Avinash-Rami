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
    
    public partial class netsuite_adjustment2
    {
        public int nas_ID { get; set; }
        public string nas_analysisCode { get; set; }
        public string nas_shipmentNo { get; set; }
        public Nullable<System.DateTime> nas_firstShipmentDate { get; set; }
        public string nas_Type { get; set; }
        public string nas_Subsidiary { get; set; }
        public Nullable<int> nas_subsidiaryID { get; set; }
        public Nullable<int> nas_businessChannelID { get; set; }
        public string nas_businessChannel { get; set; }
        public Nullable<int> nas_accountNo { get; set; }
        public Nullable<System.DateTime> nas_shipmentDate { get; set; }
        public string nas_postingPeriod { get; set; }
        public Nullable<int> nas_postingPeriodID { get; set; }
        public Nullable<int> nas_locationID { get; set; }
    }
}