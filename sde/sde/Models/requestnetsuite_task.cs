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
    
    public partial class requestnetsuite_task
    {
        public int rnt_id { get; set; }
        public string rnt_task { get; set; }
        public string rnt_description { get; set; }
        public string rnt_refNO { get; set; }
        public string rnt_jobID { get; set; }
        public string rnt_status { get; set; }
        public Nullable<System.DateTime> rnt_createdDate { get; set; }
        public string rnt_seqNO { get; set; }
        public string rnt_nsInternalId { get; set; }
        public string rnt_createdFromInternalId { get; set; }
        public string rnt_errorDesc { get; set; }
        public Nullable<System.DateTime> rnt_updatedDate { get; set; }
    }
}