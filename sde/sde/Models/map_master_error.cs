using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class map_master_error
    {
        public int mme_id { get; set; }
        public string mme_errorDesc { get; set; }
        public string mme_remark { get; set; }
        public string mme_is_rerun { get; set; }
        public string mme_is_netsuitedependent { get; set; }
        public string mme_is_sourcedatadependent { get; set; }
        public string mme_rerun_type { get; set; }
        public string mme_datapatch_desc { get; set; }
        public string mme_status { get; set; }
        public string mme_active { get; set; }
    }
    
}