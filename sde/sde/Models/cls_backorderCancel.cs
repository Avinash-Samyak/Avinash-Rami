using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_backorderCancel
    {
        public string boc_isbn { get; set; }
        public string boc_moNo { get; set; }
        public string boc_quantity { get; set; }
        public Nullable<System.DateTime> boc_createdDate { get; set; }
        public string boc_sourceFile { get; set; }
        public Nullable<int> boc_insertSequence { get; set; }
        public string boc_netsuiteProcess { get; set; }
        public string boc_netsuiteJob { get; set; }
        public string boc_netsuiteJobStatus { get; set; }
        public string boc_netsuiteJobError { get; set; }
    }

    public class backorderCancelList
    {
        public List<cls_backorderCancel> _backorderCancelList { get; set; }
    }
    
}