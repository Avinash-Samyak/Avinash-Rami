using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

namespace sde.WCF_WMS
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "IWMS_Service1" in both code and config file together.
    [ServiceContract]
    public interface IWMS_Service1
    {
        [OperationContract]
        void DoWork();

        [OperationContract]
        void WMSpullMQ();
    }
}
