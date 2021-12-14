using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

namespace sde.WCF_SSA
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "ISSA_Service1" in both code and config file together.
    [ServiceContract]
    public interface ISSA_Service1
    {
        [OperationContract]
        void DoWork();

        [OperationContract]
        void SSApullMQ();
    }
}
