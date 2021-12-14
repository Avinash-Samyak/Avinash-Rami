using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using sde.Models;

namespace sde.WCF
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "IService1" in both code and config file together.
    [ServiceContract]
    public interface IService1
    {
        [OperationContract]
        List<SchedulerEntity> GetActivatedSchedule();

        [OperationContract]
        String ExecuteScheduler();

        [OperationContract]
        String PushNetsuite();

        [OperationContract]
        String PullNetsuite();

        [OperationContract]
        String PushMQ();

        [OperationContract]
        String PullMQ();

        [OperationContract]
        String PullMQ2();

        [OperationContract]
        String WelComeMessage(String name);

        [OperationContract]
        String ConnectToSde();
    }
}
