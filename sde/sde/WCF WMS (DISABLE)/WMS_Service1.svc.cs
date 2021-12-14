using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Data.Odbc;
using sde.Models;
using log4net;
using System.Transactions;
using System.Net;
using System.Messaging;
using Sybase.Data.AseClient;
using iAnywhere.Data.SQLAnywhere;

namespace sde.WCF_WMS
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "WMS_Service1" in code, svc and config file together.
    // NOTE: In order to launch WCF Test Client for testing this service, please select WMS_Service1.svc or WMS_Service1.svc.cs at the Solution Explorer and start debugging.
    public class WMS_Service1 : IWMS_Service1
    {
        private readonly ILog WMSPullPushMQLog = LogManager.GetLogger("WMSPullPushMQ");    //#361
        public static Int32 exceptionCount = 0;

        public void WMSpullMQ()
        {
            MessageQueueTransaction msgTx = new MessageQueueTransaction();
            System.Messaging.Message objMessage = new System.Messaging.Message();

            String qname = @Resource.QUEUENAME_WMS;
            MessageQueue messageQueue = null;
            messageQueue = new MessageQueue(qname);
            this.WMSPullPushMQLog.Info("Connect to " + qname);

            #region defineDataStructure
            Object o = new Object();
            System.Type[] arrTypes = new System.Type[5];

            arrTypes[0] = o.GetType();

            List<DiscountAndTax> datList = new List<DiscountAndTax>();
            arrTypes[1] = datList.GetType();

            List<SOReturn> sorList = new List<SOReturn>();
            arrTypes[2] = sorList.GetType();

            List<SOReturnItem> sriList = new List<SOReturnItem>();
            arrTypes[3] = sriList.GetType();

            List<requestDataForm> reqData = new List<requestDataForm>();
            arrTypes[4] = reqData.GetType();
            #endregion

            messageQueue.Formatter = new XmlMessageFormatter(arrTypes);

            try
            {
                msgTx.Begin();
                byte[] bt = new byte[10];
                objMessage = messageQueue.Receive(new TimeSpan(0, 2, 0), msgTx);

                this.WMSPullPushMQLog.Info("Extracting message from MQ with label: " + objMessage.Label.ToString());
                int indexFind = objMessage.Label.IndexOf(">");
                string jobMQ = objMessage.Label.Substring(indexFind);
                String insData = "FAIL";

                #region queue checking
                switch (jobMQ)
                {
                    case "> MQPUSH-DISCOUNT AND TAX":
                        datList = ((List<DiscountAndTax>)objMessage.Body);
                        insData = DiscountAndTax(datList);
                        break;
                    case "> MQPUSH-SO RETURN":
                        sorList = ((List<SOReturn>)objMessage.Body);
                        insData = SOReturn(sorList);
                        break;
                    case "> MQPUSH-SO RETURN ITEM":
                        sriList = ((List<SOReturnItem>)objMessage.Body);
                        insData = SOReturnItem(sriList);
                        break;
                    case "> MQPUSH-SO RETURN UPDATE":
                        reqData = ((List<requestDataForm>)objMessage.Body);
                        insData = CheckRequestDataForm(reqData);
                        break;
                    default:
                        insData = "SUCCESS";
                        break;
                }
                #endregion

                if (insData != "SUCCESS")
                {
                    msgTx.Abort();
                    //break;
                }
                else
                {
                    msgTx.Commit();
                }
            }
            catch (Exception ex)
            {
                this.WMSPullPushMQLog.Info(ex.ToString());
                msgTx.Abort();
            }
        }

        #region Upload Data InTo WMS
        private String CheckRequestDataForm(List<requestDataForm> reqData)
        {
            String insData = "FAIL";
            List<SOReturn> sorList = new List<SOReturn>();

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            for (int i = 0; i < reqData.Count(); i++)
            {
                switch (reqData[i].dataType)
                {
                    case "MQPUSH-SO RETURN UPDATE":
                        sorList = SOReturnUpdate(reqData);
                        if (sorList.Count() > 0)
                        {
                            insData = SOReturnItemUpdate(sorList);
                        }
                        break;
                }
            }
            return insData;
        }
        private String DiscountAndTax(List<DiscountAndTax> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };
            insMoAddr = insertDiscountAndTax(dataList);
            /*using (var scopeOuter = new TransactionScope(TransactionScopeOption.Required, option))
            {
                try
                {
                    insertDiscountAndTax(dataList);
                    scopeOuter.Complete();
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                    insMoAddr = "FAIL";
                }
            }*/

            this.WMSPullPushMQLog.Info("Job insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String SOReturn(List<SOReturn> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };
            insMoAddr = insertSOReturn(dataList);

            this.WMSPullPushMQLog.Info("Job insertion status. " + insMoAddr);
            return insMoAddr;
        }
        private String SOReturnItem(List<SOReturnItem> dataList)
        {
            String insMoAddr = "SUCCESS";

            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };
            insMoAddr = insertSOReturnItem(dataList);

            this.WMSPullPushMQLog.Info("Job insertion status. " + insMoAddr);
            return insMoAddr;
        }

        private String insertDiscountAndTax(List<DiscountAndTax> datList)
        {
            using (SAConnection oCon = new SAConnection(@sde.Resource.CONNECTIONSTRING_WMS))
            {
                String status = "SUCCESS";
                oCon.Open();

                SACommand cmd = oCon.CreateCommand();
                SATransaction scope1 = oCon.BeginTransaction();
                cmd.Connection = oCon;
                cmd.Transaction = scope1;

                try
                {
                    for (int i = 0; i < datList.Count(); i++)
                    {
                        cmd.CommandText = "insert into sde_discountandtax (wms_jobordmaster_ID,itemID,discount,tax,moNo,moNo_internalID,wms_jobordmaster_pack_id,wms_job_id,qty,price,orderLine) values (" +
                            "'" + datList[i].wms_jobordmaster_ID + "','" + datList[i].itemID + "'," + datList[i].discount + "," + datList[i].tax + ",'" + datList[i].moNo + "'," +
                            "'" + datList[i].moNoInternalID + "','" + datList[i].wms_jobordmaster_pack_id + "','" + datList[i].wms_job_id + "'," + datList[i].qty + "," + datList[i].price + "," + datList[i].orderLine + ")";
                        cmd.ExecuteNonQuery();
                        this.WMSPullPushMQLog.Debug(cmd);
                    }
                    cmd.Dispose();
                    scope1.Commit();
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                    status = "FAILED";
                }
                return status;
            }
        }
        private String insertSOReturn(List<SOReturn> datList)
        {
            using (SAConnection oCon = new SAConnection(@sde.Resource.CONNECTIONSTRING_WMS))
            {
                String status = "SUCCESS";
                oCon.Open();

                SACommand cmd = oCon.CreateCommand();
                SATransaction scope1 = oCon.BeginTransaction();
                cmd.Connection = oCon;
                cmd.Transaction = scope1;

                try
                {
                    for (int i = 0; i < datList.Count(); i++)
                    {
                        Int32 active = 0;
                        if (datList[i].rrActive == true)
                        {
                            active = 1;
                        }
                        cmd.CommandText = "insert into wms_return (rr_id,sch_id,rr_number,rr_date,rr_description,createdby,rr_reference,rr_status,rr_active) values " +
                            "('" + datList[i].rrID + "','" + datList[i].schID + "','" + datList[i].rrNumber + "','" + datList[i].rrDate + "','" + datList[i].rrDesc + "','" + datList[i].rrCreatedBy + "'," +
                            "'" + datList[i].rrReference + "'," + datList[i].rrStatus + "," + active + ")";
                        cmd.ExecuteNonQuery();
                        this.WMSPullPushMQLog.Debug(cmd);
                    }
                    cmd.Dispose();
                    scope1.Commit();
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                    status = "FAILED";
                }
                return status;
            }
        }
        private String insertSOReturnItem(List<SOReturnItem> datList)
        {
            using (SAConnection oCon = new SAConnection(@sde.Resource.CONNECTIONSTRING_WMS))
            {
                String status = "SUCCESS";
                oCon.Open();

                SACommand cmd = oCon.CreateCommand();
                SATransaction scope1 = oCon.BeginTransaction();
                cmd.Connection = oCon;
                cmd.Transaction = scope1;

                try
                {
                    for (int i = 0; i < datList.Count(); i++)
                    {
                        cmd.CommandText = "insert into wms_returnitem (rritem_id,rr_id,rritem_invoice,rritem_isbn,rritem_isbn2,rritem_return_qty,createddate,createdby,rritem_status,remarks," +
                            "item_id,pack_id) values ('" + datList[i].riID + "','" + datList[i].rrID + "','" + datList[i].riInvoice + "'," +
                            "'" + datList[i].riIsbn + "','" + datList[i].riIsbn2 + "'," + datList[i].riReturnQty + ",'" + datList[i].riCreatedDate + "','" + datList[i].riCreatedBy + "'," +
                            "'" + datList[i].riStatus + "','" + datList[i].riRemarks + "','" + datList[i].riItemID + "','" + datList[i].riPackID + "')";
                        this.WMSPullPushMQLog.Debug(cmd.CommandText);
                        cmd.ExecuteNonQuery();
                    }
                    cmd.Dispose();
                    scope1.Commit();
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                    status = "FAILED";
                }
                return status;
            }
        }
        #endregion

        #region Push Queue Function
        private String JobOrdScan(List<requestDataForm> dataList)
        {
            this.WMSPullPushMQLog.Debug("Retrieving wms_jobordscan.");

            AseConnection oCon = new AseConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            #region wms_jobordscan
            List<JobOrdScan> josList = new List<JobOrdScan>();
            AseCommand cmd1 = oCon.CreateCommand();
            AseCommand cmd2 = oCon.CreateCommand();
            cmd1.CommandText = "select top 10 * from wms_jobordscan where load_ind = 'N'";//to be continue - remove top 10 and chage status to N
            //cmd2.CommandText = "update wms_jobordscan set load_ind = 'Y' where jobordscan_id = @jobordscan_id";
            AseDataReader reader1 = cmd1.ExecuteReader();
            try
            {
                while (reader1.Read())
                {
                    JobOrdScan jos = new JobOrdScan();

                    jos.jobOrdScanID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                    jos.consignmentNote = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);
                    jos.countryTag = (reader1.GetValue(2) == DBNull.Value) ? String.Empty : reader1.GetString(2);
                    jos.deliveryRef = (reader1.GetValue(3) == DBNull.Value) ? String.Empty : reader1.GetString(3);
                    jos.jobID = (reader1.GetValue(4) == DBNull.Value) ? String.Empty : reader1.GetString(4);
                    jos.jobMoID = (reader1.GetValue(5) == DBNull.Value) ? String.Empty : reader1.GetString(5);
                    jos.ordRecNo = (reader1.GetValue(6) == DBNull.Value) ? String.Empty : reader1.GetString(6);
                    jos.scanDate = reader1.GetDateTime(7);
                    jos.moNo = (reader1.GetValue(8) == DBNull.Value) ? String.Empty : reader1.GetString(8);
                    jos.businessChannelID = (reader1.GetValue(9) == DBNull.Value) ? String.Empty : reader1.GetString(9);
                    jos.businessChannelCode = (reader1.GetValue(10) == DBNull.Value) ? String.Empty : reader1.GetString(10);
                    jos.recID = reader1.GetString(11);
                    jos.exportDate = reader1.GetDateTime(12);
                    jos.loadInd = (reader1.GetValue(13) == DBNull.Value) ? String.Empty : reader1.GetString(13);
                    jos.doNo = (reader1.GetValue(14) == DBNull.Value) ? String.Empty : reader1.GetString(14);
                    josList.Add(jos);

                    //AseParameter param1 = new AseParameter("@jobordscan_id", jos.jobOrdScanID);
                    //cmd2.Parameters.Add(param1);
                    //cmd2.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
            }
            reader1.Close();
            cmd1.Dispose();
            #endregion

            oCon.Close();

            Guid gjob_id = Guid.NewGuid();
            String MQjob = gjob_id.ToString();
            String transactionType = "MQPUSH-JOB ORD SCAN";
            String queueLabel = "WAREHOUSE" + " - " + MQjob + " > " + transactionType;
            Boolean bTranStatus = JobOrdScanMQ(queueLabel, josList, transactionType);
            this.WMSPullPushMQLog.Debug("Sending wms_jobordscan to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }
        }
        private String JobOrdScanPack(List<requestDataForm> dataList)
        {
            this.WMSPullPushMQLog.Debug("Retrieving wms_jobordscan_pack.");

            AseConnection oCon = new AseConnection();
            oCon.ConnectionString = @sde.Resource.CONNECTIONSTRING_IMAS;
            oCon.Open();

            List<JobOrdScanPack> jospList = new List<JobOrdScanPack>();
            AseCommand cmd2 = oCon.CreateCommand();
            //cmd2.CommandText = "select top 10 * from wms_jobordscan_pack where posted_ind = 'N'";//to be continue - remove top 10
            cmd2.CommandText = "select * from wms_jobordscan_pack where posted_ind = 'T'";//to be continue - remove top 10
            AseDataReader reader2 = cmd2.ExecuteReader();
            try
            {
                while (reader2.Read())
                {
                    JobOrdScanPack josp = new JobOrdScanPack();

                    josp.jobordmaster_pack_id = (reader2.GetValue(0) == DBNull.Value) ? String.Empty : reader2.GetString(0);
                    josp.status = (reader2.GetValue(1) == DBNull.Value) ? String.Empty : reader2.GetString(1);
                    josp.ordFulfill = reader2.GetInt32(2);

                    josp.ordPoint = (reader2.GetValue(3) == DBNull.Value) ? 0 : reader2.GetDouble(3);//reader2.GetDouble(3);
                    josp.posted_ind = (reader2.GetValue(4) == DBNull.Value) ? String.Empty : reader2.GetString(4);
                    jospList.Add(josp);
                }
            }
            catch (Exception ex)
            {
                this.WMSPullPushMQLog.Error(ex.ToString());
            }
            reader2.Close();
            cmd2.Dispose();
            oCon.Close();

            Guid gjob_id = Guid.NewGuid();
            String MQjob = gjob_id.ToString();
            String transactionType = "MQPUSH-JOB ORD SCAN PACK";
            String queueLabel = "WAREHOUSE" + " - " + MQjob + " > " + transactionType;
            Boolean bTranStatus = JobOrdScanPackMQ(queueLabel, jospList, transactionType);
            this.WMSPullPushMQLog.Debug("Sending wms_jobordscan_pack to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");

            if (bTranStatus == true)
            {
                return "SUCCESS";
            }
            else
            {
                return "FAIL";
            }
        }
        private List<SOReturn> SOReturnUpdate(List<requestDataForm> dataList)
        {
            this.WMSPullPushMQLog.Debug("Retrieving wms_return.");
            using (SAConnection oCon = new SAConnection(@sde.Resource.CONNECTIONSTRING_WMS))
            {
                List<SOReturn> sorList = new List<SOReturn>();
                try
                {
                    oCon.Open();
                    SACommand cmd = oCon.CreateCommand();
                    cmd.CommandText = "select * from wms_return where rr_status = 1 and rr_return_date between '" + dataList[0].rangeFrom + "' and '" + dataList[0].rangeTo + "'";
                    this.WMSPullPushMQLog.Debug(cmd.CommandText);
                    SADataReader reader1 = cmd.ExecuteReader();
                    cmd.Connection = oCon;


                    while (reader1.Read())
                    {
                        SOReturn sor = new SOReturn();
                        sor.rrID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                        sor.schID = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);
                        sor.rrNumber = (reader1.GetValue(2) == DBNull.Value) ? String.Empty : reader1.GetString(2);
                        sor.rrDate = reader1.GetDateTime(3);
                        sor.rrDesc = (reader1.GetValue(4) == DBNull.Value) ? String.Empty : reader1.GetString(4);
                        sor.rrCreatedBy = (reader1.GetValue(5) == DBNull.Value) ? String.Empty : reader1.GetString(5);
                        sor.rrReference = (reader1.GetValue(6) == DBNull.Value) ? String.Empty : reader1.GetString(6);
                        sor.rrStatus = reader1.GetInt32(7);
                        sor.rrActive = reader1.GetBoolean(8);
                        sor.rrReturnDate = reader1.GetDateTime(9);
                        sor.rrReturnBy = (reader1.GetValue(10) == DBNull.Value) ? String.Empty : reader1.GetString(10);
                        sorList.Add(sor);
                    }
                    cmd.Dispose();
                    /*
                    if (sorList.Count() > 0)
                    {
                        Guid gjob_id = Guid.NewGuid();
                        String MQjob = gjob_id.ToString();
                        String transactionType = "MQPUSH-SO RETURN UPDATE";
                        String queueLabel = "WMS" + " - " + MQjob + " > " + transactionType;
                        Boolean bTranStatus = SOReturnUpdateMQ(queueLabel, sorList, transactionType);
                        this.WMSPullPushMQLog.Debug("Sending wms_return to MQ with label: " + queueLabel + ".(Inserted Successfully: " + bTranStatus + ")");
                    }*/
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                }
                finally
                {
                    oCon.Close();
                }
                return sorList;
            }
        }
        private String SOReturnItemUpdate(List<SOReturn> dataList)
        {
            this.WMSPullPushMQLog.Debug("Retrieving wms_returnitem.");
            using (SAConnection oCon = new SAConnection(@sde.Resource.CONNECTIONSTRING_WMS))
            {
                String status = "FAIL";
                List<SOReturnItem> sriList = new List<SOReturnItem>();
                try
                {
                    oCon.Open();
                    SACommand cmd = oCon.CreateCommand();

                    for (int i = 0; i < dataList.Count(); i++)
                    {
                        cmd.CommandText = "select * from wms_returnitem where rr_id = '" + dataList[i].rrID + "'";
                        this.WMSPullPushMQLog.Debug(cmd.CommandText);
                        SADataReader reader1 = cmd.ExecuteReader();

                        while (reader1.Read())
                        {
                            SOReturnItem sri = new SOReturnItem();
                            sri.riID = (reader1.GetValue(0) == DBNull.Value) ? String.Empty : reader1.GetString(0);
                            sri.rrID = (reader1.GetValue(1) == DBNull.Value) ? String.Empty : reader1.GetString(1);
                            sri.riInvoice = (reader1.GetValue(2) == DBNull.Value) ? String.Empty : reader1.GetString(2);
                            sri.riIsbn = (reader1.GetValue(3) == DBNull.Value) ? String.Empty : reader1.GetString(3);
                            sri.riIsbn2 = (reader1.GetValue(4) == DBNull.Value) ? String.Empty : reader1.GetString(4);
                            sri.riReturnQty = reader1.GetInt32(5);
                            sri.riCreatedDate = reader1.GetDateTime(6);
                            sri.riCreatedBy = (reader1.GetValue(7) == DBNull.Value) ? String.Empty : reader1.GetString(7);
                            sri.riStatus = reader1.GetInt32(8);
                            sri.riRemarks = (reader1.GetValue(9) == DBNull.Value) ? String.Empty : reader1.GetString(9);
                            sri.riMaxReturn = reader1.GetInt32(10);
                            sri.riItemID = (reader1.GetValue(11) == DBNull.Value) ? String.Empty : reader1.GetString(11);
                            sri.riPackID = (reader1.GetValue(12) == DBNull.Value) ? String.Empty : reader1.GetString(12);
                            sri.riReceiveQty = reader1.GetInt32(13);
                            sri.riPostingQty = reader1.GetInt32(14);
                            sriList.Add(sri);
                        }
                        reader1.Close();
                    }
                    cmd.Dispose();
                    if (sriList.Count() > 0)
                    {
                        Guid gjob_id1 = Guid.NewGuid();
                        String MQjob1 = gjob_id1.ToString();
                        String transactionType1 = "MQPUSH-SO RETURN UPDATE";
                        String queueLabel1 = "WMS" + " - " + MQjob1 + " > " + transactionType1;
                        Boolean bTranStatus1 = SOReturnUpdateMQ(queueLabel1, dataList, transactionType1);
                        this.WMSPullPushMQLog.Debug("Sending wms_return to MQ with label: " + queueLabel1 + ".(Inserted Successfully: " + bTranStatus1 + ")");


                        Guid gjob_id2 = Guid.NewGuid();
                        String MQjob2 = gjob_id2.ToString();
                        String transactionType2 = "MQPUSH-SO RETURN ITEM UPDATE";
                        String queueLabel2 = "WMS" + " - " + MQjob2 + " > " + transactionType2;
                        Boolean bTranStatus2 = SOReturnItemUpdateMQ(queueLabel2, sriList, transactionType2);
                        this.WMSPullPushMQLog.Debug("Sending wms_returnitem to MQ with label: " + queueLabel2 + ".(Inserted Successfully: " + bTranStatus2 + ")");

                        if (bTranStatus2 == true)
                        {
                            status = "SUCCESS";
                        }
                        else
                        {
                            status = "FAIL";
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.WMSPullPushMQLog.Error(ex.Message.ToString());
                }
                finally
                {
                    oCon.Close();
                }
                return status;
            }
        }

        
        // ******************************************************************     
        private Boolean JobOrdScanMQ(String queueLabel, List<JobOrdScan> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message("sde");
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean JobOrdScanPackMQ(String queueLabel, List<JobOrdScanPack> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message("sde");
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean SOReturnUpdateMQ(String queueLabel, List<SOReturn> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message("sde");
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        private Boolean SOReturnItemUpdateMQ(String queueLabel, List<SOReturnItem> dataList, String transactionType)
        {
            Boolean bTranStatus;
            var option = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = TimeSpan.FromSeconds(60)
            };

            MessageQueueTransaction msgTx = new MessageQueueTransaction();

            string qname = @Resource.QUEUENAME_SDE;

            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(@Resource.QUEUENAME_SDE))
            {
                messageQueue = new MessageQueue(qname);
            }
            else
            {
                MessageQueue.Create(@Resource.QUEUENAME_SDE, true);
                messageQueue = new MessageQueue(@Resource.QUEUENAME_SDE);
            }

            msgTx.Begin();
            try
            {
                System.Messaging.Message message = new System.Messaging.Message("sde");
                message.Priority = MessagePriority.Low;
                message.Label = queueLabel;
                message.Body = dataList;
                message.Recoverable = true;
                messageQueue.Send(message, msgTx);
                bTranStatus = true;
                msgTx.Commit();
            }
            catch
            {
                msgTx.Abort();
                bTranStatus = false;
            }
            finally
            {
                messageQueue.Close();
            }
            return bTranStatus;
        }
        #endregion

        private void exceptionTest(sdeEntities entities, List<JobMoAddress> moList)
        {
            var insertMQ = "insert into netsuite_jobmo_address_test2 (nsjma_jobMoAddress_ID) values ('tt')";
            this.WMSPullPushMQLog.Debug(insertMQ);
            entities.Database.ExecuteSqlCommand(insertMQ);
        }

        public void DoWork()
        {
        }

    }
}

