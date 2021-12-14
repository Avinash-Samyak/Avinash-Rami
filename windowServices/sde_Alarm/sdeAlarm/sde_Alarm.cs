using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net.Mail;
using MySql.Data.MySqlClient;

namespace sdeAlarm
{
    public partial class sde_Alarm : ServiceBase
    {
        private System.Timers.Timer timer;
        private int sentCount;
        // System.Timers.Timer reportTimer;
        
        public sde_Alarm()
        {
            InitializeComponent();

            
            //if (!System.Diagnostics.EventLog.SourceExists("SDE Alarm"))
            //{
            //    System.Diagnostics.EventLog.CreateEventSource(
            //        "SDE Alarm", "SDE Alarm Log");
            //}
            

            eventLog1.Source = "SDE Alarm";
            eventLog1.Log = "SDE Alarm Log";
            //process();
           
        }

        protected override void OnStart(string[] args)
        {
            this.timer = new System.Timers.Timer(Properties.Settings.Default.Timer);  // 30000 milliseconds = 30 seconds
            //this.timer.AutoReset = true;
            this.timer.Elapsed += new System.Timers.ElapsedEventHandler(this.timer_Elapsed);
            this.timer.Enabled = true;
          
            eventLog1.WriteEntry("SDE Alarm started"); 
            
        }

        protected override void OnPause()
        {
            eventLog1.WriteEntry("SDE Alarm paused");
        }

        protected override void OnStop()
        {
            this.timer.Enabled = true;
            //this.timer.Stop();
            this.timer = null;
            eventLog1.WriteEntry("SDE Alarm stopped");
        }

        protected override void OnContinue()
        {
            eventLog1.WriteEntry("SDE Alarm resumed");
        }

        private void timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            process();
        }

        private void process()
        {
            try
            {
                eventLog1.WriteEntry("SDE Alarm Start Process");

                DateTime currentTime = DateTime.Now;
                if (currentTime.TimeOfDay.Hours == 6 ||
                    currentTime.TimeOfDay.Hours == 9 ||
                    currentTime.TimeOfDay.Hours == 12 ||
                    currentTime.TimeOfDay.Hours == 15 ||
                    currentTime.TimeOfDay.Hours == 18 ||
                    currentTime.TimeOfDay.Hours == 21 ||
                    currentTime.TimeOfDay.Hours == 0 ||
                    currentTime.TimeOfDay.Hours == 3)
                {
                    summaryReport();
                }

                dbConnect dbcon = new dbConnect();
                String maxdate = dbcon.Select();
                if (maxdate == "")
                {
                    maxdate = DateTime.Now.ToString();
                }

                string path = @"C:\inetpub\wwwroot\sde_publish\Logger\SummaryLog.log";
                string path2 = @"C:\inetpub\wwwroot\sde_publish\Snapshot\SummaryLog.log";
                try
                {
                    File.Copy(path, path2, true);
                }
                catch (Exception ex)
                {
                    Email email = new Email();
                    email.sendErrorEmail("Copy Error" + ex.Message.ToString());
                }

                if (File.Exists(path2))
                {
                    //Email emailAA = new Email();
                    //emailAA.serviceErrorEmail(path2 + "0k");
                    eventLog1.WriteEntry("Snapshot of log file taken at " + DateTime.Now.ToString());

                    List<logEntry> entryList = new List<logEntry>();
                    double test;

                    using (StreamReader x = new StreamReader(path2))
                    {
                        try
                        {
                            while (x.EndOfStream == false)
                            {

                                logEntry log = new logEntry();
                                string entry = x.ReadLine();

                                if (entry.Length > 3)
                                {
                                    if (Double.TryParse(entry.Substring(0, 3), out test) == false)
                                    {
                                        entryList[entryList.Count - 1].Description += entry.Substring(3, entry.Length - 3);
                                    }
                                    else
                                    {
                                        int descriptionIndex = entry.IndexOf("-", 40);
                                        int typeIndex = 49;

                                        if ((entry[descriptionIndex + 1] == ' ') & (entry[descriptionIndex - 1] == ' '))
                                        {
                                            descriptionIndex = descriptionIndex + 1;
                                            typeIndex = descriptionIndex - 2;
                                        }
                                        log.date = Convert.ToDateTime(entry.Substring(0, 19));
                                        log.type = entry.Substring(24, 6).Trim();
                                        log.source = entry.Substring(30, typeIndex - 30).Trim();
                                        log.Description = entry.Substring(descriptionIndex, entry.Length - 51).Trim();
                                        entryList.Add(log);
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Email email2 = new Email();
                            email2.sendErrorEmail("File Error:" + ex.Message.ToString());
                        }
                    }

                    List<logEntry> errorLogList = new List<logEntry>();
                    List<logEntry> warningLogList = new List<logEntry>();
                    List<logEntry> fatalLogList = new List<logEntry>();

                    Email logEmail = new Email();
                    foreach (logEntry log in entryList)
                    {
                        try
                        {
                            switch (log.type)
                            {
                                case "ERROR":
                                    if (log.date > DateTime.Parse(maxdate))
                                    {
                                        errorLogList.Add(log);
                                    }
                                    break;

                                case "FATAL":
                                    if (log.date > DateTime.Parse(maxdate))
                                    {
                                        fatalLogList.Add(log);

                                    }
                                    break;

                                case "WARN":
                                    if (log.date > DateTime.Parse(maxdate))
                                    {
                                        warningLogList.Add(log);
                                    }
                                    break;
                            }
                        }
                        catch (Exception ex)
                        {
                            Email email3 = new Email();
                            email3.sendErrorEmail("Email Error:" + ex.Message.ToString());
                        }
                    }

                    dbConnect errorDB = new dbConnect();
                    if (errorLogList.Count > 0)
                    {
                        eventLog1.WriteEntry("Error detected from log at " + DateTime.Now.ToString());
                        if (logEmail.errorSendEmail("ERROR", errorLogList) == true)
                        {
                            foreach (logEntry log in errorLogList)
                            {
                                errorDB.Insert(DateTime.Now, log.Description, Properties.Settings.Default.errorSummayEmailList, log.type, log.date, log.source);

                            }
                        }
                    }

                    dbConnect warningDB = new dbConnect();
                    if (warningLogList.Count > 0)
                    {
                        eventLog1.WriteEntry("Warning detected from log at " + DateTime.Now.ToString());
                        if (logEmail.errorSendEmail("WARNING", warningLogList) == true)
                        {
                            foreach (logEntry log in warningLogList)
                            {
                                warningDB.Insert(DateTime.Now, log.Description, Properties.Settings.Default.errorSummayEmailList, log.type, log.date, log.source);

                            }
                        }
                    }

                    dbConnect fatalDB = new dbConnect();
                    if (fatalLogList.Count > 0)
                    {
                        eventLog1.WriteEntry("Fatal detected from log at " + DateTime.Now.ToString());
                        if (logEmail.errorSendEmail("FATAL", fatalLogList) == true)
                        {
                            foreach (logEntry log in fatalLogList)
                            {
                                fatalDB.Insert(DateTime.Now, log.Description, Properties.Settings.Default.errorSummayEmailList, log.type, log.date, log.source);

                            }
                        }
                    }

                }
                else
                {
                    eventLog1.WriteEntry("Snapshot of log file failed at " + DateTime.Now.ToString());
                    Email email = new Email();
                    email.sendErrorEmail("Snapshot of log file failed at " + DateTime.Now.ToString());
                }
                eventLog1.WriteEntry("SDE Alarm End Process");
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry("Process Start Error: " + ex.Message.ToString());
                Email email = new Email();
                email.sendErrorEmail("Process Start Error:" + ex.Message.ToString());
            }
      
        }


        private void summaryReport()
        {
            dbConnect dbcon = new dbConnect();
            Email alarm = new Email();
            
            try
            {
                List<reportItem> list = dbcon.selectReport();
                if (list.Count > 0)
                {
                    //string messageHeader = String.Format("<table><th>{0,-30}</th><th>{1,-20}</th><th>{2,-20}</th><th>{3,-20}</th><th>{4,-10}</th>", "Transaction Type", "Status", "RangeFrom", "RangeTo", "count\n");
                    string messageHeader = String.Format("<table border=\"1\"><th>{0}</th><th>{1}</th><th>{2}</th><th>{3}</th><th>{4}</th><th>{5}</th><th>{6}</th>", "Transaction Type", "Status", "RangeFrom", "RangeTo", "count\n","UpdatedDate","CompletedAt");
                    string msg = "";
                    foreach (reportItem item in list)
                    {
                        //msg = msg + String.Format("<tr><td>{0,-30}</td><td>{1,-20}</td><td>{2,-20}</td><td>{3,-20}</td><td>{4,-10}</td></tr>", item.transactiontType, item.status, item.rangeFrom, item.rangeTo, item.count + "\n");
                        msg = msg + String.Format("<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td><td>{6}</td></tr>", item.transactiontType, item.status, item.rangeFrom, item.rangeTo, item.count, item.updatedDate, item.completedAt.ToString() + "\n");
                    }

                    if (alarm.sendSummaryEmail(messageHeader + msg) == false)
                    {
                        eventLog1.WriteEntry("Summary email sending fail");
                    }
                }
            }
            catch(Exception ex)
            {
                eventLog1.WriteEntry(ex.Message);
            }
        }

        //private void reportTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        //{
        //    dbConnect dbcon = new dbConnect();
        //    Email alarm = new Email();
        //    List<reportItem> list = dbcon.selectReport();

        //    if (list.Count > 0)
        //    {
        //        //string messageHeader = String.Format("<table><th>{0,-30}</th><th>{1,-20}</th><th>{2,-20}</th><th>{3,-20}</th><th>{4,-10}</th>", "Transaction Type", "Status", "RangeFrom", "RangeTo", "count\n");
        //        string messageHeader = String.Format("<table border=\"1\"><th>{0}</th><th>{1}</th><th>{2}</th><th>{3}</th><th>{4}</th>", "Transaction Type", "Status", "RangeFrom", "RangeTo", "count\n");
        //        string msg = "";
        //        foreach (reportItem item in list)
        //        {
        //            //msg = msg + String.Format("<tr><td>{0,-30}</td><td>{1,-20}</td><td>{2,-20}</td><td>{3,-20}</td><td>{4,-10}</td></tr>", item.transactiontType, item.status, item.rangeFrom, item.rangeTo, item.count + "\n");
        //            msg = msg + String.Format("<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td></tr>", item.transactiontType, item.status, item.rangeFrom, item.rangeTo, item.count + "\n");

        //        }

        //        alarm.summaryEmail(messageHeader + msg);

        //    }


        //}
    }

    public class reportItem
    {
        public string transactiontType, status;
        public DateTime rangeFrom, rangeTo, completedAt, updatedDate;
        public int count;
    }

    public class logEntry
    {
        public DateTime date;
        public String type, Description, source;
    }

    public class dbConnect
    {
        private MySqlConnection connection;
        private string server;
        private string database;
        private string uid;
        private string password;

        //Constructor
        public dbConnect()
        {
            Initialize();
        }

        //Initialize values
        private void Initialize()
        {
            server = Properties.Settings.Default.Server;
            database = Properties.Settings.Default.Database;
            uid = Properties.Settings.Default.UserID;
            password = Properties.Settings.Default.Password;

            string connectionString;
            connectionString = "SERVER=" + server + ";" + "DATABASE=" +
            database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";

            connection = new MySqlConnection(connectionString);
            try
            {
                connection.Open();
            }
            catch (MySqlException ex)
            {
                Email email = new Email();
                email.sendErrorEmail("DB Error:" + ex.Message);
            }
        }
        public String Select()
        {
            string query = "SELECT MAX(errordate) as date FROM sde.requestnetsuite_alarm";

            //Create a list to store the result
            string maxDate = "";

            //Open connection

            //Create Command
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    maxDate = dataReader["date"].ToString();
                }

                //close Data Reader
                dataReader.Close();
            }
            catch (Exception ex)
            {
                Email email = new Email();
                email.sendErrorEmail("Data Error:" + ex.Message);
            }
            //return list to be displayed

            return maxDate;
        }
        public List<reportItem> selectReport()
        {
            string query = "SELECT rn_sche_transactionType, rn_status, rn_rangeFrom, rn_rangeTo, rn_updatedDate, rn_completedAt, count(*)";
            query = query + " FROM sde.requestnetsuite";
            query = query + " where rn_status <> 'STAND BY'";
            query = query + " and rn_updatedDate > DATE_SUB(NOW(), INTERVAL 4 HOUR)";
            query = query + " group by rn_sche_transactionType, rn_status, rn_rangeFrom, rn_rangeTo, rn_updatedDate, rn_completedAt";
            query = query + " order by rn_updatedDate desc;";

            List<reportItem> reportItemList = new List<reportItem>();
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    reportItem item = new reportItem();
                    DateTime result = new DateTime();
                    item.transactiontType = dataReader["rn_sche_transactionType"].ToString();
                    item.status = dataReader["rn_status"].ToString();
                    DateTime.TryParse(dataReader["rn_rangeFrom"].ToString(), out result);
                    item.rangeFrom = result;
                    DateTime.TryParse(dataReader["rn_rangeTo"].ToString(), out result);
                    item.rangeTo = result;
                    item.count = Int16.Parse(dataReader["count(*)"].ToString());
                    DateTime.TryParse(dataReader["rn_updatedDate"].ToString(), out result);
                    item.updatedDate = result;
                    DateTime.TryParse(dataReader["rn_completedAt"].ToString(), out result);
                    item.completedAt = result;
                    reportItemList.Add(item);
                }
                dataReader.Close();
            }
            catch (Exception ex)
            {
                Email email = new Email();
                email.sendErrorEmail("Report Error:" + ex.Message);
            }
            //close Data Reader


            //return list to be displayed

            return reportItemList;

        }
        public Boolean Insert(DateTime sendDate, string description, string recipientList, string type, DateTime errorDate, string source)
        {
            string query = String.Format(@"INSERT INTO requestnetsuite_alarm (`sendDate`,`description`,`recipientList`,`type`,errorDate,sourceFile) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}')", sendDate.ToString("yyyy-MM-dd HH:mm:ss"), description.Replace("'", "\\'"), recipientList, type, errorDate.ToString("yyyy-MM-dd HH:mm:ss"), source);
            try
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();
                dataReader.Close();
            }
            catch (Exception ex)
            {
                Email email = new Email();
                email.sendErrorEmail("Insert Error:" + ex.Message);
                return false;
            }
            return true;
        }
    }

    public class Email
    {

        public string[] recipientAddress;
        public string senderAddress = Properties.Settings.Default.senderAddress;
        public string senderPassword = Properties.Settings.Default.emailPassword;
        public string mailServer = Properties.Settings.Default.mailServer;
        public int mailPort = Properties.Settings.Default.mailPort;

        public Boolean sendErrorEmail(string errorMessage)
        {
            try
            {
                MailMessage mailMsg = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient(mailServer);
                mailMsg.From = new MailAddress(senderAddress);

                string list = Properties.Settings.Default.errorSummayEmailList;

                string[] recipientAddress = list.Split(',');

                foreach (string address in recipientAddress)
                {
                    if (address != "")
                        mailMsg.To.Add(address);
                }
    
                mailMsg.Subject = "SDE Alarm error " + DateTime.Now;
                string emailBody;
                emailBody = errorMessage;
                mailMsg.Body = emailBody;
                mailMsg.IsBodyHtml = false;

                SmtpServer.Port = mailPort;
                SmtpServer.UseDefaultCredentials = true;
                SmtpServer.Credentials = new System.Net.NetworkCredential(senderAddress, senderPassword);
                SmtpServer.EnableSsl = true;
                SmtpServer.Send(mailMsg);
                return true;
            }
            catch (Exception ex)
            {
                if (!System.Diagnostics.EventLog.SourceExists("SDE Alarm"))
                {
                    System.Diagnostics.EventLog.CreateEventSource(
                        "SDE Alarm", "SDE Alarm Error Log");
                }

                EventLog eventLog = new EventLog();

                eventLog.Source = "SDE Alarm";
                eventLog.Log = "SDE Alarm Log";
                eventLog.WriteEntry("SDE Alarm Error: " + ex.Message);
                return false;
            }
        }
        public Boolean errorSendEmail(string msgType, List<logEntry>list)
        {
            try
            {
            MailMessage mailMsg = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient(mailServer);
            mailMsg.From = new MailAddress(senderAddress);
            string emailList;
            string[] recipientAddress;

            if (msgType == "ERROR")
            {
                emailList = Properties.Settings.Default.errorSummayEmailList;
                recipientAddress = emailList.Split(',');
            }
            else if (msgType == "WARNING")
            {
                emailList = Properties.Settings.Default.FatalemailList2;
                recipientAddress = emailList.Split(',');
            }
            else
            {
                emailList = Properties.Settings.Default.WarningemailList3;
                recipientAddress = emailList.Split(',');
            }

            foreach (string address in recipientAddress)
            {
                if (address != "")
                    mailMsg.To.Add(address);
            }

            mailMsg.Subject = "SDE " + msgType + " " + DateTime.Now;
            string emailBody = "";
            foreach (logEntry log in list)
            {
                emailBody += log.date + " " + log.source + "\n" + log.Description + "\n";
            }
            
            mailMsg.Body = emailBody;
            mailMsg.IsBodyHtml = false;

            SmtpServer.Port = mailPort;
            SmtpServer.UseDefaultCredentials = true;
            SmtpServer.Credentials = new System.Net.NetworkCredential(senderAddress, senderPassword);
            SmtpServer.EnableSsl = true;
            SmtpServer.Send(mailMsg);
            return true;
        }
            catch(Exception ex)
            {
                if (!System.Diagnostics.EventLog.SourceExists("SDE Alarm"))
                {
                    System.Diagnostics.EventLog.CreateEventSource(
                        "SDE Alarm", "SDE Alarm Log");
                }

                EventLog eventLog = new EventLog();

                eventLog.Source = "SDE Alarm";
                eventLog.Log = "SDE Alarm Log";
                eventLog.WriteEntry("SDE Alarm Error: " + ex.Message);
                return false;
            }
        }
        public Boolean sendSummaryEmail(string sourceFile)
        {
            try
            {
                MailMessage mailMsg = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient(mailServer);
                mailMsg.From = new MailAddress(senderAddress);

                string list = Properties.Settings.Default.errorSummayEmailList;
                string[] recipientAddress = list.Split(',');

                foreach (string address in recipientAddress)
                {
                    if (address != "")
                        mailMsg.To.Add(address);
                }

                mailMsg.Subject = "SDE SUMMARY " + DateTime.Now;
                string emailBody;
                emailBody = sourceFile;
                mailMsg.Body = emailBody;
                mailMsg.IsBodyHtml = true;

                SmtpServer.Port = mailPort;
                SmtpServer.UseDefaultCredentials = true;
                SmtpServer.Credentials = new System.Net.NetworkCredential(senderAddress, senderPassword);
                SmtpServer.EnableSsl = true;
                SmtpServer.Send(mailMsg);
                return true;
            }
            catch(Exception ex)
            {
                return false;
            }
        }
    }
}
