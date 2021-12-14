using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace sde.Models
{
	public class linq_scheduler

    {
        private sdeEntities entities = new sdeEntities();

        public IEnumerable<cls_viewScheduler> GetSchedulerInfo(string strScheId, string strTranId)
		{
            int intSchedulerId = int.Parse(strScheId);

            ////// Data ///////
            /*
            List<mes_qual_perentry> rcardList = new List<mes_qual_perentry>();

            mes_qual_perentry ss = new mes_qual_perentry();
            ss.STATION_NO = "1";
            ss.MEASUREABLE_ITEM = "A2";
            ss.WORKWEEK = 19;
            ss.ENTRY = 1;
            ss.RATING1 = 1;
            ss.RATING2 = 2;
            ss.RATING3 = 3;
            ss.LM_DATE = DateTime.Now;
            rcardList.Add(ss);

            ss = new mes_qual_perentry();
            ss.STATION_NO = "1";
            ss.MEASUREABLE_ITEM = "A2";
            ss.WORKWEEK = 19;
            ss.ENTRY = 2;
            ss.RATING1 = 1;
            ss.RATING2 = 2;
            ss.RATING3 = 3;
            ss.LM_DATE = DateTime.Now;
            rcardList.Add(ss);

            IEnumerable<qualPerEntry> rcard =
                from o in rcardList
                where o.STATION_NO == strStationNo
                && o.MEASUREABLE_ITEM == strMeasureItem
                && o.WORKWEEK == intWorkWeek
                select new qualPerEntry
                {
                    STATION_NO = o.STATION_NO,
                    MEASUREABLE_ITEM = o.MEASUREABLE_ITEM,
                    WORKWEEK = o.WORKWEEK,
                    ENTRY = o.ENTRY,
                    LM_DATE = o.LM_DATE,
                    RATING1 = o.RATING1,
                    RATING2 = o.RATING2,
                    RATING3 = o.RATING3
                };
            */
            ////// Data ///////
            

            IQueryable<scheduler> rcard = from o in entities.schedulers
                                              where o.sche_id == intSchedulerId
                                              && o.sche_transactionType == strTranId
                                              select o;

            IEnumerable<cls_viewScheduler> result = (from o in rcard.AsEnumerable()
                                                     select new cls_viewScheduler
                                        {
                                            transactionType = o.sche_transactionType,
                                            minuteGap = o.sche_minuteGap,
                                            nextRun = o.sche_nextRun.Value.ToString("yyyy-MM-dd HH:mm:ss"),
                                            nextRunSeqNo = o.sche_nextRunSeqNo,
                                            lastRun = o.sche_lastRun.Value.ToString("yyyy-MM-dd HH:mm:ss"),
                                            lastRunSeqNo = o.sche_lastRunSeqNo,
                                            sequence = o.sche_sequence,
                                            status = o.sche_status
                                        });

            return result;
        
        }
	}
}