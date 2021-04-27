DROP VIEW `ggl-moh-dhs-healthlink-covid19.DLFConversations.BCCDCMegaDailyTotalsCombined`;
CREATE VIEW IF NOT EXISTS `ggl-moh-dhs-healthlink-covid19.DLFConversations.BCCDCMegaDailyTotalsCombined`
 OPTIONS(
   expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR),
   friendly_name="newview",
   description="a view that expires in 2 days",
   labels=[("org_unit", "development")]
 )
 AS Select
  Table,
  DTC.DateOnly,
  DATE_ADD(DTC.DateOnly, INTERVAL 1 DAY) as ReportedDate,
  Avg_Query_Session,
  DailySessions,
  queries,
  totalConvosSum,
  totalNonConvosSum FROM (
    Select
      Table,
      DateOnly,
      Avg_Query_Session,
      DailySessions,
      queries
    from
      /* AF AUG 11 2020 - PULL IN THE VALUES FROM THE TOTALS TABLE BEFORE WE HAD AUDIT LOGGING RUNNING. VALUES FROM BACK IN MAY? GIVES COMPLETE NUMBERS*/
      (
        select
          'table' as table,
          Date as DateOnly,
          Avg_Query_Session,
          dailysessions,
          CAST (queries as NUMERIC) as queries
        from
          `ggl-moh-dhs-healthlink-covid19.DLFConversations.DailyTotalsTable`
      ) DTT
    Union all
      /*AF AUG 11 2020 - ADD INTO THE RESULT TABLE THE CALCULATED TOTALS FROM THE ALL DATA PARSED TABLE WITH IS MID MAY (I THINK) ONWARD*/
      (
        select
          table,
          d.dateonly,
          Avg_Query_Session,
          dailysessions,
          queries
        from
          (
            SELECT
              'calculated' as table,
              date(DateOnly) as DateOnly,
              count(distinct sessionID) as DailySessions,
              count(*) as Queries
            FROM
              `ggl-moh-dhs-healthlink-covid19.DLFConversations.AllMegaDataParsed`
              /*AF OCT 1 ADDED TO FILTER OUT HLBC TRAFFIC*/
              /*AF UPDATED OCT 17 TO ACCOUNT FOR BCCDC ON DLF MESSENGER*/
            where
              SourceBot = 'BCCDC'
            group by
              DateOnly,
              table
          ) D
          /*AF AUG 11 2020 - CALCULATE AVERAGE QUERIES PER SESSION SEPARATELY THAN THE TOTAL QUERY COUNT, BECAUSE GOOGLE INCLUDES *ALL* INTERACTIONS IN TOTAL COUNT, BUT APPARENTLY EXCLUDES THE DEFAULT WELCOME INTENT FROM THE                 AVERAGE QUERIES PER SESSION NUMBER, THEN JOIN TO TWO RESULTS TOGETHER BY DATE.                 */
          inner join (
            SELECT
              date(DateOnly) as DateOnly,
              count(*) / count(distinct sessionID) as Avg_Query_Session
            FROM
              `ggl-moh-dhs-healthlink-covid19.DLFConversations.AllMegaDataParsed`
            where
              matchedintent not in (
                "Default Welcome Intent",
                "BCCDC_SYSTEMRESPONSE_DEFAULT-WELCOME-INTENT",
                "HEALTHLINKBC_SYSTEMRESPONSE_DEFAULT-WELCOME-INTENT","IHA_SYSTEMRESPONSE_DEFAULT-WELCOME-INTENT"
                
              )
              /*AF OCT 1 ADDED TO FILTER OUT HLBC TRAFFIC*/
              /*AF UPDATED OCT 17 TO ACCOUNT FOR BCCDC ON DLF MESSENGER*/
              and SourceBot = 'BCCDC'
            group by
              DateOnly
          ) AvgS on D.Dateonly = AvgS.Dateonly
        where
          d.dateonly not in
          /*AF AUG 11 2020 - WHEN DERIVING THE CALCULATED VALUES, DON'T BOTHER CALCULATING TOTALS THAT ARE ALREADY IN THE DAILY TOTALS TABLE THAT GETS PULLED IN FROM THE UNION QUERY                */
          (
            SELECT
              Date
            FROM
              `ggl-moh-dhs-healthlink-covid19.DLFConversations.DailyTotalsTable`
          )
      )
      /*AF DEC 16 2020 - ADDED THE DTC SELECT STATEMENT WRAPPER FOR THE CONVO QUERY BELOW TO JOIN TO, RATHER THAN STICK IT INSIDE THE CALCULATION QUERIES ABOVE */
  ) DTC
  /*AF DEC 16 2020 - ADDED THESE ADDITIONAL FIELDS TO SHOW THE BREAK DOWN OF BOT ENGAGEMENTS AND CONVERSATIONS        */
  LEFT JOIN (
    SELECT
      Dateonly,
      totalConvosSum,
      totalNonConvosSum
    FROM
      `ggl-moh-dhs-healthlink-covid19.DLFConversations.MegaConversationTotals`
    WHERE
      SourceBot = 'BCCDC'
  ) C ON DTC.DATEONLY = C.DATEONLY order by DateOnly Desc


        