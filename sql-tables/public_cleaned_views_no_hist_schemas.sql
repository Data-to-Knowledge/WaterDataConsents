CREATE OR REPLACE View "Consents_Aquifier_Tests" AS
SELECT
"WELL_NO" AS "Wap", "RESULT_S" AS "Storativity"  FROM "WATERDATAREPO"."wel"."AQUIFER_TESTS"

CREATE OR REPLACE VIEW "Consents_AssociatedPermits_Source" AS
SELECT
TRIM(UPPER("RecordNumber")) AS "RecordNumber"
, TRIM(UPPER("Record Number")) AS "OtherRecordNumber"
, TRIM(UPPER("Relationship")) AS "Relationship"
, TRIM(UPPER("Status")) AS "LinkedStatus"
, ROUND("Combined Annual Vol. (m3)"::Decimal(18,2),2) AS "CombinedAnnualVolume"
FROM "WATERDATAREPO"."acla"."vAct_Water_AssociatedPermits"

CREATE OR REPLACE VIEW "Consents_PermitAuthorisation_Source" AS
SELECT
UPPER(TRIM("RecordNumber")) AS "RecordNumber"
, INITCAP(TRIM("Activity")) AS "TakeType"
,CASE IFNULL(INITCAP(TRIM("Has a lowflow restriction condition?")),'No')
    WHEN 'Yes' THEN True
    WHEN 'No'  THEN False
    ELSE True
 END AS "LowflowCondition"
,IFF(ROUND("Consent Max Vol (m3)"::Decimal(18,2),3) <= 0,NULL,ROUND("Consent Max Vol (m3)"::Decimal(18,2),3)) AS "ConsentedMultiDayVolume"
,IFF(ROUND("Consent Max Consec Days"::Decimal(18,2),3) <= 0,NULL,ROUND("Consent Max Consec Days"::Decimal(18,2),3)) AS "ConsentedMultiDayPeriod"
,IFF(ROUND("Consent Max Rate (l/s)"::Decimal(18,2),3) <= 0,NULL,ROUND("Consent Max Rate (l/s)"::Decimal(18,2),3)) AS "ConsentedRate"
FROM "WATERDATAREPO"."acla"."vAct_Water_TakeWaterPermitAuthorisation"

CREATE OR REPLACE VIEW "Consents_PermitUse_Source" AS
SELECT
TRIM(UPPER("RecordNumber")) AS "RecordNumber"
, IFF(SUBSTRING("Activity",4,1) = ' ',INITCAP(TRIM("Activity")),'blah') AS "UseType" -- Need some clarification here
,"Activity" AS "take_type" -- Need some clarifiaction here
,IFF(ROUND("Consecutive Day Period"::Decimal(18,2),3) <= 0,NULL,ROUND("Consecutive Day Period"::Decimal(18,2),3)) AS "ConsentedMultiDayVolume"
,IFF(ROUND("Volume (m3)"::Decimal(18,2),3) <= 0,NULL,ROUND("Volume (m3)"::Decimal(18,2),3)) AS "ConsentedMultiDayPeriod"
,IFF(ROUND("Max Rate (l/s)"::Decimal(18,2),3) <= 0,NULL,ROUND("Max Rate (l/s)"::Decimal(18,2),3)) AS "ConsentedRate"
,'Some Mappings Need to go here' AS "MappingsHere"
FROM "WATERDATAREPO"."acla"."vAct_Water_TakeWaterPermitUse"

CREATE OR REPLACE VIEW "Consents_PermitVolume_Source" AS
SELECT
TRIM(UPPER("RecordNumber")) AS "RecordNumber"
, INITCAP(TRIM("Activity")) AS "TakeType"
, REPLACE(REPLACE(TRIM("Allocation Block"),'Migration: Not Classified','A'),'allo_block','A') AS "GwAllocationBlock"
,CASE INITCAP(TRIM("Include in allocation?"))
    WHEN 'Yes' THEN True
    WHEN 'No'  THEN False
 END AS "IncludeInGwAllocation"
 ,"Effective Annual Volume (m3/year)"::Int AS "AllocatedAnnualVolume"
 ,"Full Annual Volume (m3/year)"::Int AS "FullAnnualVolume"
FROM "WATERDATAREPO"."acla"."vAct_Water_TakeWaterPermitVolume"

CREATE OR REPLACE VIEW "Consents_Permit_ParentChild" AS
SELECT
TRIM(UPPER("parent RecordNo")) AS "ParentRecordNumber"
, TRIM(UPPER("child RecordNo")) AS "ChildRecordNumber"
, TRIM("parent Category") AS "ParentCategory"
, TRIM("child Category") AS "ChildCategory"
FROM "WATERDATAREPO"."acla"."vQA_Relationship_Actuals"

CREATE OR REPLACE VIEW "Consents_Permit_Source" as
	SELECT
	TRIM(UPPER("B1_ALT_ID")) as "RecordNumber",
    TRIM("B1_APPL_STATUS") as "ConsentStatus",
    TRIM("B1_PER_CATEGORY") as "ApplicationStatus",
    TRIM(UPPER("ECNUMBER")) as "ECNumber",
    TRIM("HOLDERNAME") as "HolderName",
    "FMDATE" as "FromDate",
    "TODATE" as "ToDate"
	FROM "WATERDATAREPO"."gacla"."PERMIT_DETAILS"
    where "B1_PER_SUB_TYPE" = 'Water Permit (s14)'

CREATE OR REPLACE View "Consents_Site" AS
SELECT
"UpstreamSiteID" AS "Wap", "Name", "NZTMX" AS "NzTmX", "NZTMY" AS "NzTmY"  FROM "WATERDATAREPO"."usm"."Site"

CREATE OR REPLACE View "Consents_Stream_Depletion_Locations" AS
SELECT
"Well_No" AS "Wap"
, "SD1_7" AS "SD1_7Day"
, "SD1_30" AS "SD1_30Day"
, "SD1_150" AS "SD1_150Day"
FROM "WATERDATAREPO"."wel"."Well_StreamDepletion_Locations"

CREATE OR REPLACE VIEW "Consents_WapAllocation_Source" AS
SELECT
TRIM(UPPER("RecordNumber")) AS "RecordNumber"
, INITCAP(TRIM("Activity")) AS "TakeType"
, REPLACE(REPLACE(TRIM("SW Allocation Block"),'Migration: Not Classified','A'),'sw_allo_block','A') AS "SwAllocationBlock"
, IFF(RLIKE(TRIM(UPPER("WAP")),'[A-Z]+\\d\\d/\\d+'),TRIM(UPPER("WAP")),Null) AS "Wap"
,CASE REPLACE(INITCAP(TRIM("From Month")),'Migration: Not Classified','Jul')
    WHEN 'Jan' THEN 7
    WHEN 'Feb' THEN 8
    WHEN 'Mar' THEN 9
    WHEN 'Apr' THEN 10
    WHEN 'May' THEN 11
    WHEN 'Jun' THEN 12
    WHEN 'Jul' THEN 1
    WHEN 'Aug' THEN 2
    WHEN 'Sep' THEN 3
    WHEN 'Oct' THEN 4
    WHEN 'Nov' THEN 5
    WHEN 'Dec' THEN 6
    ELSE 1
end AS "FromMonth"
,CASE REPLACE(INITCAP(TRIM("To Month")),'Migration: Not Classified','Jun')
    WHEN 'Jan' THEN 7
    WHEN 'Feb' THEN 8
    WHEN 'Mar' THEN 9
    WHEN 'Apr' THEN 10
    WHEN 'May' THEN 11
    WHEN 'Jun' THEN 12
    WHEN 'Jul' THEN 1
    WHEN 'Aug' THEN 2
    WHEN 'Sep' THEN 3
    WHEN 'Oct' THEN 4
    WHEN 'Nov' THEN 5
    WHEN 'Dec' THEN 6
    ELSE 12
end AS "ToMonth"
,CASE INITCAP(TRIM("Include in SW Allocation?"))
    WHEN 'Yes' THEN True
    WHEN 'No'  THEN False
 END AS "IncludeInSwAllocation"
,"Allocation Rate (l/s)"::Decimal(9,2) AS "AllocatedRate"
,"Max Rate for WAP (l/s)"::Decimal(9,2) AS "WapRate"
,"Daily Vol. (m3)"::Int AS "VolumeDaily"
,"Weekly Vol (m3)"::Int AS "VolumeWeekly"
,"150-day Vol (m3)"::Int AS "Volume150Day"
FROM "WATERDATAREPO"."acla"."vAct_Water_TakeWaterWAPAllocation"

CREATE OR REPLACE VIEW "Consents_WaterDiverts_Source" AS
SELECT
NULLIF(RLIKE("WAP",'[A-Z]+\\d\\d/\\d\\d\\d\\d'),FALSE) AS WAP
, TRIM(UPPER("RecordNumber")) AS "RecordNumber"
, INITCAP(TRIM("Activity")) AS "DivertType"
,CASE IFNULL(INITCAP(TRIM("Has a lowflow restriction condition?")),'No')
    WHEN 'Yes' THEN True
    WHEN 'No'  THEN False
    ELSE True
 END AS "LowflowCondition"
 ,IFF(ROUND("Volume (m3)"::Decimal(18,2),3) <= 0,NULL,ROUND("Volume (m3)"::Decimal(18,2),3)) AS "ConsentedMultiDayVolume"
 ,IFF(ROUND("Consecutive Day Period"::Decimal(18,2),3) <= 0,NULL,ROUND("Consecutive Day Period"::Decimal(18,2),3)) AS "ConsentedMultiDayPeriod"
 ,IFF(ROUND("Max Rate (l/s)"::Decimal(18,2),3) <= 0,NULL,ROUND("Max Rate (l/s)"::Decimal(18,2),3)) AS "ConsentedRate"
FROM "WATERDATAREPO"."acla"."vAct_Water_Divertwater_Water"
