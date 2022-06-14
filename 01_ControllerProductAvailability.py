import sys,os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import subprocess

sys.path.append("../CommonLibs")
import BALogger
import BAConfig
import BAMailer
import BAETLHelper
 
G_FolderData = "c:/Projects/Data_Science/Projects/Product_Availability/Data/"


# init Logger and Config
oConfig = BAConfig.BAConfig("./Config/ProductAvailability.config")
G_AppName = oConfig.getParm("AppName")
oLogger = BALogger.BALogger(oConfig.getParm("LogFile"),oConfig.getParm("LogLevel"),oConfig.getParm("ConsoleLevel"))
oMailer = BAMailer.BAMailer(oLogger)
G_StatusLog = "\n"
G_MailActive = 1


def recordStatus(pStatus,pBold=False):
    global G_StatusLog
    oLogger.debug(pStatus)
    if pBold:
        G_StatusLog = G_StatusLog + "<br/><b>" + pStatus + "</b><br/>"
    else:
        G_StatusLog = G_StatusLog + pStatus + "<br/>"

# CHeck if Master Data is uptodate
def checkMasterData():
    recordStatus("checkMasterData.Start",True)
    df = BAETLHelper.getDFFromSQLFile("Nemesis","./SQL/10_CountStaleMasterData.sql")
    rowCount = df['RC'][0]
    if rowCount > 0:
        raise Exception("Error: Master Data Not Current")
        
    print(df)
    recordStatus("checkMasterData.End.OK")

def handleZipFiles():
    recordStatus("unzipFiles.Start",True)
    unzipAllFilesInFolder(G_FolderData)
    renameAllFilesInFolder(G_FolderData)
    recordStatus("unzipFiles.End")

def countRowsInCSVs():
    dResults = {}
    dResults['Lateness'] = BAETLHelper.getLineCount("./Data/LatenessReport.csv")
    dResults['DemandPriority'] = BAETLHelper.getLineCount("./Data/Demand_Priority.csv")
    dResults['DemandSupportabilityAll'] = BAETLHelper.getLineCount("./Data/Demand_Supportability_all.csv")
    dResults['DemandSupportabilityLL'] = BAETLHelper.getLineCount("./Data/Demand_Supportability_LL.csv")
    
    return dResults

def validateCSVs():
    recordStatus("validateCSVs.Start",True)
    dResults = countRowsInCSVs()
    if abs(dResults['Lateness'] - 1300000)/dResults['Lateness'] > .5:
        raise Exception("Error: Lateness file is too small/big")
    if abs(dResults['DemandPriority'] - 10000000)/dResults['DemandPriority'] > .5:
        raise Exception("Error: Demand Priority file is too small/big")
    if abs(dResults['DemandSupportabilityLL'] - 600000)/dResults['DemandSupportabilityLL'] > .5:
        raise Exception("Error: DemandSupportabilityLL file is too small/big")
    if abs(dResults['DemandSupportabilityAll'] - 25000000)/dResults['DemandSupportabilityAll'] > .5:
        raise Exception("Error: DemandSupportabilityAll file is too small/big")
    

    df = pd.read_csv('./Data/LatenessReport.csv',delimiter=';',converters={i: str for i in range(100)})
    ctValues = len(df['Late Fiscal Week'].unique())
    if ctValues < 5:
        raise Exception("ERROR: Column Values not found for Late Fiscal Week")
    
    recordStatus("validateCSVs.End")
    return dResults

def loadCSVsToNemesis():
    recordStatus("loadCSVsToNemsis.Start",True)
    rc = 0

    rc = rc + BAETLHelper.executeSSIS("./SSIS/LoadDemandPriority.dtsx")
    recordStatus("loadDemandPriority: " + str(rc))
    
    
    rc = rc + BAETLHelper.executeSSIS("./SSIS/LoadLatenessRaw.dtsx")
    recordStatus("loadLatenessRaw: " + str(rc))
    

    rc = rc + BAETLHelper.executeSSIS("./SSIS/LoadDemandSupportability_LL.dtsx")
    recordStatus("loadDemandSupportability_LL: " + str(rc))


    rc = rc + BAETLHelper.executeSSIS("./SSIS/LoadDemandSupportability_All.dtsx")
    recordStatus("loadDeamdSupportability_All: " + str(rc))
    
    if rc > 0:
        raise Exception("ERROR: loadCSVsToNemesis Failed")
    recordStatus("loadCSVsToNemsis.End")

def validateLoadCSVsToNemesis(pResults):
    recordStatus("validateLoadCSVToNemesis.Start",True)
    
    rc = BAETLHelper.getRowCount("Nemesis","select count(*) as RC From SC_PLN_DS.SUPPLY_COMMIT.Demand_Priority where Cycle is Null")
    recordStatus("DemandPriority.Loaded:" + str(rc) +",csv:"+str(pResults["DemandPriority"]))
    if rc < pResults["DemandPriority"] - 5:
        raise Exception("ERROR: load of DemandPriority failed: " + str(rc) + " vs " + str(pResults["DemandPriority"]))
    
    rc = BAETLHelper.getRowCount("Nemesis","select count(*) as RC From SC_PLN_DS.SUPPLY_COMMIT.Lateness_Raw where Cycle is Null")
    recordStatus("Lateness.Loaded:" + str(rc) +",csv:"+str(pResults["Lateness"]))
    if rc < pResults["Lateness"] - 5:
        raise Exception("ERROR: load of Lateness failed: " + str(rc) + " vs " + str(pResults["Lateness"]))

    rc = BAETLHelper.getRowCount("Nemesis","select count(*) as RC From SC_PLN_DS.SUPPLY_COMMIT.Demand_Supportability where Cycle is Null")
    recordStatus("DemandSupportability.Loaded:" + str(rc) +",csv:"+str(pResults["DemandSupportabilityLL"]+pResults["DemandSupportabilityAll"]))
    if rc < pResults["DemandSupportabilityLL"] + pResults["DemandSupportabilityAll"] - 10:
        raise Exception("ERROR: load of DemandSupportability failed: : " + str(rc) + " vs " + str(pResults["DemandSupportabilityLL"]) + " + " + str(pResults["DemandSupportabilityAll"]))

    recordStatus("validateLoadCSVToNemesis.End")

def setCycleAndRunType():
    recordStatus("setCycleAndRunType.Start",True)
    rc = BAETLHelper.executeSQLinFile("./SQL/41_SetCycleAndRunType.sql")
    recordStatus("SetCycleAndRunType.Status="+str(rc))
    recordStatus("setCycleAndRunType.End")


def runSQLStep(pStep,pFile):
    recordStatus(pStep+".Start",True)
    rc = BAETLHelper.executeSQLinFile(pFile)
    recordStatus(pStep+".Status="+str(rc))
    if rc > 0:
        raise Exception("ERROR: " + pStep + ".Status="+ str(rc) )
    
    recordStatus(pStep+".End")

def buildRCA():
    recordStatus("buildRCA.Start",True)
    index = ['SKU_10D','LOCATION','FISCAL_MONTH','FISCAL_YEAR','MARKET','RUN_TYPE','CYCLE','DEMAND_PRIORITY_12D']
    output_columns = ['PRIMARY_RISK_UNITS','PRIMARY_RISK_UNITS_DRIVER','PRIMARY_RISK_DOLLARS','PRIMARY_RISK_DOLLARS_DRIVER']
    query_names = {
                 "units":     "./SQL/53_RCA_SQL_UNITS.sql",
                 "dollars":   "./SQL/53_RCA_SQL_DOLLARS.sql"
              }
    results = {}

    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=US-THT-NEMESIS1;DATABASE=SC_PLN_DS;UID=SC_DS_LOGIN;PWD=SC_DS_ELC;TRUSTED_CONNECTION=NO')
    for key, value in query_names.items():
        with open(value, 'r') as file:
            query = file.read()
        raw_data = pd.read_sql_query(query,cnxn)
        raw_data.drop_duplicates(inplace=True)
        raw_data.set_index(index, inplace=True)
        raw_data['PRIMARY_RISK_' + key.upper()] = raw_data.max(axis=1)    
        raw_data['PRIMARY_RISK_' + key.upper() + '_DRIVER'] = raw_data.idxmax(axis=1)
        results[key] = raw_data

    units = results['units']
    dollars = results['dollars']
    final_result = pd.merge(units, dollars, how = 'inner', left_index = True, right_index = True)
    final_result = final_result[output_columns]
    final_result = final_result[(final_result['PRIMARY_RISK_UNITS'] > 0) & (final_result['PRIMARY_RISK_DOLLARS'] > 0)].drop_duplicates()
    final_result.to_csv('./Data/RCA_Pub_Result.csv')

    recordStatus("buildRCA.End")


def loadRCA():
    recordStatus("loadRCA.Start",True)
    rc = 0
    rc = rc + BAETLHelper.executeSSIS("./SSIS/LoadRCA.dtsx")
    if rc > 0:
        raise Exception("ERROR: loadRCA Failed")
    recordStatus("loadRCA.End")
    
#NOT USED
def runSqlAndCheckResults(pStep,pSQL,pTable):
    recordStatus("\n" + pStep + ".Start")
    rc = BAETLHelper.executeSQLinFile(pSQL)
    recordStatus(pStep+".sql Status=" + str(rc) + "\n")
    rcCount = BAETLHelper.getRowCount("Nemesis","Select count(*) as RC from " + pTable + " where archLoadDate > getDate() - .1")
    recordStatus(pStep+".Records Added=" + str(rcCount) + "\n")
    if rc > 0 or rcCount < 1:
        recordStatus("Error: " + pStep + " sql not successful")
        recordStatus(pStep+".End.Error")
        raise Exception("Error: " + pStep + " sql not successful")
    recordStatus(pStep+".End.OK")


def unzipFile(pFolder,pFile):
    recordStatus("unzipFile: "+pFile)
    sCmd = "c:\\Apps\\7-Zip\\7z.exe  "
    sCmd = sCmd + " -o" + pFolder
    sCmd = sCmd + " e "+ pFolder + pFile
    sCommand = '"' + sCmd + '"'
    os.system(sCommand)

def unzipAllFilesInFolder(pFolder):
    recordStatus("unzipAllFilesInFolder")
    lFiles = os.listdir(pFolder)
    for sFile in lFiles:
        if ".zip" in sFile:
            unzipFile(pFolder,sFile)

def getNewName(pFile):
    if 'Lateness' in pFile:
        return "LatenessReport.csv"
    
    if 'Priority' in pFile:
        return "Demand_Priority.csv"
    
    if 'Demand_Supportability_LL' in pFile:
        return "Demand_Supportability_LL.csv"
    
    if 'Demand_Supportability_all' in pFile:
        return "Demand_Supportability_All.csv"
    
    return ""
    
def renameAllFilesInFolder(pFolder):
    lFiles = os.listdir(pFolder)
    for sFile in lFiles:
        if ".csv" in sFile:
            sNewName = getNewName(sFile)
            if sNewName != "":
                os.rename(G_FolderData+sFile,G_FolderData+sNewName)
                
def sendMail(pSubject,pBody):
    if G_MailActive == 1:
        oMailer.sendEmailHTML(oConfig.getParm("EmailTo"),oConfig.getParm("EmailSubjectPrefix")+pSubject,pBody)

def main():
    global G_StatusLog
    dResults = {}
    #checkMasterData()
    handleZipFiles()
    dResults = validateCSVs()
    loadCSVsToNemesis()
    validateLoadCSVsToNemesis(dResults)
    setCycleAndRunType()
    #####runSQLStep("renameTempTableAndApplyIndex","./SQL/42_RenameTempTableAndApplyIndex.sql")
    runSQLStep("handleHistory","./SQL/43_HandleHistory.sql")
    runSQLStep("buildLateness","./SQL/51_BuildLateness.sql")
    runSQLStep("demandSupportability_P12","./SQL/52_DemandSupportability_P12.sql")
    buildRCA()
    loadRCA()
    runSQLStep("demandSupportability_P3","./SQL/55_DemandSupportability_P3.sql")
    runSQLStep("buildRiskless","./SQL/56_BuildRiskless.sql")
    runSQLStep("updatePBIResult","./SQL/57_Update_PBIResult.sql")
    
try:
    recordStatus(G_AppName + ".Start",True)
    sendMail("Start",'App Started')
    main()
    recordStatus(G_AppName + ".End.OK")
    sendMail("End.OK",G_StatusLog)
    
except Exception as ex:
    recordStatus("Error: " + str(ex))
    sendMail("End.Error",G_StatusLog)


x = input("Complete")
