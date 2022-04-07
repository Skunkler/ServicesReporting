import arcpy
from arcpy import env
from RestRequest import RestRequest

import datetime
from datetime import datetime
from EmailUtilities import EmailUtil

emailObj = EmailUtil("gis_data_update_process@cincinnati-oh.gov", ["Warren.Kunkler@cincinnati-oh.gov"])



class updateCCAT_GIS_MapServies:

    def __init__(self, rootDir):
        self.rootDir = rootDir

        #self.Caggisrv01 = RestRequest("siteadmin", "admin", "caggissrv01", 6080, self.rootDir + '\\CagSrv01.csv')
        #self.Caggisrv03 = RestRequest("siteadmin", "admin", "caggissrv03", 6080, self.rootDir + "\\CagSrv03.csv")
        self.__cgdConnect = r"C:\Users\WKunkler\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\Cagis.sde"
        #self.__CreateBackUps()
        self.__updateData()

    def __getDBConnectionExpression(self):

        return ["dbConnect(!Database_Connection!)", """def dbConnect(inVal):
  return inVal.replace("Instance=", "")"""]

    def __getUserExpression(self):
        return ["userEx(!User!)", """def userEx(inVal):
  return inVal.replace("User=","")"""]

    def __getVersionExpression(self):
        return ["versionEx(!Version!)", """def versionEx(inVal):
  return inVal.replace("Version=","")"""]

    def __getFDExpression(self):
        return ["FDExpr(!Feature_Dataset!)", """def FDExpr(inVal):
  return inVal.replace("Feature Dataset=","")"""]

    def __getFCExpression(self):
        return ["FCExpr(!Feature_Class!)", """def FCExpr(inVal):
  return inVal.replace("Dataset=","")"""]


    def __CreateBackUps(self):
        
        backUpGDB = self.rootDir + '\\backUp.gdb'

        env.workspace = backUpGDB
        dt = datetime.now()
        dt_string = dt.strftime("%m/%d/%Y")

        day = dt_string.split("/")[1]
        listOfTableDates = []
        try:
            if int(day) == 1 or int(day) == 15:
                #backup tables should have backupTable_{date} format
                tables = arcpy.ListTables()
                lstOfTableDates = []
                for tbl in tables:
                    tblDate = tbl.split("_")[1:]
                    tbMonth = tblDate[0]
                    tbDay = tblDate[1]
                    tbYear = tblDate[2]
                    listOfTableDates.append([tbMonth, tbDay, tbYear])


                
                
                
                if len(tables) > 1:
                    ListOfDates = list(map(lambda elem: datetime.strptime("/".join(elem), "%m/%d/%Y"), listOfTableDates))
                    minTableDate = "backupTable_" + min(ListOfDates).strftime("%m/%d/%Y").replace("/","_")
                    print("deleting {0}".format(minTableDate))
                    #delete oldest table and add new backup
                    arcpy.Delete_management(backUpGDB+"\\"+minTableDate)
                    arcpy.TableToTable_conversion(self.__cgdConnect, backUpGDB, "backupTable_" + dt_string.replace("/","_"))
                    print("table to table for backupTable_{0}".format(dt_string.replace("/","_")))
                    
                else:
                    print("table to table")
                    #add new table
                    arcpy.TableToTable_conversion(self.__cgdConnect, backUpGDB, "backupTable_" + dt_string.replace("/","_"))
        except Exception as e:
            ouch = arcpy.GetMessages(2)
            Subject = "ERROR: in Create CreateBackUps() of UpdateCCAT_GISMapServices"
            message = "Error details below:\n\n{0}\n\nAdditional error messages:\n\n{1}".format(str(e), ouch)
            emailObj.sendMessage(Subject, message)
        

            
                
                
            
            
    
    def __updateData(self):
        try:
            #self.Caggisrv01.readResponse()
            #self.Caggisrv03.readResponse()


            cgdConnection = r"C:\Users\WKunkler\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\Cagis.sde"
            
            env.workspace = self.rootDir + '\\processing.gdb'
            env.overwriteOutput = True
            """arcpy.TableToTable_conversion(self.rootDir + "\\CagSrv01.csv", self.rootDir + '\\processing.gdb', "CCAT_MAPSERVICES")
            arcpy.TableToTable_conversion(self.rootDir + "\\CagSrv03.csv", self.rootDir + "\\processing.gdb", "CagSrv03_temp")
            arcpy.Append_management(self.rootDir + "\\processing.gdb\\CagSrv03_temp", self.rootDir + "\\processing.gdb\\CCAT_MAPSERVICES", "NO_TEST")
                    
            arcpy.CalculateField_management(self.rootDir + '\\processing.gdb\\CCAT_MAPSERVICES', "Feature_Class", self.__getFCExpression()[0], "PYTHON3",self.__getFCExpression()[1])
            
            arcpy.CalculateField_management(self.rootDir + '\\processing.gdb\\CCAT_MAPSERVICES', "Feature_Dataset", self.__getFDExpression()[0], "PYTHON3", self.__getFDExpression()[1])
            arcpy.CalculateField_management(self.rootDir + '\\processing.gdb\\CCAT_MAPSERVICES', "Version", self.__getVersionExpression()[0], "PYTHON3", self.__getVersionExpression()[1])
            arcpy.CalculateField_management(self.rootDir + '\\processing.gdb\\CCAT_MAPSERVICES', "User", self.__getUserExpression()[0], "PYTHON3", self.__getUserExpression()[1])
            arcpy.CalculateField_management(self.rootDir + '\\processing.gdb\\CCAT_MAPSERVICES', "Database_Connection", self.__getDBConnectionExpression()[0], "PYTHON3", self.__getDBConnectionExpression()[1])"""

            
            arcpy.TruncateTable_management(self.__cgdConnect + "\\CAGIS.CCAT_MAPSERVICES")
            arcpy.Append_management(self.rootDir + "\\processing.gdb\\CCAT_MAPSERVICES", self.__cgdConnect + "\\CAGIS.CCAT_MAPSERVICES", "NO_TEST")
            print("done!")
        except Exception as e:
            ouch = arcpy.GetMessages(2)
            Subject = "ERROR: in Create updateData() of UpdateCCAT_GISMapServices"
            message = "Error details below:\n\n{0}\n\nAdditional error messages:\n\n{1}".format(str(e), ouch)
            emailObj.sendMessage(Subject, message)
            
        
        
main = updateCCAT_GIS_MapServies(r"C:\workingScratchArea\AGOL_QueryTesting\ServicesReportingNew")

    
