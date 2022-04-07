#########################################################################################################################
#
#This script was written by Warren Kunkler on 4/8/2020 in support of efforts to map out cagis online feature services
#This script outputs a table of each service on our servers (caggissrv01, caggissrv02, caggissrv03, caggissrv04) 
#which can be used with the python api to match sde feature classes to our services on arcgis online
#
#Note this is still a draft, more testing and development is in progress
#
#
##########################################################################################################################



import http, urllib, json, socket, sys, getpass, arcpy, os
from ServicesCheck import UrlFormat
from arcpy import env
from arcgis.gis import GIS
from arcgis.mapping import WebMap
from Token import GenToken



     
class RestRequest:

    ##static variables of RestRequest
    serverURL = "/arcgis/admin/services/"
    __headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    
    
    #constructor needs admin login credentials along with server information and output for file
    def __init__(self, userName, passWord, serverName, serverPort, resultFile):

        
        self.serverName = serverName
        self.resultFile = resultFile
        self.serverPort = serverPort
        
        self.genToken = GenToken(userName, passWord, serverName, serverPort)

        self.__serviceResultFile = open(resultFile,"w")

        self.__params = urllib.parse.urlencode({"token": self.genToken.getResponse(), "f":"json"})

        self.__Conn = http.client.HTTPConnection(serverName, serverPort)
        self.__Conn.request("POST", self.serverURL, self.__params, self.__headers) 



    def __getLayerFileNames(self, inputRow):

        if inputRow != None:
            
            print(inputRow.split(",")[-1].replace("\\","/").replace(".msd", ".mxd").strip())
            fullPath = inputRow.split(",")[-1].replace("\\","/").replace(".msd", ".mxd").strip()
            dirPath = inputRow.split(',')[-1].split("\\")[:-1]
            env.workspace = "\\".join(dirPath)
            mxdsList = arcpy.ListFiles("*.mxd")
            
            
            if self.serverName in ["caggissrv01", "caggissrv02"]:
                FirstLines = "https://gocagis.coc.ads/arcgis/rest/services/" + inputRow.split(",")[1] + inputRow.split(",")[0] + "/MapServer/"
            else:
                FirstLines = "https://cagisonline.hamilton-co.org/arcgis/rest/services/" + inputRow.split(",")[1] + inputRow.split(",")[0] + "/MapServer/"

            
            aprx = arcpy.mp.ArcGISProject(r"C:\Users\WKunkler\Documents\ArcGIS\Projects\MyProject\MyProject.aprx")
            if ".mxd" in fullPath:
                if len(mxdsList) > 0:
                    aprx.importDocument(fullPath)
                else:
                    fullPath = fullPath.replace(".mxd",".mapx")
                    aprx.importDocument(fullPath)
                print("imported successfully!")
                count = 0
                for m in aprx.listMaps():
                    for lyr in m.listLayers():
                        if lyr.name != "Cagis_Aerial_2020" and lyr.supports("DATASOURCE"):
                            
                            dataSrcList=lyr.dataSource.split(",")

                            if len(dataSrcList) ==7:
                                if "User=" in dataSrcList[2]:
                                    self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +dataSrcList[1] + ',' + dataSrcList[2] + ',' + dataSrcList[3] + ',' + dataSrcList[5] + ',' + dataSrcList[6] + ',' + lyr.definitionQuery + '\n')
                                else:
                                    
                                     self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +dataSrcList[1] + ',' + dataSrcList[3] + ','+""+ ',' + "" + ',' + dataSrcList[6]+ ',' + lyr.definitionQuery + '\n')
                            elif len(dataSrcList) == 6:
                                if "User=" in dataSrcList[2]:
                                    self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +dataSrcList[1] + ',' + dataSrcList[2] + ',' + dataSrcList[3] + ',' + "" + ',' + dataSrcList[5] + ',' + lyr.definitionQuery + '\n')
                                else:
                                    self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +dataSrcList[1] + ',' + dataSrcList[3] + ',' + "" + ',' + "" + ',' + dataSrcList[5] + ',' + lyr.definitionQuery + '\n')
                            elif len(dataSrcList) == 1:
                                self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +"" + ',' + "" + ',' + "" + ',' + "" + ',' + dataSrcList[0] + ',' + lyr.definitionQuery + '\n')
                            #self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," + ",".join(dataSrcList) + "\n")
                            count += 1
                        elif lyr.name != "Cagis_Aerial_2020":
                            self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," + "" + "," + "" + "," + "" + "," + "" + "\n")
                            count += 1
                    for tbl in m.listTables():
                        dataSrcList=tbl.dataSource.split(",")
                        if len(dataSrcList) ==7:
                            if "User=" in dataSrcList[2]:  
                                self.__serviceResultFile.write(FirstLines+str(count) + "," + tbl.name + "," +dataSrcList[1] + ',' + dataSrcList[2] + ',' + dataSrcList[3] + ',' + dataSrcList[5] + ',' + dataSrcList[6] + ','+ tbl.definitionQuery + '\n')
                            else:    
                                self.__serviceResultFile.write(FirstLines+str(count) + "," + lyr.name + "," +dataSrcList[1] + ',' + dataSrcList[3] + ','+""+ ',' + "" + ',' + dataSrcList[6]+ ','+ tbl.definitionQuery + '\n')
                        elif len(dataSrcList) == 6:
                            if "User=" in dataSrcList[2]:
                                self.__serviceResultFile.write(FirstLines+str(count) + "," + tbl.name + "," +dataSrcList[1] + ',' + dataSrcList[2] + ',' + dataSrcList[3] + ',' + "" + ',' + dataSrcList[5]+ ','+ tbl.definitionQuery + '\n')
                            else:
                                self.__serviceResultFile.write(FirstLines+str(count) + "," + tbl.name + "," +dataSrcList[1] + ',' + dataSrcList[3] + ',' + "" + ',' + "" + ',' + dataSrcList[5] + ','+ tbl.definitionQuery+ '\n')
                        elif len(dataSrcList) == 1:
                            self.__serviceResultFile.write(FirstLines+str(count) + "," + tbl.name + "," +"" + ',' + "" + ',' + "" + ',' + "" + ',' + dataSrcList[0]+ ','+ tbl.definitionQuery + '\n')
                        #self.__serviceResultFile.write(FirstLines+str(count) + "," + tbl.name + "," + ",".join(dataSrcList)  + "\n")
                        count += 1
            


        
    #private method to query each item within each service folder. this method delegates functionality 
    #to the methods within the UrlFormat helper class
    def __queryItems(self, folder):
        FormUrl = UrlFormat(self.__params, self.__headers, self.serverName, self.serverPort)
        for item in self.__dataObj["services"]:
            if item["type"] in ["GeometryServer", "SearchServer"]:
                package = FormUrl.firstHandler(item, folder, self.__Conn)
                self.__getLayerFileNames(package[0])
                #self.__serviceResultFile.write(package[0])
                package[1].close()
           
            
            elif item["type"] not in ["GeometryServer", "SearchServer"]:
                package = FormUrl.secondHandler(item, folder, self.__Conn)
                #self.__serviceResultFile.write(package[0])
                self.__getLayerFileNames(package[0])
                package[1].close()
            else:
                self.__Conn.close()


            
        
                    
                    
    #private method that queries folders
    def __queryFolder(self, folders):
       

        for folder in folders:

            if folder != "":
                folder += "/"

            folderURL = "/arcgis/admin/services/"+folder
            self.__params = urllib.parse.urlencode({"token": self.genToken.getResponse(), "f":"json"})

            self.__Conn = http.client.HTTPConnection(self.serverName, self.serverPort)
            self.__Conn.request("POST", folderURL, self.__params, self.__headers)

            response = self.__Conn.getresponse()
            if (response.status != 200):
                self.__Conn.close()
                print("could not read folder information")
                return
            else:
                data = response.read()
                if not self.genToken.JsonSuccess(data):
                    print("Error when reaing folder information " + str(data))
                else:
                    print("Processed folder information successfully. Now processing services...")

                self.__dataObj = json.loads(data)
                self.__Conn.close()
                self.__queryItems(folder)
        


    #main client method for starting the querying process
    def readResponse(self):
        self.__response = self.__Conn.getresponse()
        
        if (self.__response.status != 200):
            self.__Conn.close()
            print("Could not read folder information")
            return
        else:
            data = self.__response.read()
            if not self.genToken.JsonSuccess(data):
                print("Error when reading server information. " + str(data))
                return
            else:
                print("Processed server information successfully. Now processing folders...")

            self.__dataObj = json.loads(data)
            self.__Conn.close()

            folders = self.__dataObj["folders"]
            folders.remove("System")
            folders.append("")

            self.__serviceResultFile.write("Service, Feature Layer Name, Database Connection, User, Version, Feature Dataset, Feature Class, Definition Query  " + "\n")
            self.__queryFolder(folders)
            self.__serviceResultFile.close()






#R = RestRequest("siteadmin", "admin", "caggissrv01", 6080, r"C:\workingScratchArea\AGOL_QueryTesting\ServicesReportingNew\someplace.txt")

#R.readResponse()

#cagSrv03 = RestRequest("siteadmin", "admin", "caggissrv03", 6080, r"C:\workingScratchArea\AGOL_QueryTesting\ServicesReportingNew\CagSrv03.csv")
#cagSrv03.readResponse()






    
