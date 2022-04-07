##################################################################################################
#This script was written by Warren Kunkler on 4/9/2020 in support of CAGIS efforts to relate
#services on CAGIS servers to data within Oracle SDE. This script composes the UrlFormat class
#which helps the RestRequest class with handeling rest requests to server to obtain information
#from within each service. This also handles each request by loading the output data into a json
#object, and returns the necessary line of output data to the RestRequest class
##################################################################################################


import http, urllib, json, socket, sys


class UrlFormat:

    #to initialize this object, we needs parameters, headers, along with
    #the serverName and port
    def __init__(self, params, headers, serverName, serverPort):
        self.__params = params
        self.__headers = headers
        self.__serverName = serverName
        self.__serverPort = serverPort



    ##our various getters for outputing lines of data depending on the type of server being queried, they all have different requirements
    #but at a base level all need a reference to the json obj and object status along with the service item and folder
    def __getGeomServLine(self,item, folder, jsonOBJ, jsonOBJStatus):
        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] +\
                    "," + str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + \
                    "," + "NA" + "," + "NA" + "," + str(jsonOBJ["clusterName"]) + "," + "NA" + "," + "NA" + "," + "NA" +"\n"
        return ln

    def __getSearchServLine(self,item, folder, jsonOBJ, jsonOBJStatus):
        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," +\
                   str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + "NA" + \
                   "," + "NA" + "," + str(jsonOBJ["clusterName"]) + "," + "NA" + "," + str(jsonOBJ["properties"]["jobsDirectory"]) + "," + \
                   str(jsonOBJ["properties"]["outputDir"]) + str(jsonOBJ["properties"]["filePath"]) +"\n"

        return ln

    def __getImageServLine(self, item, folder, jsonOBJ, jsonOBJStatus, wmsStatus):
        try:
            ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," \
                        + str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + \
                        wmsStatus +"," + "NA" + "," + str(jsonOBJ["clusterName"]) + "," + str(jsonOBJ["properties"]["cacheDir"]) + "," + "NA," + \
                        str(jsonOBJ["properties"]["outputDir"]) + str(jsonOBJ["properties"]["filePath"]) +"\n"

            return ln
        except Exception as e:
            print(str(e))
            
    def __getGlobeServLine(self, item, folder, jsonOBJ, jsonOBJStatus):

        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," + \
                    str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + "NA" + \
                    "," + str(jsonOBJ["properties"]["maxRecordCount"]) + "," + str(jsonOBJ["clusterName"]) + "," + str(jsonOBJ["properties"]["cacheDir"]) + \
                    "," + "NA" + "," + str(jsonOBJ["properties"]["outputDir"]) + str(jsonOBJ["properties"]["filePath"]) +"\n"
        return ln

    def __getGPSServLine(self, item, folder, jsonOBJ, jsonOBJStatus):
         ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," + \
                    str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + "NA" +\
                   "," + "NA" + "," + str(jsonOBJ["clusterName"]) + "," + "NA" + "," + str(jsonOBJ["properties"]["jobsDirectory"]) + "," + \
                   str(jsonOBJ["properties"]["outputDir"])+"\n"
         return ln

    def __getGeoCServLine(self, item, folder, jsonOBJ, jsonOBJStatus):
        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," +\
                   str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + "NA" + "," + \
                   "NA" + "," + str(jsonOBJ["clusterName"]) + "," + "NA" + "," + "NA" + "," + str(jsonOBJ["properties"]["outputDir"]) + "\n"
        return ln

    def __getGeoDServLine(self, item, folder, jsonOBJ, jsonOBJStatus):

        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," + \
                    str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "NA" + "," + "NA" + "," + "NA" + "," + \
                    str(jsonOBJ["properties"]["maxRecordCount"]) + "," + str(jsonOBJ["clusterName"]) + "," + "NA" + "," + "NA" + "," +\
                   str(jsonOBJ["properties"]["outputDir"])+','+str(jsonOBJ["properties"]["filePath"]) +"\n"

        return ln

    def __getMapServLine(self, item, folder, jsonOBJ, jsonOBJStatus, cacheDir): 

        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," + \
            str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + "FeatServHolder" + "," + \
            "Disabled" + "," + "Disabled" +"," + str(jsonOBJ["properties"]["maxRecordCount"]) + "," + str(jsonOBJ["clusterName"]) + \
            "," + cacheDir + "," + "NA" + "," + str(jsonOBJ["properties"]["outputDir"])+','+str(jsonOBJ["properties"]["filePath"]) +"\n"
        return ln

    def __getMapServLine2(self, item, folder, jsonOBJ, jsonOBJStatus, cacheDir, featureStatus, kmlStatus, wmsStatus):

        ln = str(jsonOBJ["serviceName"]) + "," + folder + "," + str(item["type"]) + "," + jsonOBJStatus['realTimeState'] + "," + \
                                 str(jsonOBJ["minInstancesPerNode"]) + "," + str(jsonOBJ["maxInstancesPerNode"]) + "," + featureStatus + "," + kmlStatus + \
                                 "," + wmsStatus +"," + str(jsonOBJ["properties"]["maxRecordCount"]) + "," + str(jsonOBJ["clusterName"]) + "," + cacheDir + "," + \
                                 "NA" + "," + str(jsonOBJ["properties"]["outputDir"])+','+str(jsonOBJ["properties"]["filePath"]) +"\n"
        return ln



    #Our first handler of requests, this handles requests that are either of type GeometryServer or SearchServer
    #as those sUrl and statusUrls are the same, returns tuple with the line and http client that needs to be closed once
    #the output line is consumed
    def firstHandler(self, item, folder, httpConn):

        if folder:
            sUrl = "/arcgis/admin/services/%s%s.%s" %(folder,item["serviceName"], item["type"])
            statusUrl = "/arcgis/admin/services/%s%s.%s/status" %(folder,item["serviceName"], item["type"])
        else:
            sUrl = "/arcgis/admin/services/%s.%s" %(item["serviceName"], item["type"])
            statusUrl = "/arcgis/admin/services/%s.%s/status" %(item["serviceName"], item["type"])

        httpConn.request("POST", sUrl, self.__params, self.__headers)
                        
        # Get the response
        servResponse = httpConn.getresponse()
        readData = servResponse.read()
        jsonOBJ = json.loads(readData)

       # Submit the request to the server
        httpConn.request("POST", statusUrl, self.__params, self.__headers)
        servStatusResponse = httpConn.getresponse()

       # Obtain the data from the response
        readData = servStatusResponse.read()
        jsonOBJStatus = json.loads(readData)

        ln = ""
        if item["type"] == "GeometryServer":
            ln = self.__getGeomServLine(item, folder, jsonOBJ, jsonOBJStatus)
        elif item["type"] == "SearchServer":
            ln = self.__getSearchServLine(item, folder, jsonOBJ, jsonOBJStatus)

        return (ln, httpConn)



    #Our second handler of requests, this handles requests from other types of servers and has special handling of image and map servers
    #as the image server handles WMS and the map server may be from a cached directory
    #same as before, this returns a tuple with the line and http client that needs to be closed once
    #the output line is consumed

    def secondHandler(self, item, folder,httpConn):

        if folder:
            sUrl = "/arcgis/admin/services/%s%s.%s" %(folder,item["serviceName"], item["type"])
        else:
            sUrl = "/arcgis/admin/services/%s.%s" %(item["serviceName"], item["type"])

       
        httpConn.request("POST", sUrl, self.__params, self.__headers)
        servResponse = httpConn.getresponse()
        readData = servResponse.read()
        jsonOBJ = json.loads(readData)

        if folder:
            statusUrl = "/arcgis/admin/services/%s%s.%s/status" %(folder,item["serviceName"], item["type"])
        else:
            statusUrl = "/arcgis/admin/services/%s.%s/status" %(item["serviceName"], item["type"])


        httpConn.request("POST", statusUrl, self.__params, self.__headers)
        servStatusResponse = httpConn.getresponse()
        readData = servStatusResponse.read()
        jsonOBJStatus = json.loads(readData)
        


        if item["type"] == "ImageServer":
            
            wmsProps = [imageWMS for imageWMS in jsonOBJ["extensions"] if imageWMS["typeName"] == 'WMSServer']
            if len(wmsProps) > 0:
                wmsStatus = str(wmsProps[0]["enabled"])
            else:
                wmsStatus = "NA"
            ln = self.__getImageServLine(item, folder, jsonOBJ, jsonOBJStatus, wmsStatus)


        elif item["type"] == "GlobeServer":
            ln = self.__getGlobeServLine(item, folder, jsonOBJ, jsonOBJStatus)
        elif item["type"] == "GPServer":
            ln = self.__getGPSServLine(item, folder, jsonOBJ, jsonOBJStatus)
        elif item["type"] == "GeocodeServer":
            ln = self.__getGeoCServLine(item, folder, jsonOBJ, jsonOBJStatus)
            
        elif item["type"] == "GeoDataServer":
            ln = self.__getGeoDServLine(item, folder, jsonOBJ, jsonOBJStatus)


        elif item["type"] == "MapServer":

            isCached = jsonOBJ["properties"]["isCached"]
            if isCached == "true":
                cacheDir = str(jsonOBJ["properties"]["cacheDir"])
            else:
                cacheDir = jsonOBJ["properties"]["isCached"]

            if len(jsonOBJ["extensions"]) == 0:
                ln = self.__getMapServLine(item, folder, jsonOBJ, jsonOBJStatus, cacheDir)
            else:
                kmlProps = [mapKML for mapKML in jsonOBJ["extensions"] if mapKML["typeName"] == 'KmlServer']
                wmsProps = [mapWMS for mapWMS in jsonOBJ["extensions"] if mapWMS["typeName"] == 'WMSServer']
                featServProps = [featServ for featServ in jsonOBJ["extensions"] if featServ["typeName"] == 'FeatureServer']



            if len(featServProps) > 0:
                featureStatus = str(featServProps[0]["enabled"])
            else:
                featureStatus = "NA"

            if len(kmlProps) > 0:
                kmlStatus = str(kmlProps[0]["enabled"])
            else:
                kmlStatus = "NA"

            if len(wmsProps) > 0:
                wmsStatus = str(wmsProps[0]["enabled"])
            else:
                wmsStatus = "NA"
            ln = self.__getMapServLine2(item, folder, jsonOBJ, jsonOBJStatus, cacheDir, featureStatus, kmlStatus, wmsStatus)
        return (ln, httpConn)
