####################################################################################################
#This script was written by Warren Kunkler on 4/7/2020 in support of CAGIS efforts to report
#service usage through arcgis online. This composes the helper GenToken class which will 
#generate a token using admin login credentials. If the connection to the server is a success
#this sends a successful signal back to the client class object that it can proceed to loop
#through folders within the server, otherwise the connection is bad and it will abort the process
#
#
#Note this is still a draft and testing is still in progress
####################################################################################################


import http, urllib, json, sys, getpass



class GenToken:
    tokenURL = "/arcgis/admin/generateToken"
   
    
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}

    #our constructor takes necessary paramaters to initiate connection with server
    def __init__(self, userName, passWord, serverName, serverPort):
        self.__params = urllib.parse.urlencode({'username': userName, 'password': passWord, 'client': 'requestip', 'f': 'json'})
        self.serverName = serverName
        self.serverPort = serverPort

        




    #fetches the tokens from the admin url
    def getResponse(self):
        self.__conn = http.client.HTTPConnection(self.serverName, self.serverPort)
        self.__conn.request("POST", self.tokenURL, self.__params, self.headers)
        response = self.__conn.getresponse()

        if (response.status != 200):
            self.__conn.close()
            print("error while fetching tokens from admin URL. Please check the URL and try again.")
            return
        else:
            data = response.read()
            self.__conn.close()

            if not self.JsonSuccess(data):
                return
            
            self.token = json.loads(data)
            return self.token["token"]
    
    #handles if the JSON object was successfully loaded or contains errors
    def JsonSuccess(self, data):
        obj = json.loads(data)
        if "status" in obj and obj["status"] == "error":
            print("Error: JSON object returns an error. " + str(obj))
            return False
        else:
            return True

